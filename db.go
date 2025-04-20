package bitcask_go

import (
	"bitcask-go/data"
	"bitcask-go/index"
	"errors"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type DB struct {
	options    Options
	mu         *sync.RWMutex
	fileIds    []int                     // 数据文件id,只用于加载索引
	activeFile *data.DataFile            // 当前活跃文件,可以写入
	olderFiles map[uint32]*data.DataFile // 旧文件,可以读取
	index      index.Indexer             // 内存索引
	seqNo      uint64                    // 事务序列号，全局递增
}

// 打开存储引擎实例
func Open(options Options) (*DB, error) {
	// 校验用户配置
	if errors := checkOptions(options); errors != nil {
		return nil, errors
	}

	// 判断目录是否存在
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// 初始化DB实例
	db := &DB{
		options:    options,
		mu:         new(sync.RWMutex),
		olderFiles: make(map[uint32]*data.DataFile),
		index:      index.NewIndexer(options.IndexType),
	}

	//加载数据文件
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}

	// 加载索引文件
	if err := db.loadIndexFromDataFiles(); err != nil {
		return nil, err
	}

	return db, nil
}

// 关闭数据库
func (db *DB) Close() error {
	if db.activeFile == nil {
		return nil
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.activeFile.Close(); err != nil {
		return err
	}

	for _, dataFile := range db.olderFiles {
		if err := dataFile.Close(); err != nil {
			return err
		}
	}
	return nil
}

// 持久化活跃文件
func (db *DB) Sync() error {
	if db.activeFile == nil {
		return nil
	}

	db.mu.Lock()
	defer db.mu.Unlock()
	return db.activeFile.Sync()
}

// 检查配置
func checkOptions(options Options) error {
	if len(options.DirPath) == 0 {
		return errors.New("the database dir path is empty")
	}

	if options.DataFileSize <= 0 {
		return errors.New("the data file size is invalid")
	}

	return nil
}

// 写入 kv，不能为空
func (db *DB) Put(key []byte, value []byte) error {
	// Check if the key is empty
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	logRecord := data.LogRecord{
		Key:   logRecordKeyWithSeq(key, nonTransactionSeqNo),
		Value: value,
		Type:  data.LogRecordNormal,
	}

	// 追加写入活跃文件
	pos, err := db.appendLogRecordWithLock(&logRecord)
	if err != nil {
		return err
	}
	// 更新内存索引
	if ok := db.index.Put(key, pos); !ok {
		return ErrIndexUpdataFailed
	}

	return nil
}

// 获得所有key
func (db *DB) ListKeys() [][]byte {
	iterator := db.index.Iterator(false)
	keys := make([][]byte, db.index.Size())
	var idx int
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		keys[idx] = iterator.Key()
		idx++
	}
	return keys
}

// 获得所有数据，并执行指定操作，fn返回false则停止遍历
func (db *DB) Fold(fn func(key []byte, value []byte) bool) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	iterator := db.index.Iterator(false)
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		value, err := db.getValueByPosition(iterator.Value())
		if err != nil {
			continue
		}

		if !fn(iterator.Key(), value) {
			break
		}
	}

	return nil
}

// 根据位置读取数据
func (db *DB) getValueByPosition(logRecordpos *data.LogRecordPos) ([]byte, error) {

	// 根据文件id找到数据文件
	var dataFile *data.DataFile
	if db.activeFile.FileId == logRecordpos.Fid {
		dataFile = db.activeFile
	} else {
		dataFile = db.olderFiles[logRecordpos.Fid]
	}

	// 未找到
	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}

	// 根据偏移量读取数据
	logRecord, _, err := dataFile.ReadLogRecord(logRecordpos.Offset)
	if err != nil {
		return nil, err
	}

	if logRecord.Type == data.LogRecordDeleted {
		return nil, ErrKeyNotFound
	}

	return logRecord.Value, nil
}

// 获得数据
func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}

	// 从内存索引中查找
	logRecordpos := db.index.Get(key)
	if logRecordpos == nil {
		return nil, ErrKeyNotFound
	}
	return db.getValueByPosition(logRecordpos)
}

// 删除数据
func (db *DB) Delete(key []byte) error {

	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 从内存索引中查找, 不存在
	if pos := db.index.Get(key); pos == nil {
		return nil
	}

	// 构造logrecord，标记为删除
	logRecord := &data.LogRecord{
		Key:  logRecordKeyWithSeq(key, nonTransactionSeqNo),
		Type: data.LogRecordDeleted,
	}

	// 追加写入活跃文件
	_, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}

	// 更新内存索引为删除
	if ok := db.index.Delete(key); !ok {
		return ErrIndexUpdataFailed
	}

	return nil
}

func (db *DB) appendLogRecordWithLock(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.appendLogRecord(logRecord)
}

// 追加写到活跃文件中
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {

	// initialize the log file if it doesn't exist
	if db.activeFile == nil {
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	// 写入数据编码
	encRecord, size := data.EncodeLogRecord(logRecord)

	// 如果写入文件达到活跃文件的阈值，则关闭活跃文件，并打开新的文件
	if int64(db.activeFile.WriteOff)+size > db.options.DataFileSize {
		// 持久化数据文件到磁盘
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}

		// 当前活跃文件转化为旧的数据文件
		db.olderFiles[db.activeFile.FileId] = db.activeFile

		// 打开新的活跃文件
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	writeOff := db.activeFile.WriteOff
	if err := db.activeFile.Write(encRecord); err != nil {
		return nil, err
	}

	// 根据用户配置决定是否持久化
	if db.options.SyncWrites {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
	}

	// 构造内存索引信息
	pos := &data.LogRecordPos{Fid: db.activeFile.FileId, Offset: writeOff}
	return pos, nil
}

// 加互斥锁
func (db *DB) setActiveDataFile() error {
	var initialFiledId uint32 = 0
	if db.activeFile != nil {
		initialFiledId = db.activeFile.FileId + 1
	}

	dataFile, err := data.OpenDataFile(db.options.DirPath, initialFiledId)
	if err != nil {
		return err
	}

	db.activeFile = dataFile
	return nil
}

// 从磁盘中加载数据文件
func (db *DB) loadDataFiles() error {
	dirEmtries, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return err
	}

	var fileIds []int
	// 遍历目录中以.data结尾的文件
	for _, entry := range dirEmtries {
		if strings.HasSuffix(entry.Name(), data.DataFileNameSuffix) {
			// 获取文件id
			splitName := strings.Split(entry.Name(), ".")
			fileId, err := strconv.Atoi(splitName[0])
			if err != nil {
				return ErrDataDirectoryCorrupted
			}
			fileIds = append(fileIds, fileId)
		}
	}

	// 排序文件id
	sort.Ints(fileIds)
	db.fileIds = fileIds

	// 遍历文件id，打开数据文件
	for i, fileId := range fileIds {
		dataFile, err := data.OpenDataFile(db.options.DirPath, uint32(fileId))
		if err != nil {
			return err
		}

		if i == len(fileIds)-1 { // 最后一个文件,说明为活跃文件
			db.activeFile = dataFile
		} else { // 其他文件,说明为旧文件
			db.olderFiles[uint32(fileId)] = dataFile
		}
	}

	return nil
}

// 从数据文件中加载索引
func (db *DB) loadIndexFromDataFiles() error {
	// 说明数据库为空
	if len(db.fileIds) == 0 {
		return nil
	}

	updateIndex := func(key []byte, typ data.LogRecordType, pos *data.LogRecordPos) {
		var ok bool
		if typ == data.LogRecordDeleted {
			ok = db.index.Delete(key)
		} else {
			ok = db.index.Put(key, pos)
		}

		if !ok {
			panic("failed to update index")
		}
	}

	// 暂存事务数据
	transactionRecords := make(map[uint64][]*data.TransactionRecord)
	var currentSeqNo = nonTransactionSeqNo

	// 遍历数据文件，加载索引
	for i, fileId := range db.fileIds {
		var fileId = uint32(fileId)
		var dataFile *data.DataFile

		if fileId == db.activeFile.FileId {
			dataFile = db.activeFile
		} else {
			dataFile = db.olderFiles[fileId]
		}

		var offset int64 = 0
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			// 构建内存索引并保存
			logRecordPos := &data.LogRecordPos{Fid: fileId, Offset: offset}

			// 解析key，拿到序列号
			realKey, seqNo := parseLogRecordKey(logRecord.Key)
			if seqNo == nonTransactionSeqNo {
				// 非事务操作，直接更新索引
				updateIndex(realKey, logRecord.Type, logRecordPos)
			} else {
				if logRecord.Type == data.LogRecordTxnFinished {
					for _, txnRecord := range transactionRecords[seqNo] {
						updateIndex(txnRecord.Record.Key, txnRecord.Record.Type, txnRecord.Pos)
					}
					delete(transactionRecords, seqNo)
				} else {
					logRecord.Key = realKey
					transactionRecords[seqNo] = append(transactionRecords[seqNo], &data.TransactionRecord{
						Record: logRecord,
						Pos:    logRecordPos,
					})
				}
			}

			// 更新事务序列号
			if seqNo > currentSeqNo {
				currentSeqNo = seqNo
			}

			// 读取下一条记录
			offset += size
		}

		// 最后一个文件,说明为活跃文件
		if i == len(db.fileIds)-1 {
			db.activeFile.WriteOff = offset
		}
	}

	// 更新序列号
	db.seqNo = currentSeqNo

	return nil

}
