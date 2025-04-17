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
		Key:   key,
		Value: value,
		Type:  data.LogRecordNormal,
	}

	// 追加写入活跃文件
	pos, err := db.appendLogRecord(&logRecord)
	if err != nil {
		return err
	}
	// 更新内存索引
	if ok := db.index.Put(key, pos); !ok {
		return ErrIndexUpdataFailed
	}

	return nil
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
		Key:  key,
		Type: data.LogRecordDeleted,
	}

	// 追加写入活跃文件
	_, err := db.appendLogRecord(logRecord)
	if err != nil {
		return err
	}

	// 更新内存索引为删除
	if ok := db.index.Delete(key); !ok {
		return ErrIndexUpdataFailed
	}

	return nil
}

// 追加写到活跃文件中
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

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

func (db *DB) Close() error {
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
			var ok bool
			if logRecord.Type == data.LogRecordDeleted {
				ok = db.index.Delete(logRecord.Key)
			} else {
				ok = db.index.Put(logRecord.Key, logRecordPos)
			}

			if !ok {
				return ErrIndexUpdataFailed
			}

			offset += size
		}

		// 最后一个文件,说明为活跃文件
		if i == len(db.fileIds)-1 {
			db.activeFile.WriteOff = offset
		}
	}

	return nil

}
