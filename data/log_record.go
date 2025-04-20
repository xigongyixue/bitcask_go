package data

import (
	"encoding/binary"
	"hash/crc32"
)

type LogRecordType = byte

const (
	LogRecordNormal      LogRecordType = iota // 写入
	LogRecordDeleted                          // 删除
	LogRecordTxnFinished                      // 结束标记
)

// crc type keysize valuesize : 4 + 1 + 5 + 5
const maxLogRecordHeaderSize = binary.MaxVarintLen32*2 + 5 // 单个日志记录的头部大小

// 写入到数据文件的记录，添加写
type LogRecord struct {
	Key   []byte        // 键
	Value []byte        // 值
	Type  LogRecordType // 类型
}

// 日志记录头部
type LogRecordHeader struct {
	crc        uint32        // 校验和
	recordType LogRecordType // 记录类型
	keySize    uint32        // 键大小
	valueSize  uint32        // 值大小
}

// 数据内存索引，数据在磁盘上的位置
type LogRecordPos struct {
	Fid    uint32 // 文件ID
	Offset int64  // 偏移量
}

// 暂存的事务数据
type TransactionRecord struct {
	Record *LogRecord
	Pos    *LogRecordPos
}

// 编码日志记录
// +---------------+---------------------+------------------+---------------------+------+--------+
// | crc (4 bytes) | recordType (1 byte) | keySize (5 bytes)| valueSize (5 bytes) | key  | value  |
// +---------------+---------------------+------------------+---------------------+------+--------+
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	// 编码日志记录头部
	header := make([]byte, maxLogRecordHeaderSize)

	header[4] = logRecord.Type
	var index = 5

	// 存储变长kv
	index += binary.PutVarint(header[index:], int64(len(logRecord.Key)))
	index += binary.PutVarint(header[index:], int64(len(logRecord.Value)))

	var size = index + len(logRecord.Key) + len(logRecord.Value)
	encBytes := make([]byte, size)
	copy(encBytes[:index], header[:index])
	copy(encBytes[index:], logRecord.Key)
	copy(encBytes[index+len(logRecord.Key):], logRecord.Value)

	// 计算校验和
	crc := crc32.ChecksumIEEE(encBytes[4:])
	binary.LittleEndian.PutUint32(encBytes[:4], crc)

	return encBytes, int64(size)
}

// 解码日志记录头部
func decodeLogRecordHeader(buf []byte) (*LogRecordHeader, int64) {
	if len(buf) <= 4 {
		return nil, 0
	}

	header := &LogRecordHeader{
		crc:        binary.LittleEndian.Uint32(buf[:4]),
		recordType: buf[4],
	}

	var index = 5
	// 读取变长kv size
	keySize, n := binary.Varint(buf[index:])
	header.keySize = uint32(keySize)
	index += n
	valueSize, n := binary.Varint(buf[index:])
	header.valueSize = uint32(valueSize)
	index += n

	return header, int64(index)
}

// 计算日志记录的CRC值, header本身不包括CRC值
func getLogRecordCRC(lr *LogRecord, header []byte) uint32 {
	if lr == nil {
		return 0
	}

	crc := crc32.ChecksumIEEE(header[:])
	crc = crc32.Update(crc, crc32.IEEETable, lr.Key)
	crc = crc32.Update(crc, crc32.IEEETable, lr.Value)

	return crc
}
