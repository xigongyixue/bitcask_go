package data

import "encoding/binary"

type LogRecordType = byte

const (
	LogRecordNormal  LogRecordType = iota // 写入
	LogRecordDeleted                      // 删除
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

// 编码日志记录
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	return nil, 0
}

// 解码日志记录头部
func decodeLogRecordHeader(data []byte) (*LogRecordHeader, int64) {
	return nil, 0
}
