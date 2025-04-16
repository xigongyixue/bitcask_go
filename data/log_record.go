package data

type LogRecordType = byte

const (
	LogRecordNormal  LogRecordType = iota // 写入
	LogRecordDeleted                      // 删除
)

// 写入到数据文件的记录，添加写
type LogRecord struct {
	Key   []byte        // 键
	Value []byte        // 值
	Type  LogRecordType // 类型
}

// 数据内存索引，数据在磁盘上的位置
type LogRecordPos struct {
	Fid    uint32 // 文件ID
	Offset int64  // 偏移量
}

func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	return nil, 0
}
