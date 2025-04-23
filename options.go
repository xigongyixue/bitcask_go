package bitcask_go

import "os"

type Options struct {
	DirPath      string      // 数据库数据目录
	DataFileSize int64       // 数据文件大小
	SyncWrites   bool        // 是否持久化
	IndexType    IndexerType // 索引类型
}

type IteratorOptions struct {
	Prefix  []byte // 前缀
	Reverse bool   // 是否倒序
}

type WriteBatchOptions struct {
	MaxBatchNum uint // 最大批次数量
	SyncWrites  bool // 是否持久化
}

type IndexerType = int8

const (
	BTree IndexerType = iota + 1
	ART
	BPlusTree
)

var DefaultOptions = Options{
	DirPath:      os.TempDir(),
	DataFileSize: 256 * 1024 * 1024, // 1GB
	SyncWrites:   false,
	IndexType:    BPlusTree,
}

var DefaultIteratorOptions = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}

var DefaultWriteBatchOptions = WriteBatchOptions{
	MaxBatchNum: 10000,
	SyncWrites:  true,
}
