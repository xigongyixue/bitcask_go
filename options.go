package bitcask_go

import "os"

type Options struct {
	DirPath            string      // 数据库数据目录
	DataFileSize       int64       // 数据文件大小
	SyncWrites         bool        // 是否持久化
	BytesPerSync       uint        // 每次同步的字节数
	IndexType          IndexerType // 索引类型
	MMapAtStartup      bool        // 是否在启动时内存映射数据文件
	DataFileMergeRatio float32     // 数据文件合并阈值
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
	DirPath:            os.TempDir(),
	DataFileSize:       64 * 1024 * 1024, // 64MB
	SyncWrites:         false,
	BytesPerSync:       0,
	IndexType:          BTree,
	MMapAtStartup:      true,
	DataFileMergeRatio: 0.5,
}

var DefaultIteratorOptions = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}

var DefaultWriteBatchOptions = WriteBatchOptions{
	MaxBatchNum: 10000,
	SyncWrites:  true,
}
