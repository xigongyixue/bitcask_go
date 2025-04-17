package bitcask_go

import "os"

type Options struct {
	DirPath      string      // 数据库数据目录
	DataFileSize int64       // 数据文件大小
	SyncWrites   bool        // 是否持久化
	IndexType    IndexerType // 索引类型
}

type IndexerType = int8

const (
	BTree IndexerType = iota + 1
)

var DefaultOptions = Options{
	DirPath:      os.TempDir(),
	DataFileSize: 256 * 1024 * 1024, // 1GB
	SyncWrites:   false,
	IndexType:    BTree,
}
