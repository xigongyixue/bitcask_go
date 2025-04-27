package index

import (
	"bitcask-go/data"
	"bytes"

	"github.com/google/btree"
)

type Indexer interface {
	// Put inserts a key-value pair into the index.
	Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos

	// Get retrieves the position of a key from the index.
	Get(key []byte) *data.LogRecordPos

	// Delete removes a key-value pair from the index.
	Delete(key []byte) (*data.LogRecordPos, bool)

	// Size returns the number of key-value pairs in the index.
	Size() int

	// Iterator returns an iterator over the index.
	Iterator(reverse bool) Iterator

	// Close closes the index and releases any resources.
	Close() error
}
type IndexType = int8

const (
	Btree IndexType = iota + 1
	ART
	BPTree
)

// 根据类型初始化索引
func NewIndexer(typ IndexType, dirPath string, sync bool) Indexer {
	switch typ {
	case Btree:
		return NewBTree()
	case ART:
		return NewART()
	case BPTree:
		return NewBPlusTree(dirPath, sync)
	default:
		panic("unknown index type")
	}
}

type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (ai *Item) Less(bi btree.Item) bool {
	return bytes.Compare(ai.key, bi.(*Item).key) == -1
}

// 通用索引迭代器
type Iterator interface {
	Rewind()                   // 回到起始位置
	Seek(key []byte)           // 从这个key开始遍历
	Next()                     // 跳转到下一个key
	Valid() bool               // 是否完成遍历
	Key() []byte               // 当前key
	Value() *data.LogRecordPos // 当前value
	Close()                    // 关闭迭代器
}
