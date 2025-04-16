package index

import (
	"bitcask-go/data"
	"bytes"

	"github.com/google/btree"
)

type Indexer interface {
	// Put inserts a key-value pair into the index.
	Put(key []byte, pos *data.LogRecordPos) bool

	// Get retrieves the position of a key from the index.
	Get(key []byte) *data.LogRecordPos

	// Delete removes a key-value pair from the index.
	Delete(key []byte) bool
}
type IndexType = int8

const (
	Btree IndexType = iota + 1

	ART
)

// 根据类型初始化索引
func NewIndexer(typ IndexType) Indexer {
	switch typ {
	case Btree:
		return NewBTree()
	case ART:
		return nil
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
