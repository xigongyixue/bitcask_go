package bitcask_go

import (
	"bitcask-go/index"
	"bytes"
)

type Iterator struct {
	indexIter index.Iterator
	db        *DB
	options   IteratorOptions
}

func (db *DB) NewIterator(options IteratorOptions) *Iterator {
	indexIter := db.index.Iterator(options.Reverse)
	return &Iterator{
		indexIter: indexIter,
		db:        db,
		options:   options,
	}
}

// 回到起始位置
func (it *Iterator) Rewind() {
	it.indexIter.Rewind()
	it.skipToNext()
}

// 从这个key开始遍历
func (it *Iterator) Seek(key []byte) {
	it.indexIter.Seek(key)
	it.skipToNext()
}

// 跳转到下一个key
func (it *Iterator) Next() {
	it.indexIter.Next()
	it.skipToNext()
}

// 当前位置是否有效
func (it *Iterator) Valid() bool {
	return it.indexIter.Valid()
}

// 当前key
func (it *Iterator) Key() []byte {
	return it.indexIter.Key()
}

// 当前value
func (it *Iterator) Value() ([]byte, error) {
	logRecordPos := it.indexIter.Value()
	it.db.mu.RLock()
	defer it.db.mu.RUnlock()
	return it.db.getValueByPosition(logRecordPos)

}

// 关闭迭代器
func (it *Iterator) Close() {
	it.indexIter.Close()
}

// 跳过不符合前缀的key
func (it *Iterator) skipToNext() {
	prefixLen := len(it.options.Prefix)
	if prefixLen == 0 {
		return
	}

	for ; it.Valid(); it.Next() {
		key := it.indexIter.Key()
		if prefixLen <= len(key) && bytes.Compare(key[:prefixLen], it.options.Prefix) == 0 {
			break
		}
	}
}
