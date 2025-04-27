package index

import (
	"bitcask-go/data"
	"bytes"
	"sort"
	"sync"

	goart "github.com/plar/go-adaptive-radix-tree"
)

type AdaptiveRadixTree struct {
	tree goart.Tree
	lock *sync.RWMutex
}

// 初始化ART树索引
func NewART() *AdaptiveRadixTree {
	return &AdaptiveRadixTree{
		tree: goart.New(),
		lock: new(sync.RWMutex),
	}
}

// Put inserts a key-value pair into the index.
func (art *AdaptiveRadixTree) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	art.lock.Lock()
	oldValue, _ := art.tree.Insert(key, pos)
	art.lock.Unlock()
	if oldValue == nil {
		return nil
	}
	return oldValue.(*data.LogRecordPos)
}

// Get retrieves the position of a key from the index.
func (art *AdaptiveRadixTree) Get(key []byte) *data.LogRecordPos {
	art.lock.RLock()
	defer art.lock.RUnlock()
	oldValue, found := art.tree.Search(key)
	if !found {
		return nil
	}
	return oldValue.(*data.LogRecordPos) // 转换类型
}

// Delete removes a key-value pair from the index.
func (art *AdaptiveRadixTree) Delete(key []byte) (*data.LogRecordPos, bool) {
	art.lock.RLock()
	oldValue, deleted := art.tree.Delete(key)
	art.lock.RUnlock()
	if oldValue == nil {
		return nil, false
	}
	return oldValue.(*data.LogRecordPos), deleted
}

// Size returns the number of key-value pairs in the index.
func (art *AdaptiveRadixTree) Size() int {
	art.lock.RLock()
	size := art.tree.Size()
	art.lock.RUnlock()
	return size
}

func (art *AdaptiveRadixTree) Close() error {
	return nil
}

// Iterator returns an iterator over the index.
func (art *AdaptiveRadixTree) Iterator(reverse bool) Iterator {
	art.lock.RLock()
	defer art.lock.RUnlock()
	return newARTIterator(art.tree, reverse)
}

// ART 索引迭代器
type artIterator struct {
	curIndex int     // 当前索引
	reverse  bool    // 是否倒序
	values   []*Item // key索引值
}

func newARTIterator(tree goart.Tree, reverse bool) *artIterator {
	var idx int
	if reverse {
		idx = tree.Size() - 1
	}
	values := make([]*Item, tree.Size())
	saveValues := func(node goart.Node) bool {
		item := &Item{
			key: node.Key(),
			pos: node.Value().(*data.LogRecordPos),
		}
		values[idx] = item
		if reverse {
			idx--
		} else {
			idx++
		}
		return true
	}

	tree.ForEach(saveValues)

	return &artIterator{
		curIndex: 0,
		reverse:  reverse,
		values:   values,
	}
}

// 回到起始位置
func (ai *artIterator) Rewind() {
	ai.curIndex = 0
}

// 从这个key开始遍历
func (ai *artIterator) Seek(key []byte) {

	// 二分查找有序数组
	if ai.reverse {
		ai.curIndex = sort.Search(len(ai.values), func(i int) bool {
			return bytes.Compare(ai.values[i].key, key) <= 0
		})
	} else {
		ai.curIndex = sort.Search(len(ai.values), func(i int) bool {
			return bytes.Compare(ai.values[i].key, key) >= 0
		})
	}

}

// 跳转到下一个key
func (ai *artIterator) Next() {
	ai.curIndex += 1
}

// 当前位置是否有效
func (ai *artIterator) Valid() bool {
	return ai.curIndex < len(ai.values)
}

// 当前key
func (ai *artIterator) Key() []byte {
	return ai.values[ai.curIndex].key
}

// 当前value
func (ai *artIterator) Value() *data.LogRecordPos {
	return ai.values[ai.curIndex].pos
}

// 关闭迭代器
func (ai *artIterator) Close() {
	ai.values = nil
}
