package index

import (
	"bitcask-go/data"
	"bytes"
	"sort"
	"sync"

	"github.com/google/btree"
)

type BTree struct {
	tree *btree.BTree
	lock *sync.RWMutex //写并发不安全
}

func NewBTree() *BTree {
	return &BTree{tree: btree.New(32), lock: new(sync.RWMutex)}
}

func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	it := &Item{key: key, pos: pos}
	bt.lock.Lock()
	oldItem := bt.tree.ReplaceOrInsert(it)
	bt.lock.Unlock()
	if oldItem == nil {
		return nil
	}
	return oldItem.(*Item).pos

}

func (bt *BTree) Get(key []byte) *data.LogRecordPos {
	it := &Item{key: key}
	btreeItem := bt.tree.Get(it)
	if btreeItem == nil {
		return nil
	}
	return btreeItem.(*Item).pos
}

func (bt *BTree) Delete(key []byte) (*data.LogRecordPos, bool) {
	it := &Item{key: key}
	bt.lock.Lock()
	oldItem := bt.tree.Delete(it)
	bt.lock.Unlock()
	if oldItem == nil {
		return nil, false
	}
	return oldItem.(*Item).pos, true
}

func (bt *BTree) Size() int {
	return bt.tree.Len()
}

func (bt *BTree) Iterator(reverse bool) Iterator {
	if bt.tree == nil {
		return nil
	}
	bt.lock.RLock()
	defer bt.lock.RUnlock()
	return newBtreeIterator(bt.tree, reverse)
}

func (bt *BTree) Close() error {
	return nil
}

// BTree 索引迭代器
type btreeIterator struct {
	curIndex int     // 当前索引
	reverse  bool    // 是否倒序
	values   []*Item // key索引值
}

func newBtreeIterator(tree *btree.BTree, reverse bool) *btreeIterator {
	var idx int
	values := make([]*Item, tree.Len())

	saveValues := func(it btree.Item) bool {
		values[idx] = it.(*Item)
		idx++
		return true
	}

	if reverse {
		tree.Descend(saveValues)
	} else {
		tree.Ascend(saveValues)
	}

	return &btreeIterator{
		curIndex: 0,
		reverse:  reverse,
		values:   values,
	}
}

// 回到起始位置
func (bti *btreeIterator) Rewind() {
	bti.curIndex = 0
}

// 从这个key开始遍历
func (bti *btreeIterator) Seek(key []byte) {

	// 二分查找有序数组
	if bti.reverse {
		bti.curIndex = sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(bti.values[i].key, key) <= 0
		})
	} else {
		bti.curIndex = sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(bti.values[i].key, key) >= 0
		})
	}

}

// 跳转到下一个key
func (bti *btreeIterator) Next() {
	bti.curIndex += 1
}

// 当前位置是否有效
func (bti *btreeIterator) Valid() bool {
	return bti.curIndex < len(bti.values)
}

// 当前key
func (bti *btreeIterator) Key() []byte {
	return bti.values[bti.curIndex].key
}

// 当前value
func (bti *btreeIterator) Value() *data.LogRecordPos {
	return bti.values[bti.curIndex].pos
}

// 关闭迭代器
func (bti *btreeIterator) Close() {
	bti.values = nil
}
