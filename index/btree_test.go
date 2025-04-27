package index

import (
	"bitcask-go/data"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBTree_Put(t *testing.T) {
	bt := NewBTree()

	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.Nil(t, res1)

	res2 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 2})
	assert.Nil(t, res2)

	res3 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 3})
	assert.Equal(t, &data.LogRecordPos{Fid: 1, Offset: 2}, res3)
}

func TestBTree_Get(t *testing.T) {
	bt := NewBTree()

	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.Nil(t, res1)

	res2 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 2})
	assert.Nil(t, res2)
	res3 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 3})
	assert.Equal(t, &data.LogRecordPos{Fid: 1, Offset: 2}, res3)

	pos2 := bt.Get([]byte("a"))
	assert.Equal(t, uint32(1), pos2.Fid)
	assert.Equal(t, int64(3), pos2.Offset)

}

func TestBTree_Delete(t *testing.T) {
	bt := NewBTree()

	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.Nil(t, res1)

	res2, ok1 := bt.Delete(nil)
	assert.Equal(t, true, ok1)
	assert.Equal(t, &data.LogRecordPos{Fid: 1, Offset: 100}, res2)

	res3 := bt.Put([]byte("aaa"), &data.LogRecordPos{Fid: 22, Offset: 33})
	assert.Nil(t, res3)

	res4, ok2 := bt.Delete([]byte("aaa"))
	assert.True(t, ok2)
	assert.Equal(t, &data.LogRecordPos{Fid: 22, Offset: 33}, res4)
}

// func TestBTree_Iterator(t *testing.T) {
// 	bt1 := NewBTree()
// 	iter1 := bt1.Iterator(false)
// 	assert.Equal(t, false, iter1.Valid())

// 	bt1.Put([]byte("code"), &data.LogRecordPos{Fid: 1, Offset: 2})
// 	iter2 := bt1.Iterator(false)
// 	assert.Equal(t, true, iter2.Valid())

// 	bt1.Put([]byte("cdase"), &data.LogRecordPos{Fid: 1, Offset: 2})
// 	bt1.Put([]byte("cc333"), &data.LogRecordPos{Fid: 1, Offset: 2})
// 	bt1.Put([]byte("rd212e"), &data.LogRecordPos{Fid: 1, Offset: 2})
// 	iter3 := bt1.Iterator(false)
// 	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
// 		assert.NotNil(t, iter3.Key())
// 	}
// 	iter4 := bt1.Iterator(true)
// 	for iter4.Rewind(); iter4.Valid(); iter4.Next() {
// 		assert.NotNil(t, iter4.Key())
// 	}

// 	iter5 := bt1.Iterator(false)
// 	for iter5.Seek([]byte("cc")); iter5.Valid(); iter5.Next() {
// 		assert.NotNil(t, iter5.Key())
// 	}

// 	iter6 := bt1.Iterator(true)
// 	for iter6.Seek([]byte("ddd")); iter6.Valid(); iter6.Next() {
// 		assert.NotNil(t, iter6.Key())
// 		t.Log(string(iter6.Key()))
// 	}
// }
