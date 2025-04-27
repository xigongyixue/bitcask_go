package index

import (
	"bitcask-go/data"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAdaptiveRadixTree_Put(t *testing.T) {
	art := NewART()
	res1 := art.Put([]byte("key1"), &data.LogRecordPos{Fid: 100, Offset: 100})
	assert.Nil(t, res1)
	res2 := art.Put([]byte("key2"), &data.LogRecordPos{Fid: 200, Offset: 200})
	assert.Nil(t, res2)
	res3 := art.Put([]byte("key3"), &data.LogRecordPos{Fid: 300, Offset: 300})
	assert.Nil(t, res3)
	res4 := art.Put([]byte("key3"), &data.LogRecordPos{Fid: 400, Offset: 400})
	t.Log(res4)
}

func TestAdaptiveRadixTree_Get(t *testing.T) {
	art := NewART()
	art.Put([]byte("key1"), &data.LogRecordPos{Fid: 100, Offset: 100})
	pos := art.Get([]byte("key1"))
	t.Log(pos)
	art.Put([]byte("key1"), &data.LogRecordPos{Fid: 200, Offset: 200})
	pos = art.Get([]byte("key1"))
	t.Log(pos)
	art.Put([]byte("key3"), &data.LogRecordPos{Fid: 300, Offset: 300})

}

func TestAdaptiveRadixTree_Delete(t *testing.T) {
	art := NewART()

	art.Put([]byte("key1"), &data.LogRecordPos{Fid: 100, Offset: 100})
	res1 := art.Get([]byte("key1"))
	t.Log(res1)
	art.Delete([]byte("key1"))
	res1 = art.Get([]byte("key1"))
	t.Log(res1)
}

func TestAdaptiveRadixTree_Size(t *testing.T) {
	art := NewART()
	t.Log(art.Size())
	art.Put([]byte("key1"), &data.LogRecordPos{Fid: 100, Offset: 100})
	art.Put([]byte("key2"), &data.LogRecordPos{Fid: 200, Offset: 200})
	art.Put([]byte("key3"), &data.LogRecordPos{Fid: 300, Offset: 300})
	t.Log(art.Size())
}

func TestAdaptiveRadixTree_Iterator(t *testing.T) {
	art := NewART()
	art.Put([]byte("key1"), &data.LogRecordPos{Fid: 100, Offset: 100})
	art.Put([]byte("2rew2"), &data.LogRecordPos{Fid: 200, Offset: 200})
	art.Put([]byte("0342y3"), &data.LogRecordPos{Fid: 300, Offset: 300})

	it := art.Iterator(true)
	for it.Rewind(); it.Valid(); it.Next() {
		t.Log(string(it.Key()))
	}
	it.Close()
}
