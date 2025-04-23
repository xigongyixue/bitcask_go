package index

import (
	"bitcask-go/data"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewBPlusTree_Put(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree-put")
	_ = os.MkdirAll(path, os.ModePerm)
	defer func() {
		_ = os.RemoveAll(path)
	}()

	tree := NewBPlusTree(path, false)

	tree.Put([]byte("key1"), &data.LogRecordPos{Fid: 1, Offset: 100})
	tree.Put([]byte("key2"), &data.LogRecordPos{Fid: 2, Offset: 200})
	tree.Put([]byte("key3"), &data.LogRecordPos{Fid: 3, Offset: 300})
}

func TestNewBPlusTree_Get(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree-get")
	_ = os.MkdirAll(path, os.ModePerm)
	defer func() {
		_ = os.RemoveAll(path)
	}()

	tree := NewBPlusTree(path, false)
	pos := tree.Get([]byte("not exit"))
	assert.Nil(t, pos)
	pos = tree.Get([]byte("key1"))
	t.Log(pos)
	tree.Put([]byte("key1"), &data.LogRecordPos{Fid: 2, Offset: 243})
	pos = tree.Get([]byte("key1"))
	t.Log(pos)
}

func TestNewBPlusTree_Delete(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree-delete")
	_ = os.MkdirAll(path, os.ModePerm)
	defer func() {
		_ = os.RemoveAll(path)
	}()

	tree := NewBPlusTree(path, false)
	tree.Put([]byte("key1"), &data.LogRecordPos{Fid: 1, Offset: 100})
	pos := tree.Get([]byte("key1"))
	t.Log(pos)
	tree.Delete([]byte("key1"))
	pos = tree.Get([]byte("key1"))
	t.Log(pos)

}

func TestNewBPlusTree_Size(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree-size")
	_ = os.MkdirAll(path, os.ModePerm)
	defer func() {
		_ = os.RemoveAll(path)
	}()

	tree := NewBPlusTree(path, false)

	tree.Put([]byte("key1"), &data.LogRecordPos{Fid: 1, Offset: 100})
	t.Log(tree.Size())
	tree.Put([]byte("key2"), &data.LogRecordPos{Fid: 2, Offset: 200})
	tree.Put([]byte("key3"), &data.LogRecordPos{Fid: 3, Offset: 300})
	t.Log(tree.Size())
}

func TestNewBPlusTree_Iterator(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree-iter")
	_ = os.MkdirAll(path, os.ModePerm)
	defer func() {
		_ = os.RemoveAll(path)
	}()

	tree := NewBPlusTree(path, false)
	tree.Put([]byte("key1"), &data.LogRecordPos{Fid: 1, Offset: 100})
	tree.Put([]byte("key2"), &data.LogRecordPos{Fid: 2, Offset: 200})
	tree.Put([]byte("key3"), &data.LogRecordPos{Fid: 3, Offset: 300})
	tree.Put([]byte("key5"), &data.LogRecordPos{Fid: 5, Offset: 500})

	iter := tree.Iterator(false)
	for iter.Rewind(); iter.Valid(); iter.Next() {
		t.Log(iter.Key(), iter.Value())
	}

}
