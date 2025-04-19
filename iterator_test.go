package bitcask_go

import (
	"bitcask-go/utils"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDB_NewIterator(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-iterator-1")
	opts.DirPath = dir
	db, err := Open(opts)
	db.Close()
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	iterator := db.NewIterator(DefaultIteratorOptions)
	assert.NotNil(t, iterator)
	assert.Equal(t, false, iterator.Valid())
}

func TestDB_Iterator_One_Value(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-iterator-2")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(utils.GetTestKey(10), utils.GetTestKey(10))
	assert.Nil(t, err)

	iterator := db.NewIterator(DefaultIteratorOptions)
	assert.NotNil(t, iterator)
	assert.Equal(t, true, iterator.Valid())
	assert.Equal(t, utils.GetTestKey(10), iterator.Key())
	val, err := iterator.Value()
	assert.Nil(t, err)
	assert.Equal(t, utils.GetTestKey(10), val)

	db.Close()
}

func TestDB_Iterator_Multiple_Values(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-iterator-3")
	opts.DirPath = dir
	db, err := Open(opts)
	db.Close()
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put([]byte("312daddas"), utils.GetTestKey(10))
	assert.Nil(t, err)
	err = db.Put([]byte("ewqdas"), utils.GetTestKey(10))
	assert.Nil(t, err)
	err = db.Put([]byte("bewers"), utils.GetTestKey(10))
	assert.Nil(t, err)
	err = db.Put([]byte("d4326s"), utils.GetTestKey(10))
	assert.Nil(t, err)
	err = db.Put([]byte("bitas"), utils.GetTestKey(10))
	assert.Nil(t, err)
	err = db.Put([]byte("455434"), utils.GetTestKey(10))
	assert.Nil(t, err)
	err = db.Put([]byte("tretgg"), utils.GetTestKey(10))
	assert.Nil(t, err)

	iter1 := db.NewIterator(DefaultIteratorOptions)
	for iter1.Rewind(); iter1.Valid(); iter1.Next() {
		assert.NotNil(t, iter1.Key())
	}

	iter1.Rewind()
	for iter1.Seek([]byte("b")); iter1.Valid(); iter1.Next() {
		assert.NotNil(t, iter1.Key())
	}

	iterOpts1 := DefaultIteratorOptions
	iterOpts1.Reverse = true
	iter2 := db.NewIterator(iterOpts1)
	for iter2.Rewind(); iter2.Valid(); iter2.Next() {
		assert.NotNil(t, iter2.Key())
	}

	iter2.Rewind()
	for iter2.Seek([]byte("b")); iter2.Valid(); iter2.Next() {
		assert.NotNil(t, iter2.Key())
	}

	iterOpts2 := DefaultIteratorOptions
	iterOpts2.Prefix = []byte("b")
	iter3 := db.NewIterator(iterOpts2)
	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
		t.Log(string(iter3.Key()))
	}

}
