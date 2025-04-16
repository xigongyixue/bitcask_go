package fio

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func destoryFile(name string) {
	if err := os.RemoveAll(name); err != nil {
		panic(err)
	}
}

func TestNewFileIOManafer(t *testing.T) {
	//注意相对文件路径
	path := filepath.Join("./tmp", "a.data")
	fio, err := NewFileIOManager(path)
	defer destoryFile(path)

	assert.Nil(t, err)
	assert.NotNil(t, fio)
	fio.Close()

}

func TestFileIO_Write(t *testing.T) {
	path := filepath.Join("./tmp", "a.data")
	fio, err := NewFileIOManager(path)
	defer destoryFile(path)

	assert.Nil(t, err)
	assert.NotNil(t, fio)

	n, err := fio.Write([]byte(""))
	assert.Equal(t, 0, n)
	assert.Nil(t, err)

	n1, err := fio.Write([]byte("bitcask kv"))
	assert.Equal(t, 10, n1)
	assert.Nil(t, err)

	n2, err := fio.Write([]byte("storage"))
	assert.Equal(t, 7, n2)
	assert.Nil(t, err)

	fio.Close()
}

func TestFileIO_Read(t *testing.T) {
	path := filepath.Join("./tmp", "a.data")
	fio, err := NewFileIOManager(path)
	defer destoryFile(path)

	assert.Nil(t, err)
	assert.NotNil(t, fio)

	_, err = fio.Write([]byte("key-a"))
	assert.Nil(t, err)

	_, err = fio.Write([]byte("key-a"))
	assert.Nil(t, err)

	b1 := make([]byte, 5)
	n, err := fio.Read(b1, 0)
	assert.Nil(t, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, []byte("key-a"), b1)

	b2 := make([]byte, 5)
	n2, err := fio.Read(b2, 5)
	t.Log(b2, n2, err)

	fio.Close()
}

func TestFileIO_Sync(t *testing.T) {
	path := filepath.Join("./tmp", "a.data")
	fio, err := NewFileIOManager(path)
	defer destoryFile(path)

	assert.Nil(t, err)
	assert.NotNil(t, fio)

	err = fio.Sync()
	assert.Nil(t, err)

	fio.Close()
}

func TestFileIO_Close(t *testing.T) {
	path := filepath.Join("./tmp", "a.data")
	fio, err := NewFileIOManager(path)
	defer destoryFile(path)

	assert.Nil(t, err)
	assert.NotNil(t, fio)

	err = fio.Close()
	assert.Nil(t, err)
}
