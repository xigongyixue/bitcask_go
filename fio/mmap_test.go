package fio

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMMap_Read(t *testing.T) {
	path := filepath.Join("/tmp", "mmap-a.data")
	// defer destoryFile(path) // 报错，进程被占用！

	fio, err := NewFileIOManager(path)
	assert.Nil(t, err)
	_, err = fio.Write([]byte("hello world"))
	assert.Nil(t, err)

	mmapIO2, err := NewMMapIOManager(path)
	assert.Nil(t, err)

	b2 := make([]byte, 20)
	n2, err := mmapIO2.Read(b2, 0)
	t.Log(string(b2), n2)

}
