package fio

import (
	"os"

	"golang.org/x/exp/mmap"
)

// 内存文件映射
type MMap struct {
	readerAt *mmap.ReaderAt // 这个包只可以读数据，不能写数据

}

func NewMMapIOManager(fileName string) (*MMap, error) {
	_, err := os.OpenFile(fileName, os.O_CREATE, DataFilePerm)
	if err != nil {
		return nil, err
	}
	readerAt, err := mmap.Open(fileName)
	if err != nil {
		return nil, err
	}
	return &MMap{readerAt: readerAt}, nil
}

// ReadAt reads from the file at the given offset.
func (mmap *MMap) Read(b []byte, off int64) (int, error) {
	return mmap.readerAt.ReadAt(b, off)
}

// Write writes to the file.
func (mmap *MMap) Write([]byte) (int, error) {
	panic("not implemented")
}

// Sync flushes the file to disk.
func (mmap *MMap) Sync() error {
	panic("not implemented")
}

// Close closes the file.
func (mmap *MMap) Close() error {
	return mmap.readerAt.Close()
}

// Size returns the size of the file.
func (mmap *MMap) Size() (int64, error) {
	return int64(mmap.readerAt.Len()), nil
}
