package fio

const DataFilePerm = 0644

type IOManager interface {
	// ReadAt reads from the file at the given offset.
	Read([]byte, int64) (int, error)

	// Write writes to the file.
	Write([]byte) (int, error)

	// Sync flushes the file to disk.
	Sync() error

	// Close closes the file.
	Close() error

	// Size returns the size of the file.
	Size() (int64, error)
}

// 初始化IOManager
func NewIOManager(fileName string) (IOManager, error) {
	return NewFileIOManager(fileName)
}
