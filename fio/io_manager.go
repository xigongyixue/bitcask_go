package fio

const DataFilePerm = 0744

type IOManager interface {
	// ReadAt reads from the file at the given offset.
	Read([]byte, int64) (int, error)

	// Write writes to the file.
	Write([]byte) (int, error)

	// Sync flushes the file to disk.
	Sync() error

	// Close closes the file.
	Close() error
}
