package data

import "bitcask-go/fio"

const DataFileNameSuffix = ".data"

// 数据文件
type DataFile struct {
	FileId    uint32        // file id
	WriteOff  int64         // write offset
	IoManager fio.IOManager // io manager
}

// OpenDataFile opens a data file with the given file id.
func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
	return nil, nil
}

func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	return nil, 0, nil
}

// 持久化到磁盘
func (df *DataFile) Sync() error {
	return nil
}

func (df *DataFile) Write(buf []byte) error {
	return nil
}
