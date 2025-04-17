package data

import (
	"bitcask-go/fio"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"path/filepath"
)

const DataFileNameSuffix = ".data"

// 数据文件
type DataFile struct {
	FileId    uint32        // file id
	WriteOff  int64         // write offset
	IoManager fio.IOManager // io manager
}

var (
	ErrInvalidCRC = errors.New("invalid crc value, log record may be corrupted")
)

// OpenDataFile opens a data file with the given file id.
func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {

	fileName := filepath.Join(dirPath, fmt.Sprintf("%09d", fileId)+DataFileNameSuffix)

	ioManager, err := fio.NewIOManager(fileName)
	if err != nil {
		return nil, err
	}

	return &DataFile{
		FileId:    fileId,
		WriteOff:  0,
		IoManager: ioManager,
	}, nil

}

// 根据offset从数据文件读取日志记录
func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {

	fileSize, err := df.IoManager.Size()
	if err != nil {
		return nil, 0, err
	}

	// 如果header大小超过文件大小，则只需读到文件末尾
	var headerBytes int64 = maxLogRecordHeaderSize
	if offset+headerBytes > fileSize {
		headerBytes = fileSize - offset
	}

	// 读取日志记录头部
	headerBuf, err := df.ReadNBytes(headerBytes, offset)
	if err != nil {
		return nil, 0, err
	}

	header, headerSize := decodeLogRecordHeader(headerBuf)

	// 读到了文件末尾
	if header == nil {
		return nil, 0, io.EOF
	}

	if header.crc == 0 && header.keySize == 0 && header.valueSize == 0 {
		return nil, 0, io.EOF
	}

	// 去除kv长度
	keySize, valueSize := int64(header.keySize), int64(header.valueSize)
	var recordSize = headerSize + keySize + valueSize

	logReord := &LogRecord{
		Type: header.recordType,
	}

	// 读取kv
	if keySize > 0 || valueSize > 0 {
		kvBuf, err := df.ReadNBytes(keySize+valueSize, offset+headerSize)
		if err != nil {
			return nil, 0, err
		}

		logReord.Key = kvBuf[:keySize]
		logReord.Value = kvBuf[keySize:]
	}

	// 校验
	crc := getLogRecordCRC(logReord, headerBuf[crc32.Size:headerSize])
	if crc != header.crc {
		return nil, 0, ErrInvalidCRC
	}

	return logReord, recordSize, nil
}

// 持久化到磁盘
func (df *DataFile) Sync() error {
	return df.IoManager.Sync()
}

func (df *DataFile) Write(buf []byte) error {
	n, err := df.IoManager.Write(buf)
	if err != nil {
		return err
	}

	df.WriteOff += int64(n)
	return nil
}

func (df *DataFile) Close() error {
	return df.IoManager.Close()
}

func (df *DataFile) ReadNBytes(n int64, offset int64) ([]byte, error) {
	b := make([]byte, n)
	_, err := df.IoManager.Read(b, offset)
	return b, err
}

func getLogRecordCRC(lr *LogRecord, header []byte) uint32 {
	return 0
}
