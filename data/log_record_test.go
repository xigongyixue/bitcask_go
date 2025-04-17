package data

import (
	"hash/crc32"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEncodeLogRecord(t *testing.T) {
	// 正常情况
	// rec1 := &LogRecord{
	// 	Key:   []byte("key1"),
	// 	Value: []byte("value1"),
	// 	Type:  LogRecordNormal,
	// }

	// res1, n1 := EncodeLogRecord(rec1)
	// assert.NotNil(t, res1)
	// assert.Greater(t, n1, int64(5))
	// t.Log(res1)

	// value为空, 类型为删除
	rec2 := &LogRecord{
		Key:  []byte("key2"),
		Type: LogRecordDeleted,
	}
	res2, n2 := EncodeLogRecord(rec2)
	assert.NotNil(t, res2)
	assert.Greater(t, n2, int64(5))
	t.Log(res2)
}

func TestDecodeLogRecord(t *testing.T) {
	// headerBuf1 := []byte{115, 112, 45, 146, 0, 8, 12}
	// h1, size1 := decodeLogRecordHeader(headerBuf1)
	// t.Log(h1, size1)

	headerBuf2 := []byte{81, 188, 89, 67, 1, 8, 0}
	h2, size2 := decodeLogRecordHeader(headerBuf2)
	t.Log(h2, size2)
}

func TestGetLogRecordCRC(t *testing.T) {
	rec1 := &LogRecord{
		Key:   []byte("key1"),
		Value: []byte("value1"),
		Type:  LogRecordNormal,
	}

	headerBuf1 := []byte{115, 112, 45, 146, 0, 8, 12}
	crc1 := getLogRecordCRC(rec1, headerBuf1[crc32.Size:])
	assert.Equal(t, crc1, uint32(2452451443))
}
