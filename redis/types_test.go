package redis

import (
	bitcask "bitcask-go"
	"bitcask-go/utils"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRedisDataStructure_Get(t *testing.T) {
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitkcask-go-redis-get")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	err = rds.Set(utils.GetTestKey(1), 0, utils.RandomValue(100))
	assert.Nil(t, err)

	err = rds.Set(utils.GetTestKey(2), time.Second*5, utils.RandomValue(100))
	assert.Nil(t, err)

	val1, err := rds.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val1)

	val2, err := rds.Get(utils.GetTestKey(2))
	assert.Nil(t, err)
	assert.NotNil(t, val2)

	_, err = rds.Get(utils.GetTestKey(3))
	assert.Equal(t, bitcask.ErrKeyNotFound, err)
}

func TestRedisDataStructure_Type(t *testing.T) {
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitkcask-go-redis-type")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	err = rds.Del(utils.GetTestKey(21))
	assert.Nil(t, err)

	err = rds.Set(utils.GetTestKey(1), 0, utils.RandomValue(100))
	assert.Nil(t, err)

	typ, err := rds.Type(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.Equal(t, String, typ)

	err = rds.Del(utils.GetTestKey(1))
	assert.Nil(t, err)

	_, err = rds.Get(utils.GetTestKey(1))
	assert.Equal(t, bitcask.ErrKeyNotFound, err)
}

func TestRedisDataStructure_HGet(t *testing.T) {
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitkcask-go-redis-hget")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	ok1, _ := rds.HSet(utils.GetTestKey(1), []byte("field1"), utils.RandomValue(100))
	assert.True(t, ok1)
	ok2, _ := rds.HSet(utils.GetTestKey(1), []byte("field1"), utils.RandomValue(100))
	assert.False(t, ok2)
	ok3, _ := rds.HSet(utils.GetTestKey(1), []byte("field2"), utils.RandomValue(200))
	assert.True(t, ok3)

	val1, _ := rds.HGet(utils.GetTestKey(1), []byte("field1"))
	assert.NotNil(t, val1)
	val2, _ := rds.HGet(utils.GetTestKey(1), []byte("field2"))
	assert.NotNil(t, val2)
	val3, _ := rds.HGet(utils.GetTestKey(1), []byte("field3"))
	assert.Nil(t, val3)
}

func TestRedisDataStructure_HDel(t *testing.T) {
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitkcask-go-redis-hdel")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	ok1, _ := rds.HSet(utils.GetTestKey(1), []byte("field1"), utils.RandomValue(100))
	assert.True(t, ok1)
	ok2, _ := rds.HSet(utils.GetTestKey(1), []byte("field1"), utils.RandomValue(100))
	assert.False(t, ok2)
	ok3, _ := rds.HSet(utils.GetTestKey(1), []byte("field2"), utils.RandomValue(200))
	assert.True(t, ok3)

	ok4, _ := rds.HDel(utils.GetTestKey(1), []byte("field1"))
	assert.True(t, ok4)
	ok5, _ := rds.HDel(utils.GetTestKey(1), []byte("field1"))
	assert.False(t, ok5)
	ok6, _ := rds.HDel(utils.GetTestKey(1), []byte("field3"))
	assert.False(t, ok6)
}

func TestRedisDataStructure_LPop(t *testing.T) {
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-lpop")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	res, err := rds.LPush(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.Equal(t, uint32(1), res)
	res, err = rds.LPush(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.Equal(t, uint32(2), res)
	res, err = rds.LPush(utils.GetTestKey(1), []byte("val-2"))
	assert.Nil(t, err)
	assert.Equal(t, uint32(3), res)

	val, err := rds.LPop(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val)
	val, err = rds.LPop(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val)
	val, err = rds.LPop(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val)
}

func TestRedisDataStructure_RPop(t *testing.T) {
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-rpop")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	res, err := rds.RPush(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.Equal(t, uint32(1), res)
	res, err = rds.RPush(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.Equal(t, uint32(2), res)
	res, err = rds.RPush(utils.GetTestKey(1), []byte("val-2"))
	assert.Nil(t, err)
	assert.Equal(t, uint32(3), res)

	val, err := rds.RPop(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val)
	val, err = rds.RPop(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val)
	val, err = rds.RPop(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val)
}
