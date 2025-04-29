package redis

import (
	"encoding/binary"
	"math"
)

const (
	maxMetadataSize   = 1 + binary.MaxVarintLen64*2 + binary.MaxVarintLen32
	extraListMetaSize = binary.MaxVarintLen64 * 2

	initialListMark = math.MaxUint64 / 2
)

type metadata struct {
	dataType byte   // 数据类型
	expire   int64  // 过期时间
	version  int64  // 版本号
	size     uint32 // 数据大小
	head     uint64 // List头部指针
	tail     uint64 // List尾部指针
}

func (md *metadata) encode() []byte {
	var size = maxMetadataSize
	if md.dataType == List {
		size += extraListMetaSize
	}

	buf := make([]byte, size)
	buf[0] = md.dataType
	var index = 1
	index += binary.PutVarint(buf[index:], md.expire)
	index += binary.PutVarint(buf[index:], md.version)
	index += binary.PutVarint(buf[index:], int64(md.size))

	if md.dataType == List {
		index += binary.PutUvarint(buf[index:], md.head)
		index += binary.PutUvarint(buf[index:], md.tail)
	}

	return buf[:index]
}

func decodeMetadata(buf []byte) *metadata {
	dataType := buf[0]

	var index = 1
	expire, n := binary.Varint(buf[index:])
	index += n
	version, n := binary.Varint(buf[index:])
	index += n
	size, n := binary.Varint(buf[index:])
	index += n

	var head uint64 = 0
	var tail uint64 = 0
	if dataType == List {
		head, n = binary.Uvarint(buf[index:])
		index += n
		tail, _ = binary.Uvarint(buf[index:])
	}

	return &metadata{
		dataType: dataType,
		expire:   expire,
		version:  version,
		size:     uint32(size),
		head:     head,
		tail:     tail,
	}
}

type hashInternalKey struct {
	key     []byte
	version int64
	field   []byte
}

func (hk *hashInternalKey) encode() []byte {
	buf := make([]byte, len(hk.key)+8+len(hk.field))

	var index = 0
	copy(buf[index:], hk.key)
	index += len(hk.key)
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(hk.version))
	index += 8
	copy(buf[index:], hk.field)

	return buf
}

type listInternalKey struct {
	key     []byte
	version int64
	index   uint64
}

func (lk *listInternalKey) encode() []byte {
	buf := make([]byte, len(lk.key)+8+8)

	var index = 0
	copy(buf[index:index+len(lk.key)], lk.key)
	index += len(lk.key)
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(lk.version))
	index += 8
	binary.LittleEndian.PutUint64(buf[index:], lk.index)

	return buf
}
