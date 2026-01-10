package dbfile

import (
	"encoding/binary"
	"strconv"
)

type ByteBuffer struct {
	buffer []byte
}

const intSize = strconv.IntSize / 8
const uint64Size = 8

func NewByteBuffer(blockSize int) *ByteBuffer {
	return &ByteBuffer{buffer: make([]byte, blockSize)}
}

func NewByteBufferFromBytes(bytes []byte) *ByteBuffer {
	return &ByteBuffer{buffer: bytes}
}

func (b *ByteBuffer) Size() int {
	return len(b.buffer)
}

func (b *ByteBuffer) GetInt(offset int) int {
	if intSize == 8 {
		return int(binary.BigEndian.Uint64(b.buffer[offset : offset+intSize]))
	}
	return int(binary.BigEndian.Uint32(b.buffer[offset : offset+4]))
}

func (b *ByteBuffer) SetInt(offset int, value int) {
	if intSize == 8 {
		binary.BigEndian.PutUint64(b.buffer[offset:offset+intSize], uint64(value))
	} else {
		binary.BigEndian.PutUint32(b.buffer[offset:offset+4], uint32(value))
	}
}

func (b *ByteBuffer) GetBytes(offset int) []byte {
	len := b.GetInt(offset)
	buf := make([]byte, len)
	contentPos := offset + intSize
	copy(buf, b.buffer[contentPos:contentPos+len])
	return buf
}

// len, contentをoffset位置に書き込む
func (b *ByteBuffer) SetBytes(offset int, content []byte) {
	b.SetInt(offset, len(content))
	contentPos := offset + intSize
	copy(b.buffer[contentPos:contentPos+len(content)], content)
}

func (b *ByteBuffer) GetUint64(offset int) uint64 {
	return binary.BigEndian.Uint64(b.buffer[offset : offset+uint64Size])
}

func (b *ByteBuffer) SetUint64(offset int, value uint64) {
	binary.BigEndian.PutUint64(b.buffer[offset:offset+uint64Size], value)
}
