package dbfile

import (
	"encoding/binary"
	"strconv"
)

type ByteBuffer struct {
	buffer   []byte
	position int
}

const intSize = strconv.IntSize / 8
const uint64Size = 8

func NewByteBuffer(blockSize int) *ByteBuffer {
	return &ByteBuffer{buffer: make([]byte, blockSize)}
}

func NewByteBufferFromBytes(bytes []byte) *ByteBuffer {
	return &ByteBuffer{buffer: bytes}
}

func (b *ByteBuffer) Position() int {
	return b.position
}

func (b *ByteBuffer) SetPosition(position int) {
	b.position = position
}

func (b *ByteBuffer) Size() int {
	return len(b.buffer)
}

// 現在のポジションからintを取得してポジションを進める
func (b *ByteBuffer) GetCurrentInt() int {
	c := b.GetInt(b.Position())
	b.SetPosition(b.Position() + intSize)
	return c
}

func (b *ByteBuffer) SetCurrentInt(value int) {
	b.SetInt(b.Position(), value)
	b.SetPosition(b.Position() + intSize)
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

func (b *ByteBuffer) GetBytes(offset, len int) []byte {
	buf := make([]byte, len)
	copy(buf, b.buffer[offset:offset+len])
	return buf
}

// 現在のポジションからbufの長さ分のバイトを取得してbufにセットし、ポジションを進める
func (b *ByteBuffer) GetCurrentByte(buf []byte) {
	copy(buf, b.GetBytes(b.Position(), len(buf)))
	b.SetPosition(b.Position() + len(buf))
}

func (b *ByteBuffer) SetCurrentByte(buf []byte) {
	copy(b.buffer[b.Position():b.Position()+len(buf)], buf)
	b.SetPosition(b.Position() + len(buf))
}

func (b *ByteBuffer) GetUint64(offset int) uint64 {
	return binary.BigEndian.Uint64(b.buffer[offset : offset+uint64Size])
}

func (b *ByteBuffer) SetUint64(offset int, value uint64) {
	binary.BigEndian.PutUint64(b.buffer[offset:offset+uint64Size], value)
}
