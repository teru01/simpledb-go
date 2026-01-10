package dbfile

import "fmt"

type Page struct {
	buffer *ByteBuffer
}

// IO Bufferを使わないのでパフォーマンスどうか？
func NewPage(blockSize int) *Page {
	return &Page{buffer: NewByteBuffer(blockSize)}
}

func NewPageFromBytes(bytes []byte) *Page {
	return &Page{buffer: NewByteBufferFromBytes(bytes)}
}

func (p *Page) GetInt(offset int) int {
	return p.buffer.GetInt(offset)
}

func (p *Page) SetInt(offset int, value int) error {
	if offset < 0 || offset+intSize > p.buffer.Size() {
		return fmt.Errorf("offset %d out of range [0, %d)", offset, p.buffer.Size()-intSize+1)
	}
	p.buffer.SetInt(offset, value)
	return nil
}

func (p *Page) GetBytes(offset int) []byte {
	return p.buffer.GetBytes(offset)
}

// bufferにbyte length, payloadを書き込む. len(int)+len(bytes)の長さのバッファを消費する
func (p *Page) SetBytes(offset int, bytes []byte) error {
	requiredSize := offset + intSize + len(bytes)
	if offset < 0 || requiredSize > p.buffer.Size() {
		return fmt.Errorf("offset %d with %d bytes exceeds page size %d", offset, len(bytes), p.buffer.Size())
	}
	p.buffer.SetBytes(offset, bytes)
	return nil
}

func (p *Page) GetString(offset int) string {
	return string(p.GetBytes(offset))
}

func (p *Page) SetString(offset int, value string) error {
	return p.SetBytes(offset, []byte(value))
}

func (p *Page) MaxLength(strLen int) int {
	return intSize + strLen*4
}

func MaxStringLengthOnPage(strLen int) int {
	return intSize + strLen*4
}

func (p *Page) pageBuffer() *ByteBuffer {
	return p.buffer
}

func (p *Page) Length() int {
	return p.buffer.Size()
}

func (p *Page) GetUint64(offset int) uint64 {
	return p.buffer.GetUint64(offset)
}

func (p *Page) SetUint64(offset int, value uint64) error {
	if offset < 0 || offset+8 > p.buffer.Size() {
		return fmt.Errorf("offset %d out of range [0, %d)", offset, p.buffer.Size()-8+1)
	}
	p.buffer.SetUint64(offset, value)
	return nil
}
