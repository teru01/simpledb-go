package dbfile

import "fmt"

type Page struct {
	buffer ByteBuffer
}

// IO Bufferを使わないのでパフォーマンスどうか？
func NewPage(blockSize int) Page {
	return Page{buffer: NewByteBuffer(blockSize)}
}

func NewPageFromBytes(bytes []byte) Page {
	return Page{buffer: NewByteBufferFromBytes(bytes)}
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
	p.buffer.SetPosition(offset)
	size := p.buffer.GetCurrentInt()
	b := make([]byte, size)
	p.buffer.GetCurrentByte(b)
	return b
}

// bufferにbyte length, payloadを書き込む. len(int)+len(bytes)の長さのバッファを消費する
func (p *Page) SetBytes(offset int, bytes []byte) error {
	requiredSize := offset + intSize + len(bytes)
	if offset < 0 || requiredSize > p.buffer.Size() {
		return fmt.Errorf("offset %d with %d bytes exceeds page size %d", offset, len(bytes), p.buffer.Size())
	}
	p.buffer.SetPosition(offset)
	p.buffer.SetCurrentInt(len(bytes))
	p.buffer.SetCurrentByte(bytes)
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

func (p *Page) pageBuffer() ByteBuffer {
	p.buffer.SetPosition(0)
	return p.buffer
}

func (p *Page) Length() int {
	return p.buffer.Size()
}
