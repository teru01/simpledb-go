package dbfile

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

func (p *Page) SetInt(offset int, value int) {
	p.buffer.SetInt(offset, value)
}

func (p *Page) GetBytes(offset int) []byte {
	p.buffer.SetPosition(offset)
	size := p.buffer.GetCurrentInt()
	b := make([]byte, size)
	p.buffer.GetCurrentByte(b)
	return b
}

func (p *Page) SetBytes(offset int, bytes []byte) {
	p.buffer.SetPosition(offset)
	p.buffer.SetCurrentInt(len(bytes))
	p.buffer.SetCurrentByte(bytes)
}

func (p *Page) GetString(offset int) string {
	return string(p.GetBytes(offset))
}

func (p *Page) SetString(offset int, value string) {
	p.SetBytes(offset, []byte(value))
}

func (p *Page) MaxLength(strLen int) int {
	return intSize + strLen*4
}

func (p *Page) pageBuffer() ByteBuffer {
	p.buffer.SetPosition(0)
	return p.buffer
}
