package file

import "encoding/binary"

const Charset = "ascii"

type Page struct {
	buffer []byte
}

func NewPage(blockSize int) Page {
	return Page{buffer: make([]byte, blockSize)}
}

func NewPageFromBytes(bytes []byte) Page {
	return Page{buffer: bytes}
}

func (p *Page) GetInt(offset int) int {
	return int(binary.BigEndian.Uint32(p.buffer[offset : offset+4]))
}
