package dbtx

import (
	"fmt"

	"github.com/teru01/simpledb-go/dbbuffer"
	"github.com/teru01/simpledb-go/dbfile"
)

type BufferList struct {
	buffers       map[dbfile.BlockID]*dbbuffer.Buffer
	pinsCount     map[dbfile.BlockID]int
	bufferManager *dbbuffer.BufferManager
}

func NewBufferList(bm *dbbuffer.BufferManager) *BufferList {
	return &BufferList{
		buffers:       make(map[dbfile.BlockID]*dbbuffer.Buffer),
		pinsCount:     make(map[dbfile.BlockID]int),
		bufferManager: bm,
	}
}

func (b *BufferList) Buffer(blk dbfile.BlockID) *dbbuffer.Buffer {
	if buf, ok := b.buffers[blk]; ok {
		return buf
	}
	return nil
}

func (b *BufferList) Pin(blk dbfile.BlockID) error {
	buf, err := b.bufferManager.Pin(blk)
	if err != nil {
		return fmt.Errorf("failed to pin: %w", err)
	}
	b.buffers[blk] = buf
	b.pinsCount[blk]++
	return nil
}

func (b *BufferList) UnPin(blk dbfile.BlockID) error {
	buf := b.Buffer(blk)
	if buf == nil {
		return fmt.Errorf("failed to unpin. no such buffer %s", blk)
	}
	b.bufferManager.Unpin(buf)
	b.pinsCount[blk]--
	if b.pinsCount[blk] <= 0 {
		delete(b.buffers, blk)
		delete(b.pinsCount, blk)
	}
	return nil
}

func (b *BufferList) UnpinAll() {
	for blk, count := range b.pinsCount {
		buf := b.Buffer(blk)
		if buf != nil {
			for range count {
				b.bufferManager.Unpin(buf)
			}
		}
	}
	clear(b.buffers)
	clear(b.pinsCount)
}
