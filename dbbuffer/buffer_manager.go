package dbbuffer

import (
	"fmt"
	"sync"

	"github.com/teru01/simpledb-go/dbfile"
	"github.com/teru01/simpledb-go/dblog"
)

const MAX_WAIT_TIME = 10000

type BufferManager struct {
	mu           sync.Mutex
	bufferPool   []Buffer
	numAvailable int
}

func NewBufferManager(fm *dbfile.FileManager, lm *dblog.LogManager, numBuffers int) BufferManager {
	bufs := make([]Buffer, numBuffers)
	for i := range numBuffers {
		bufs[i] = NewBuffer(fm, lm)
	}
	return BufferManager{
		bufferPool:   bufs,
		numAvailable: numBuffers,
	}
}

func (bm *BufferManager) Available() int {
	bm.mu.Lock()
	defer bm.mu.Unlock()
	return bm.numAvailable
}

// txNumによって変更されたbufferをflushする
func (bm *BufferManager) FlushAll(txNum int) error {
	bm.mu.Lock()
	defer bm.mu.Unlock()
	for i, buf := range bm.bufferPool {
		if buf.txNum != txNum {
			continue
		}
		if err := buf.flush(); err != nil {
			return fmt.Errorf("failed to flush [%v]: %w", i, err)
		}
	}
	return nil
}

func (bm *BufferManager) Unpin(buffer *Buffer) {
	bm.mu.Lock()
	defer bm.mu.Unlock()
	buffer.unpin()
	if !buffer.IsPinned() {
		bm.numAvailable++
	}
}

func (bm *BufferManager) Pin(blk dbfile.BlockID) error {

	return nil
}

func (bm *BufferManager) tryToPin(blk dbfile.BlockID) (*Buffer, error) {
	buf := bm.findExistingBuffer(blk)
	if buf == nil {
		buf = bm.chooseUnpinnedBuffer()
		if buf == nil {
			return nil, nil
		}
		if err := buf.assignToBlock(blk); err != nil {
			return nil, fmt.Errorf("failed to tryPin: %w", err)
		}
	}
	if !buf.IsPinned() {
		bm.numAvailable--
	}
	buf.pin()
	return buf, nil
}

func (bm *BufferManager) findExistingBuffer(blk dbfile.BlockID) *Buffer {
	for _, buf := range bm.bufferPool {
		if buf.BlockID().Equals(blk) {
			return &buf
		}
	}
	return nil
}

func (bm *BufferManager) chooseUnpinnedBuffer() *Buffer {
	for _, buf := range bm.bufferPool {
		if !buf.IsPinned() {
			return &buf
		}
	}
	return nil
}
