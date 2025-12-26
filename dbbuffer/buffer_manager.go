package dbbuffer

import (
	"fmt"
	"sync"
	"time"

	"github.com/teru01/simpledb-go/dbfile"
	"github.com/teru01/simpledb-go/dblog"
)

const MAX_WAIT_TIME = 10000 * time.Millisecond

type BufferManager struct {
	mu           sync.Mutex
	cond         *sync.Cond
	bufferPool   []Buffer
	numAvailable int
}

func NewBufferManager(fm *dbfile.FileManager, lm *dblog.LogManager, numBuffers int) *BufferManager {
	bufs := make([]Buffer, numBuffers)
	for i := range numBuffers {
		bufs[i] = NewBuffer(fm, lm)
	}
	bm := &BufferManager{
		bufferPool:   bufs,
		numAvailable: numBuffers,
	}
	bm.cond = sync.NewCond(&bm.mu)
	return bm
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
		if buf.state.txNum != txNum {
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
		bm.cond.Broadcast()
	}
}

func (bm *BufferManager) Pin(blk dbfile.BlockID) (*Buffer, error) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	// 最大max_timeまつ
	// tryToPinが失敗したら1つunpinされるのを待つ
	startTime := time.Now()
	for {
		buf, err := bm.tryToPinLocked(blk)
		if err != nil {
			return nil, fmt.Errorf("failed to pin: %w", err)
		}
		if buf != nil {
			return buf, nil
		}

		// タイムアウトチェック
		if time.Since(startTime) >= MAX_WAIT_TIME {
			return nil, fmt.Errorf("failed to pin. It took too long to get an unpinned buffer")
		}

		// bufferが利用可能になるまで待つ
		bm.cond.Wait()
	}
}

func (bm *BufferManager) tryToPinLocked(blk dbfile.BlockID) (*Buffer, error) {
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
