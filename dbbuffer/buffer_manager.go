package dbbuffer

import (
	"container/list"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/teru01/simpledb-go/dberr"
	"github.com/teru01/simpledb-go/dbfile"
	"github.com/teru01/simpledb-go/dblog"
)

const MAX_WAIT_TIME = 10000 * time.Millisecond

type BufferManager struct {
	mu                       sync.Mutex
	availabilityNotification chan struct{}
	bufferPool               []Buffer
	// unpinされた時間を見たdequeue unpinされると左からenq, replaceは右からdequeされていく
	// 常にclear unpinされたもの詰まっていて、右側が最も昔にclear unpinされたもの、左側が新しいもの 初期状態では全部clear unpinされたとみなせる
	lru          *list.List
	numAvailable int
}

func NewBufferManager(fm *dbfile.FileManager, lm *dblog.LogManager, numBuffers int) *BufferManager {
	bufs := make([]Buffer, numBuffers)
	lru := list.New()
	for i := range numBuffers {
		bufs[i] = NewBuffer(i, fm, lm)
		lru.PushFront(i)
	}
	bm := &BufferManager{
		bufferPool:               bufs,
		numAvailable:             numBuffers,
		availabilityNotification: make(chan struct{}, 1),
		lru:                      lru,
	}

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
	for i := range bm.bufferPool {
		if bm.bufferPool[i].state.txNum != txNum {
			continue
		}
		if err := bm.bufferPool[i].flush(); err != nil {
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
		bm.lru.PushFront(buffer.ID)
		select {
		case bm.availabilityNotification <- struct{}{}:
		default:
			// pinを待機してるものがいない
		}
	}
}

func (bm *BufferManager) Pin(blk dbfile.BlockID) (*Buffer, error) {
	// 最大max_timeまつ
	// tryToPinが失敗したら1つunpinされるのを待つ
	ctx, cancel := context.WithTimeout(context.Background(), MAX_WAIT_TIME)
	defer cancel()

	for {
		buf, err := func() (*Buffer, error) {
			bm.mu.Lock()
			defer bm.mu.Unlock()
			return bm.tryToPinLocked(blk)
		}()

		if err != nil {
			return nil, fmt.Errorf("failed to pin: %w", err)
		}
		if buf != nil {
			return buf, nil
		}

		select {
		case <-bm.availabilityNotification:
		case <-ctx.Done():
			return nil, dberr.New(dberr.CodeBufferWaitAbort, "failed to pin. It took too long to get an unpinned buffer", nil)
		}
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
	for i := range bm.bufferPool {
		if bm.bufferPool[i].BlockID().Equals(blk) {
			for e := bm.lru.Front(); e != nil; {
				id, _ := e.Value.(int)
				if id == bm.bufferPool[i].ID {
					bm.lru.Remove(e)
					break
				}
				e = e.Next()
			}
			return &bm.bufferPool[i]
		}
	}
	return nil
}

func (bm *BufferManager) chooseUnpinnedBuffer() *Buffer {
	back := bm.lru.Back()
	if back == nil {
		return nil
	}
	id, ok := back.Value.(int)
	if !ok {
		return nil
	}
	bm.lru.Remove(back)
	return &bm.bufferPool[id]
}
