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

const MaxWaitTime = 10000 * time.Millisecond

type BufferManager struct {
	mu                       sync.Mutex
	availabilityNotification chan struct{}
	bufferPool               []Buffer
	// unpinされた時間を見たdequeue unpinされると左からenq, replaceは右からdequeされていく
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
		availabilityNotification: make(chan struct{}),
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
func (bm *BufferManager) FlushAll(txNum uint64) error {
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
		if bm.availabilityNotification != nil {
			close(bm.availabilityNotification)
			bm.availabilityNotification = nil
		}
	}
}

func (bm *BufferManager) Pin(ctx context.Context, blk dbfile.BlockID) (*Buffer, error) {
	// 最大max_timeまつ
	// tryToPinが失敗したら1つunpinされるのを待つ
	ctx, cancel := context.WithTimeout(ctx, MaxWaitTime)
	defer cancel()
	for {
		var waitCh chan struct{}
		buf, err := func() (*Buffer, error) {
			bm.mu.Lock()
			defer bm.mu.Unlock()
			if bm.availabilityNotification == nil {
				bm.availabilityNotification = make(chan struct{})
			}
			waitCh = bm.availabilityNotification
			return bm.tryToPinLocked(blk)
		}()

		if err != nil {
			return nil, fmt.Errorf("failed to pin: %w", err)
		}
		if buf != nil {
			return buf, nil
		}

		select {
		case <-waitCh:
		case <-ctx.Done():
			return nil, dberr.New(dberr.CodeBufferWaitAbort, "failed to pin. It took too long to get an unpinned buffer", nil)
		}
	}
}

// goroutineセーフではないので事前にlockが必要
func (bm *BufferManager) tryToPinLocked(blk dbfile.BlockID) (*Buffer, error) {
	// 不要なreplaceを防ぐためpin, unpin関係なくblkにひもづくbufferを探す
	buf := bm.findExistingBuffer(blk)
	if buf == nil {
		// unpinされたbufferから最適なものを探す
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

// ターゲットのblkに現在pin済み、あるいは過去にpinされてunpinされたbufferを探す。
// TODO: 計算量の改善
func (bm *BufferManager) findExistingBuffer(blk dbfile.BlockID) *Buffer {
	for i := range bm.bufferPool {
		if bm.bufferPool[i].BlockID().Equals(blk) {
			// lruに見つかれば除去（unpin済みのbufferとblkが一致した場合）
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
