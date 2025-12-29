package dbtx

import (
	"context"
	"sync"
	"time"

	"github.com/teru01/simpledb-go/dberr"
	"github.com/teru01/simpledb-go/dbfile"
)

const MAX_WAIT_TIME_SECOND = 10

type LockTable struct {
	mu               sync.Mutex
	lockWaitChannels map[dbfile.BlockID]chan struct{}
	locks            map[dbfile.BlockID]int
}

func NewLockTable() LockTable {
	return LockTable{
		mu:               sync.Mutex{},
		lockWaitChannels: make(map[dbfile.BlockID]chan struct{}),
		locks:            make(map[dbfile.BlockID]int),
	}
}

func (l *LockTable) SLock(ctx context.Context, blk dbfile.BlockID) error {
	ctx, cancel := context.WithTimeout(ctx, MAX_WAIT_TIME_SECOND*time.Second)
	defer cancel()

	for {
		var (
			gotLock bool
			waitCh  chan struct{}
		)

		func() {
			l.mu.Lock()
			defer l.mu.Unlock()
			if l.lockWaitChannels[blk] == nil {
				l.lockWaitChannels[blk] = make(chan struct{}, 1)
			}
			waitCh = l.lockWaitChannels[blk] // lockの外で使うため
			if !l.hasXLockLocked(blk) {
				l.locks[blk]++
				gotLock = true
			}
		}()
		if gotLock {
			return nil
		}
		select {
		case <-waitCh: // xlockが外れた時
		case <-ctx.Done():
			return dberr.New(dberr.CodeTransactionLockWaitAbort, "timeout. It took too long to get xlock", nil)
		}
	}
}

// XLockをとる
// XLockを取る前にSLockが取られる必要がある
func (l *LockTable) XLock(ctx context.Context, blk dbfile.BlockID) error {
	ctx, cancel := context.WithTimeout(ctx, MAX_WAIT_TIME_SECOND*time.Second)
	defer cancel()

	for {
		var (
			gotLock bool
			waitCh  chan struct{}
		)
		func() {
			l.mu.Lock()
			defer l.mu.Unlock()
			if l.lockWaitChannels[blk] == nil {
				l.lockWaitChannels[blk] = make(chan struct{}, 1)
			}
			waitCh = l.lockWaitChannels[blk] // lockの外で使うため
			if !l.hasOtherSLocksLocked(blk) {
				l.locks[blk] = -1
				gotLock = true
			}
		}()
		if gotLock {
			return nil
		}
		select {
		case <-waitCh:
		case <-ctx.Done():
			return dberr.New(dberr.CodeTransactionLockWaitAbort, "timeout. It took too long to get xlock", nil)
		}
	}
}

// ロックを外す
func (l *LockTable) UnLock(blk dbfile.BlockID) {
	l.mu.Lock()
	defer l.mu.Unlock()
	val, ok := l.locks[blk]
	if !ok {
		return
	}
	if val > 1 {
		// slockが複数ついている時
		l.locks[blk]--
	} else {
		// xlock, slock1つのみ
		delete(l.locks, blk)
		if ch, exists := l.lockWaitChannels[blk]; exists {
			close(ch)
			delete(l.lockWaitChannels, blk)
		}
	}
}

func (l *LockTable) hasXLockLocked(blk dbfile.BlockID) bool {
	return l.locks[blk] < 0
}

func (l *LockTable) hasOtherSLocksLocked(blk dbfile.BlockID) bool {
	return l.locks[blk] > 1
}

func (l *LockTable) getLockValLocked(blk dbfile.BlockID) int {
	return l.locks[blk]
}
