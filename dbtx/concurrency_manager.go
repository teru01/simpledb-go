package dbtx

import (
	"context"
	"fmt"

	"github.com/teru01/simpledb-go/dbfile"
)

// 全てのtransactionで共有
var lockTable = NewLockTable()

// 個々のtransactionが保持する.
type ConcurrencyManager struct {
	locks map[dbfile.BlockID]string
}

func NewConcurrencyManager() *ConcurrencyManager {
	return &ConcurrencyManager{
		locks: make(map[dbfile.BlockID]string),
	}
}

func (c *ConcurrencyManager) SLock(ctx context.Context, blk dbfile.BlockID) error {
	if _, ok := c.locks[blk]; !ok {
		if err := lockTable.SLock(ctx, blk); err != nil {
			return fmt.Errorf("failed to SLock: %w", err)
		}
		c.locks[blk] = "S"
	}
	return nil
}

func (c *ConcurrencyManager) XLock(ctx context.Context, blk dbfile.BlockID) error {
	if !c.hasXLock(blk) {
		if err := c.SLock(ctx, blk); err != nil {
			return fmt.Errorf("failed to SLock: %w", err)
		}
		if err := lockTable.XLock(ctx, blk); err != nil {
			return fmt.Errorf("failed to upgrade to XLock: %w", err)
		}
		c.locks[blk] = "X"
	}
	return nil
}

func (c *ConcurrencyManager) Release() {
	for blk := range c.locks {
		lockTable.UnLock(blk)
	}
	clear(c.locks)
}

func (c *ConcurrencyManager) hasXLock(blk dbfile.BlockID) bool {
	return c.locks[blk] == "X"
}
