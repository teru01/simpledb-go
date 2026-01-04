package dbtx_test

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/teru01/simpledb-go/dbfile"
	"github.com/teru01/simpledb-go/dbtx"
)

func TestLockTableSLock(t *testing.T) {
	lt := dbtx.NewLockTable()
	blk := dbfile.NewBlockID("testfile", 0)
	ctx := context.Background()

	err := lt.SLock(ctx, blk)
	if err != nil {
		t.Fatalf("failed to acquire SLock: %v", err)
	}

	if got := lt.GetLockValLocked(blk); got != 1 {
		t.Errorf("expected lock count 1, got %d", got)
	}
}

func TestLockTableMultipleSLocks(t *testing.T) {
	lt := dbtx.NewLockTable()
	blk := dbfile.NewBlockID("testfile", 0)
	ctx := context.Background()

	// Acquire multiple SLocks on the same block
	for i := 0; i < 3; i++ {
		if err := lt.SLock(ctx, blk); err != nil {
			t.Fatalf("failed to acquire SLock %d: %v", i, err)
		}
	}

	if got := lt.GetLockValLocked(blk); got != 3 {
		t.Errorf("expected lock count 3, got %d", got)
	}

	// Unlock twice (should still have 1 SLock)
	lt.UnLock(blk)
	lt.UnLock(blk)

	if got := lt.GetLockValLocked(blk); got != 1 {
		t.Errorf("expected lock count 1 after 2 unlocks, got %d", got)
	}

	// Final unlock
	lt.UnLock(blk)

	if got := lt.GetLockValLocked(blk); got != 0 {
		t.Errorf("expected lock count 0 after final unlock, got %d", got)
	}
}

func TestLockTableXLock(t *testing.T) {
	lt := dbtx.NewLockTable()
	blk := dbfile.NewBlockID("testfile", 0)
	ctx := context.Background()

	// Must acquire SLock first
	if err := lt.SLock(ctx, blk); err != nil {
		t.Fatalf("failed to acquire SLock: %v", err)
	}

	if got := lt.GetLockValLocked(blk); got != 1 {
		t.Errorf("expected lock count 1 after SLock, got %d", got)
	}

	// Upgrade to XLock
	if err := lt.XLock(ctx, blk); err != nil {
		t.Fatalf("failed to acquire XLock: %v", err)
	}

	if got := lt.GetLockValLocked(blk); got != -1 {
		t.Errorf("expected lock count -1 after XLock, got %d", got)
	}

	// Unlock XLock
	lt.UnLock(blk)

	if got := lt.GetLockValLocked(blk); got != 0 {
		t.Errorf("expected lock count 0 after unlock, got %d", got)
	}
}

func TestLockTableSLockBlockedByXLock(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		lt := dbtx.NewLockTable()
		blk := dbfile.NewBlockID("testfile", 0)
		ctx := context.Background()

		// Goroutine 1: Acquire SLock and upgrade to XLock
		if err := lt.SLock(ctx, blk); err != nil {
			t.Fatalf("failed to acquire SLock: %v", err)
		}
		if err := lt.XLock(ctx, blk); err != nil {
			t.Fatalf("failed to acquire XLock: %v", err)
		}

		// Goroutine 2: Try to acquire SLock (should block)
		var slockAcquired bool
		go func() {
			if err := lt.SLock(ctx, blk); err != nil {
				t.Errorf("failed to acquire SLock in goroutine: %v", err)
				return
			}
			slockAcquired = true
			lt.UnLock(blk)
		}()

		// Wait for goroutine to block
		synctest.Wait()

		// SLock should not be acquired yet
		if slockAcquired {
			t.Error("SLock should not be acquired while XLock is held")
		}

		// Release XLock
		lt.UnLock(blk)

		// Wait for goroutine to acquire SLock
		// goroutineの終了を待っている
		synctest.Wait()

		// SLock should now be acquired
		if !slockAcquired {
			t.Error("SLock should be acquired after XLock is released")
		}
	})
}

func TestLockTableXLockBlockedByMultipleSLocks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		lt := dbtx.NewLockTable()
		blk := dbfile.NewBlockID("testfile", 0)
		ctx := context.Background()

		// Acquire 2 SLocks
		if err := lt.SLock(ctx, blk); err != nil {
			t.Fatalf("failed to acquire first SLock: %v", err)
		}
		if err := lt.SLock(ctx, blk); err != nil {
			t.Fatalf("failed to acquire second SLock: %v", err)
		}

		if got := lt.GetLockValLocked(blk); got != 2 {
			t.Errorf("expected lock count 2, got %d", got)
		}

		// Try to upgrade to XLock (should block because there are 2 SLocks)
		var xlockAcquired bool
		go func() {
			if err := lt.XLock(ctx, blk); err != nil {
				t.Errorf("failed to acquire XLock in goroutine: %v", err)
				return
			}
			xlockAcquired = true
		}()

		// Wait for goroutine to block
		synctest.Wait()

		// XLock should not be acquired yet
		if xlockAcquired {
			t.Error("XLock should not be acquired while multiple SLocks are held")
		}

		if got := lt.GetLockValLocked(blk); got != 2 {
			t.Errorf("expected lock count 2 while XLock is blocked, got %d", got)
		}

		// Release one SLock (still 1 SLock remaining)
		lt.UnLock(blk)
		synctest.Wait()

		// XLock should still not be acquired
		if xlockAcquired {
			t.Error("XLock should not be acquired while 1 SLock is held")
		}

		if got := lt.GetLockValLocked(blk); got != 1 {
			t.Errorf("expected lock count 1 after one unlock, got %d", got)
		}

		// Release the last SLock
		lt.UnLock(blk)
		synctest.Wait()

		// XLock should now be acquired
		if !xlockAcquired {
			t.Error("XLock should be acquired after all SLocks are released")
		}

		if got := lt.GetLockValLocked(blk); got != -1 {
			t.Errorf("expected lock count -1 after XLock acquired, got %d", got)
		}

		// Clean up
		lt.UnLock(blk)
	})
}

func TestLockTableSLockTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		lt := dbtx.NewLockTable()
		blk := dbfile.NewBlockID("testfile", 0)
		ctx := context.Background()

		// Acquire XLock
		if err := lt.SLock(ctx, blk); err != nil {
			t.Fatalf("failed to acquire SLock: %v", err)
		}
		if err := lt.XLock(ctx, blk); err != nil {
			t.Fatalf("failed to acquire XLock: %v", err)
		}

		// Try to acquire SLock from another goroutine (should timeout)
		err := lt.SLock(ctx, blk)
		if err == nil {
			t.Error("expected timeout error when acquiring SLock blocked by XLock")
		}

		lt.UnLock(blk)
	})
}

func TestLockTableXLockTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		lt := dbtx.NewLockTable()
		blk := dbfile.NewBlockID("testfile", 0)
		ctx := context.Background()

		// Acquire 2 SLocks
		if err := lt.SLock(ctx, blk); err != nil {
			t.Fatalf("failed to acquire first SLock: %v", err)
		}
		if err := lt.SLock(ctx, blk); err != nil {
			t.Fatalf("failed to acquire second SLock: %v", err)
		}

		// Try to acquire XLock (should timeout because there are 2 SLocks)
		err := lt.XLock(ctx, blk)
		if err == nil {
			t.Error("expected timeout error when acquiring XLock blocked by multiple SLocks")
		}

		lt.UnLock(blk)
		lt.UnLock(blk)
	})
}

func TestLockTableConcurrentAccess(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		lt := dbtx.NewLockTable()
		blk := dbfile.NewBlockID("testfile", 0)
		ctx := context.Background()

		var wg sync.WaitGroup
		numGoroutines := 5

		// Multiple goroutines acquire and release SLocks
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				if err := lt.SLock(ctx, blk); err != nil {
					t.Errorf("goroutine %d: failed to acquire SLock: %v", id, err)
					return
				}
				lt.UnLock(blk)
			}(i)
		}

		wg.Wait()

		// All locks should be released (verified by no panics/errors)
	})
}

func TestLockTableUnlockNonExistentLock(t *testing.T) {
	lt := dbtx.NewLockTable()
	blk := dbfile.NewBlockID("testfile", 0)

	// Unlock without acquiring lock (should not panic)
	lt.UnLock(blk)

	// No error should occur
}

func TestLockTableMultipleBlocks(t *testing.T) {
	lt := dbtx.NewLockTable()
	blk1 := dbfile.NewBlockID("testfile", 0)
	blk2 := dbfile.NewBlockID("testfile", 1)
	ctx := context.Background()

	// Acquire locks on different blocks
	if err := lt.SLock(ctx, blk1); err != nil {
		t.Fatalf("failed to acquire SLock on blk1: %v", err)
	}
	if err := lt.SLock(ctx, blk2); err != nil {
		t.Fatalf("failed to acquire SLock on blk2: %v", err)
	}

	// Unlock blk1
	lt.UnLock(blk1)

	// Unlock blk2
	lt.UnLock(blk2)
}
