package dbbuffer_test

import (
	"context"
	"os"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/teru01/simpledb-go/dbbuffer"
	"github.com/teru01/simpledb-go/dbfile"
	"github.com/teru01/simpledb-go/dblog"
)

func setupTestBufferManager(t *testing.T, numBuffers int) (*dbbuffer.BufferManager, *dbfile.FileManager, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "buffermanager_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	dirFile, err := os.Open(dir)
	if err != nil {
		t.Fatalf("failed to open temp dir: %v", err)
	}

	fm, err := dbfile.NewFileManager(dirFile, 400)
	if err != nil {
		t.Fatalf("failed to create file manager: %v", err)
	}

	lm, err := dblog.NewLogManager(fm, "test.log")
	if err != nil {
		t.Fatalf("failed to create log manager: %v", err)
	}

	bm := dbbuffer.NewBufferManager(fm, lm, numBuffers)

	cleanup := func() {
		dirFile.Close()
		os.RemoveAll(dir)
	}

	return bm, fm, cleanup
}

func TestBufferManagerAvailable(t *testing.T) {
	bm, _, cleanup := setupTestBufferManager(t, 3)
	defer cleanup()

	if available := bm.Available(); available != 3 {
		t.Errorf("expected 3 available buffers, got %d", available)
	}
}

func TestBufferManagerPinUnpin(t *testing.T) {
	bm, fm, cleanup := setupTestBufferManager(t, 3)
	defer cleanup()

	// Create the file by appending a block first
	_, err := fm.Append("testfile")
	if err != nil {
		t.Fatalf("failed to append block: %v", err)
	}

	blk := dbfile.NewBlockID("testfile", 0)
	ctx := context.Background()

	// Pin a buffer
	buf, err := bm.Pin(ctx, blk)
	if err != nil {
		t.Fatalf("failed to pin buffer: %v", err)
	}

	if buf == nil {
		t.Fatal("expected non-nil buffer")
	}

	if available := bm.Available(); available != 2 {
		t.Errorf("expected 2 available buffers after pin, got %d", available)
	}

	// Unpin the buffer
	bm.Unpin(buf)

	if available := bm.Available(); available != 3 {
		t.Errorf("expected 3 available buffers after unpin, got %d", available)
	}
}

func TestBufferManagerMultiplePins(t *testing.T) {
	bm, fm, cleanup := setupTestBufferManager(t, 3)
	defer cleanup()

	_, err := fm.Append("testfile")
	if err != nil {
		t.Fatalf("failed to append block: %v", err)
	}

	blk := dbfile.NewBlockID("testfile", 0)
	ctx := context.Background()

	// Pin same block twice
	buf1, err := bm.Pin(ctx, blk)
	if err != nil {
		t.Fatalf("failed to pin buffer first time: %v", err)
	}

	buf2, err := bm.Pin(ctx, blk)
	if err != nil {
		t.Fatalf("failed to pin buffer second time: %v", err)
	}

	// Should get the same buffer
	if buf1 != buf2 {
		t.Error("expected same buffer for same block")
	}

	// Should still have 2 available since same buffer is pinned twice
	if available := bm.Available(); available != 2 {
		t.Errorf("expected 2 available buffers, got %d", available)
	}

	// First unpin
	bm.Unpin(buf1)
	if available := bm.Available(); available != 2 {
		t.Errorf("expected 2 available buffers after first unpin, got %d", available)
	}

	// Second unpin should make buffer available
	bm.Unpin(buf2)
	if available := bm.Available(); available != 3 {
		t.Errorf("expected 3 available buffers after second unpin, got %d", available)
	}
}

func TestBufferManagerPinDifferentBlocks(t *testing.T) {
	bm, fm, cleanup := setupTestBufferManager(t, 3)
	defer cleanup()

	// Create blocks
	for i := 0; i < 3; i++ {
		if _, err := fm.Append("testfile"); err != nil {
			t.Fatalf("failed to append block %d: %v", i, err)
		}
	}

	blk1 := dbfile.NewBlockID("testfile", 0)
	blk2 := dbfile.NewBlockID("testfile", 1)
	blk3 := dbfile.NewBlockID("testfile", 2)
	ctx := context.Background()

	// Pin three different blocks
	buf1, err := bm.Pin(ctx, blk1)
	if err != nil {
		t.Fatalf("failed to pin blk1: %v", err)
	}

	buf2, err := bm.Pin(ctx, blk2)
	if err != nil {
		t.Fatalf("failed to pin blk2: %v", err)
	}

	buf3, err := bm.Pin(ctx, blk3)
	if err != nil {
		t.Fatalf("failed to pin blk3: %v", err)
	}

	// All buffers should be different
	if buf1 == buf2 || buf2 == buf3 || buf1 == buf3 {
		t.Error("expected different buffers for different blocks")
	}

	// No buffers available
	if available := bm.Available(); available != 0 {
		t.Errorf("expected 0 available buffers, got %d", available)
	}
}

func TestBufferManagerBufferReplacement(t *testing.T) {
	bm, fm, cleanup := setupTestBufferManager(t, 2)
	defer cleanup()

	// Create blocks
	for i := 0; i < 3; i++ {
		if _, err := fm.Append("testfile"); err != nil {
			t.Fatalf("failed to append block %d: %v", i, err)
		}
	}

	blk1 := dbfile.NewBlockID("testfile", 0)
	blk2 := dbfile.NewBlockID("testfile", 1)
	blk3 := dbfile.NewBlockID("testfile", 2)
	ctx := context.Background()

	// Pin and unpin blk1
	buf1, err := bm.Pin(ctx, blk1)
	if err != nil {
		t.Fatalf("failed to pin blk1: %v", err)
	}
	bm.Unpin(buf1)

	// Pin and keep blk2 pinned
	buf2, err := bm.Pin(ctx, blk2)
	if err != nil {
		t.Fatalf("failed to pin blk2: %v", err)
	}

	// Pin blk3 should reuse buf1's buffer (unpinned)
	buf3, err := bm.Pin(ctx, blk3)
	if err != nil {
		t.Fatalf("failed to pin blk3: %v", err)
	}

	if buf3 == buf2 {
		t.Error("blk3 should not use the same buffer as pinned blk2")
	}

	bm.Unpin(buf2)
	bm.Unpin(buf3)
}

func TestBufferManagerFlushAll(t *testing.T) {
	bm, fm, cleanup := setupTestBufferManager(t, 3)
	defer cleanup()

	// Create blocks
	for i := 0; i < 2; i++ {
		if _, err := fm.Append("testfile"); err != nil {
			t.Fatalf("failed to append block %d: %v", i, err)
		}
	}

	blk1 := dbfile.NewBlockID("testfile", 0)
	blk2 := dbfile.NewBlockID("testfile", 1)
	ctx := context.Background()

	// Pin buffers and modify them
	buf1, err := bm.Pin(ctx, blk1)
	if err != nil {
		t.Fatalf("failed to pin blk1: %v", err)
	}
	buf1.SetModified(1, 100)

	buf2, err := bm.Pin(ctx, blk2)
	if err != nil {
		t.Fatalf("failed to pin blk2: %v", err)
	}
	buf2.SetModified(2, 200)

	// FlushAll for txNum=1 should flush only buf1
	if err := bm.FlushAll(1); err != nil {
		t.Fatalf("failed to flush all: %v", err)
	}

	// After flush, buf1's txNum should be -1
	if buf1.ModifyingTx() != 0 {
		t.Errorf("expected buf1 txNum to be 0 after flush, got %d", buf1.ModifyingTx())
	}

	// buf2 should still have txNum=2
	if buf2.ModifyingTx() != 2 {
		t.Errorf("expected buf2 txNum to be 2, got %d", buf2.ModifyingTx())
	}

	bm.Unpin(buf1)
	bm.Unpin(buf2)
}

func TestBufferManagerConcurrentPinUnpin(t *testing.T) {
	bm, fm, cleanup := setupTestBufferManager(t, 3)
	defer cleanup()

	// Create blocks
	for i := 0; i < 3; i++ {
		if _, err := fm.Append("testfile"); err != nil {
			t.Fatalf("failed to append block %d: %v", i, err)
		}
	}

	var wg sync.WaitGroup
	numGoroutines := 10
	numOperations := 20

	ctx := context.Background()
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				blk := dbfile.NewBlockID("testfile", id%3)
				buf, err := bm.Pin(ctx, blk)
				if err != nil {
					t.Errorf("goroutine %d: failed to pin: %v", id, err)
					return
				}
				// Do some work
				bm.Unpin(buf)
			}
		}(i)
	}

	wg.Wait()

	// After all operations, all buffers should be available
	if available := bm.Available(); available != 3 {
		t.Errorf("expected 3 available buffers after concurrent operations, got %d", available)
	}
}

func TestBufferManagerPinTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		bm, fm, cleanup := setupTestBufferManager(t, 1)
		defer cleanup()

		// Create blocks
		for i := 0; i < 2; i++ {
			if _, err := fm.Append("testfile"); err != nil {
				t.Fatalf("failed to append block %d: %v", i, err)
			}
		}

		blk1 := dbfile.NewBlockID("testfile", 0)
		blk2 := dbfile.NewBlockID("testfile", 1)
		ctx := context.Background()

		// Pin the only buffer
		buf1, err := bm.Pin(ctx, blk1)
		if err != nil {
			t.Fatalf("failed to pin blk1: %v", err)
		}

		// Try to pin another block - should timeout since no buffers available
		_, err = bm.Pin(ctx, blk2)
		if err == nil {
			t.Error("expected error when pinning with no available buffers")
		}

		bm.Unpin(buf1)
	})
}
