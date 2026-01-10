package dbtx_test

import (
	"context"
	"os"
	"sync"
	"testing"

	"github.com/teru01/simpledb-go/dbbuffer"
	"github.com/teru01/simpledb-go/dbfile"
	"github.com/teru01/simpledb-go/dblog"
	"github.com/teru01/simpledb-go/dbtx"
)

func setupTestBufferManager(t *testing.T, numBuffers int) (*dbbuffer.BufferManager, *dbfile.FileManager, *dblog.LogManager, func()) {
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

	return bm, fm, lm, cleanup
}

func setupTestTransaction(t *testing.T) (*dbtx.Transaction, *dbfile.FileManager, func()) {
	t.Helper()
	bm, fm, lm, cleanup := setupTestBufferManager(t, 8)

	tx, err := dbtx.NewTransaction(fm, lm, bm)
	if err != nil {
		t.Fatalf("failed to create transaction: %v", err)
	}
	return tx, fm, cleanup
}

func TestTransactionPinSetIntGetInt(t *testing.T) {
	tx, fm, cleanup := setupTestTransaction(t)
	defer cleanup()

	ctx := context.Background()

	// Create a block
	blk, err := fm.Append("testfile")
	if err != nil {
		t.Fatalf("failed to append block: %v", err)
	}

	// Pin the block
	if err := tx.Pin(ctx, blk); err != nil {
		t.Fatalf("failed to pin block: %v", err)
	}

	// Set an integer value
	if err := tx.SetInt(ctx, blk, 0, 42, true); err != nil {
		t.Fatalf("failed to set int: %v", err)
	}

	// Get the integer value
	val, err := tx.GetInt(ctx, blk, 0)
	if err != nil {
		t.Fatalf("failed to get int: %v", err)
	}

	if val != 42 {
		t.Errorf("expected value 42, got %d", val)
	}

	// Unpin the block
	if err := tx.UnPin(blk); err != nil {
		t.Fatalf("failed to unpin block: %v", err)
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}
}

func TestTransactionPinSetStringGetString(t *testing.T) {
	tx, fm, cleanup := setupTestTransaction(t)
	defer cleanup()

	ctx := context.Background()

	blk, err := fm.Append("testfile")
	if err != nil {
		t.Fatalf("failed to append block: %v", err)
	}

	// Pin the block
	if err := tx.Pin(ctx, blk); err != nil {
		t.Fatalf("failed to pin block: %v", err)
	}

	answer := "hello-world"
	if err := tx.SetString(ctx, blk, 0, answer, true); err != nil {
		t.Fatalf("failed to set string: %v", err)
	}

	val, err := tx.GetString(ctx, blk, 0)
	if err != nil {
		t.Fatalf("failed to get int: %v", err)
	}

	if val != answer {
		t.Errorf("expected value %s, got %s", answer, val)
	}

	if err := tx.UnPin(blk); err != nil {
		t.Fatalf("failed to unpin block: %v", err)
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}
}

func TestTransactionGetStringParallel(t *testing.T) {
	dbtx.ResetTxNum()
	bm, fm, lm, cleanup := setupTestBufferManager(t, 8)
	defer cleanup()
	ctx := context.Background()

	blk, err := fm.Append("testfile")
	if err != nil {
		t.Fatalf("failed to append block: %v", err)
	}

	tx, err := dbtx.NewTransaction(fm, lm, bm)
	if err != nil {
		t.Fatalf("failed to create transaction: %v", err)
	}

	// Pin the block
	if err := tx.Pin(ctx, blk); err != nil {
		t.Fatalf("failed to pin block: %v", err)
	}

	answer := "parallel-test"
	if err := tx.SetString(ctx, blk, 0, answer, true); err != nil {
		t.Fatalf("failed to set string: %v", err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	txCount := 10
	var wg sync.WaitGroup
	for i := range txCount {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			tx, err := dbtx.NewTransaction(fm, lm, bm)
			if err != nil {
				t.Errorf("failed to create transaction: %v", err)
			}
			// Pin the block
			if err := tx.Pin(ctx, blk); err != nil {
				t.Errorf("failed to pin block: %v", err)
			}
			val, err := tx.GetString(ctx, blk, 0)
			if err != nil {
				t.Errorf("failed to get string: %v", err)
			}
			if val != answer {
				t.Errorf("expected value %s, got %s", answer, val)
			}
			if err := tx.Commit(); err != nil {
				t.Errorf("failed to commit: %v", err)
			}
		}(i)
	}
	wg.Wait()
	next := dbtx.NextTxNum()
	if next != uint64(txCount+2) {
		t.Fatalf("transaction number mismatch, expected %d actual %d", txCount+2, next)
	}
}

func TestTransactionRollback(t *testing.T) {
	dbtx.ResetTxNum()
	bm, fm, lm, cleanup := setupTestBufferManager(t, 8)
	defer cleanup()
	ctx := context.Background()

	blk, err := fm.Append("testfile")
	if err != nil {
		t.Fatalf("failed to append block: %v", err)
	}

	// First transaction: set initial value and commit
	tx1, err := dbtx.NewTransaction(fm, lm, bm)
	if err != nil {
		t.Fatalf("failed to create transaction: %v", err)
	}

	if err := tx1.Pin(ctx, blk); err != nil {
		t.Fatalf("failed to pin block: %v", err)
	}

	initialValue := 100
	if err := tx1.SetInt(ctx, blk, 0, initialValue, true); err != nil {
		t.Fatalf("failed to set int: %v", err)
	}

	if err := tx1.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	// Second transaction: change value but rollback
	tx2, err := dbtx.NewTransaction(fm, lm, bm)
	if err != nil {
		t.Fatalf("failed to create transaction: %v", err)
	}

	if err := tx2.Pin(ctx, blk); err != nil {
		t.Fatalf("failed to pin block: %v", err)
	}

	if err := tx2.SetInt(ctx, blk, 0, 999, true); err != nil {
		t.Fatalf("failed to set int: %v", err)
	}

	// Rollback the change
	if err := tx2.Rollback(ctx); err != nil {
		t.Fatalf("failed to rollback: %v", err)
	}

	// Third transaction: verify value is reverted to initial
	tx3, err := dbtx.NewTransaction(fm, lm, bm)
	if err != nil {
		t.Fatalf("failed to create transaction: %v", err)
	}

	if err := tx3.Pin(ctx, blk); err != nil {
		t.Fatalf("failed to pin block: %v", err)
	}

	val, err := tx3.GetInt(ctx, blk, 0)
	if err != nil {
		t.Fatalf("failed to get int: %v", err)
	}

	if val != initialValue {
		t.Errorf("expected value %d after rollback, got %d", initialValue, val)
	}

	if err := tx3.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}
}

func TestTransactionMultipleBlocks(t *testing.T) {
	tx, fm, cleanup := setupTestTransaction(t)
	defer cleanup()
	ctx := context.Background()

	// Create multiple blocks
	blk1, err := fm.Append("testfile")
	if err != nil {
		t.Fatalf("failed to append block 1: %v", err)
	}

	blk2, err := fm.Append("testfile")
	if err != nil {
		t.Fatalf("failed to append block 2: %v", err)
	}

	blk3, err := fm.Append("testfile")
	if err != nil {
		t.Fatalf("failed to append block 3: %v", err)
	}

	// Pin all blocks
	if err := tx.Pin(ctx, blk1); err != nil {
		t.Fatalf("failed to pin block 1: %v", err)
	}
	if err := tx.Pin(ctx, blk2); err != nil {
		t.Fatalf("failed to pin block 2: %v", err)
	}
	if err := tx.Pin(ctx, blk3); err != nil {
		t.Fatalf("failed to pin block 3: %v", err)
	}

	// Set different values in each block
	if err := tx.SetInt(ctx, blk1, 0, 111, true); err != nil {
		t.Fatalf("failed to set int in block 1: %v", err)
	}
	if err := tx.SetInt(ctx, blk2, 0, 222, true); err != nil {
		t.Fatalf("failed to set int in block 2: %v", err)
	}
	if err := tx.SetInt(ctx, blk3, 0, 333, true); err != nil {
		t.Fatalf("failed to set int in block 3: %v", err)
	}

	// Verify all values
	val1, err := tx.GetInt(ctx, blk1, 0)
	if err != nil {
		t.Fatalf("failed to get int from block 1: %v", err)
	}
	if val1 != 111 {
		t.Errorf("expected value 111 in block 1, got %d", val1)
	}

	val2, err := tx.GetInt(ctx, blk2, 0)
	if err != nil {
		t.Fatalf("failed to get int from block 2: %v", err)
	}
	if val2 != 222 {
		t.Errorf("expected value 222 in block 2, got %d", val2)
	}

	val3, err := tx.GetInt(ctx, blk3, 0)
	if err != nil {
		t.Fatalf("failed to get int from block 3: %v", err)
	}
	if val3 != 333 {
		t.Errorf("expected value 333 in block 3, got %d", val3)
	}

	// Unpin all blocks
	if err := tx.UnPin(blk1); err != nil {
		t.Fatalf("failed to unpin block 1: %v", err)
	}
	if err := tx.UnPin(blk2); err != nil {
		t.Fatalf("failed to unpin block 2: %v", err)
	}
	if err := tx.UnPin(blk3); err != nil {
		t.Fatalf("failed to unpin block 3: %v", err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}
}

func TestTransactionBufferPinUnpin(t *testing.T) {
	tx, fm, cleanup := setupTestTransaction(t)
	defer cleanup()
	ctx := context.Background()

	blk, err := fm.Append("testfile")
	if err != nil {
		t.Fatalf("failed to append block: %v", err)
	}

	// Pin the same block multiple times
	if err := tx.Pin(ctx, blk); err != nil {
		t.Fatalf("failed to pin block first time: %v", err)
	}
	if err := tx.Pin(ctx, blk); err != nil {
		t.Fatalf("failed to pin block second time: %v", err)
	}
	if err := tx.Pin(ctx, blk); err != nil {
		t.Fatalf("failed to pin block third time: %v", err)
	}

	// Set a value
	if err := tx.SetInt(ctx, blk, 0, 777, true); err != nil {
		t.Fatalf("failed to set int: %v", err)
	}

	// Unpin twice (should still be pinned once)
	if err := tx.UnPin(blk); err != nil {
		t.Fatalf("failed to unpin block first time: %v", err)
	}
	if err := tx.UnPin(blk); err != nil {
		t.Fatalf("failed to unpin block second time: %v", err)
	}

	// Should still be able to get the value
	val, err := tx.GetInt(ctx, blk, 0)
	if err != nil {
		t.Fatalf("failed to get int: %v", err)
	}
	if val != 777 {
		t.Errorf("expected value 777, got %d", val)
	}

	// Final unpin
	if err := tx.UnPin(blk); err != nil {
		t.Fatalf("failed to unpin block third time: %v", err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}
}

func TestTransactionGetIntWithoutPin(t *testing.T) {
	tx, fm, cleanup := setupTestTransaction(t)
	defer cleanup()
	ctx := context.Background()

	blk, err := fm.Append("testfile")
	if err != nil {
		t.Fatalf("failed to append block: %v", err)
	}

	// Try to get int without pinning - should error
	_, err = tx.GetInt(ctx, blk, 0)
	if err == nil {
		t.Error("expected error when getting int without pin")
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}
}

func TestTransactionSetIntWithoutPin(t *testing.T) {
	tx, fm, cleanup := setupTestTransaction(t)
	defer cleanup()
	ctx := context.Background()

	blk, err := fm.Append("testfile")
	if err != nil {
		t.Fatalf("failed to append block: %v", err)
	}

	// Try to set int without pinning - should error
	err = tx.SetInt(ctx, blk, 0, 42, true)
	if err == nil {
		t.Error("expected error when setting int without pin")
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}
}

func TestTransactionSizeAndAppend(t *testing.T) {
	tx, _, cleanup := setupTestTransaction(t)
	defer cleanup()
	ctx := context.Background()

	fileName := "testfile"

	// Initially, file should have 0 blocks
	size, err := tx.Size(ctx, fileName)
	if err != nil {
		t.Fatalf("failed to get size: %v", err)
	}
	if size != 0 {
		t.Errorf("expected size 0, got %d", size)
	}

	// Append a block
	blk1, err := tx.Append(ctx, fileName)
	if err != nil {
		t.Fatalf("failed to append block: %v", err)
	}
	if blk1.BlockNum() != 0 {
		t.Errorf("expected block number 0, got %d", blk1.BlockNum())
	}

	// Size should now be 1
	size, err = tx.Size(ctx, fileName)
	if err != nil {
		t.Fatalf("failed to get size: %v", err)
	}
	if size != 1 {
		t.Errorf("expected size 1, got %d", size)
	}

	// Append another block
	blk2, err := tx.Append(ctx, fileName)
	if err != nil {
		t.Fatalf("failed to append block: %v", err)
	}
	if blk2.BlockNum() != 1 {
		t.Errorf("expected block number 1, got %d", blk2.BlockNum())
	}

	// Size should now be 2
	size, err = tx.Size(ctx, fileName)
	if err != nil {
		t.Fatalf("failed to get size: %v", err)
	}
	if size != 2 {
		t.Errorf("expected size 2, got %d", size)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}
}

func TestTransactionAvailableBuffs(t *testing.T) {
	bm, fm, lm, cleanup := setupTestBufferManager(t, 3)
	defer cleanup()
	ctx := context.Background()

	tx, err := dbtx.NewTransaction(fm, lm, bm)
	if err != nil {
		t.Fatalf("failed to create transaction: %v", err)
	}

	// Initially all buffers should be available
	if avail := tx.AvailableBuffs(); avail != 3 {
		t.Errorf("expected 3 available buffers, got %d", avail)
	}

	// Create and pin a block
	blk1, err := fm.Append("testfile")
	if err != nil {
		t.Fatalf("failed to append block: %v", err)
	}
	if err := tx.Pin(ctx, blk1); err != nil {
		t.Fatalf("failed to pin block 1: %v", err)
	}

	// Should have 2 available buffers
	if avail := tx.AvailableBuffs(); avail != 2 {
		t.Errorf("expected 2 available buffers, got %d", avail)
	}

	// Pin another block
	blk2, err := fm.Append("testfile")
	if err != nil {
		t.Fatalf("failed to append block: %v", err)
	}
	if err := tx.Pin(ctx, blk2); err != nil {
		t.Fatalf("failed to pin block 2: %v", err)
	}

	// Should have 1 available buffer
	if avail := tx.AvailableBuffs(); avail != 1 {
		t.Errorf("expected 1 available buffer, got %d", avail)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}
}
