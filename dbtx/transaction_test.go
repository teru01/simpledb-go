package dbtx_test

import (
	"context"
	"os"
	"testing"

	"github.com/teru01/simpledb-go/dbbuffer"
	"github.com/teru01/simpledb-go/dbfile"
	"github.com/teru01/simpledb-go/dblog"
	"github.com/teru01/simpledb-go/dbtx"
)

func setupTestTransaction(t *testing.T) (*dbtx.Transaction, *dbfile.FileManager, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "transaction_test")
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

	bm := dbbuffer.NewBufferManager(fm, lm, 8)

	tx, err := dbtx.NewTransaction(fm, lm, bm)
	if err != nil {
		t.Fatalf("failed to create transaction: %v", err)
	}

	cleanup := func() {
		dirFile.Close()
		os.RemoveAll(dir)
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
