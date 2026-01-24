package dbmetadata_test

import (
	"context"
	"os"
	"testing"

	"github.com/teru01/simpledb-go/dbbuffer"
	"github.com/teru01/simpledb-go/dbfile"
	"github.com/teru01/simpledb-go/dblog"
	"github.com/teru01/simpledb-go/dbmetadata"
	"github.com/teru01/simpledb-go/dbrecord"
	"github.com/teru01/simpledb-go/dbtx"
)

func setupTestStatManager(t *testing.T) (*dbmetadata.StatManager, *dbmetadata.TableManager, *dbtx.Transaction, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "stat_manager_test")
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

	tm := dbmetadata.NewTableManager(true, tx)
	ctx := context.Background()
	sm, err := dbmetadata.NewStatManager(ctx, tm, tx)
	if err != nil {
		t.Fatalf("failed to create stat manager: %v", err)
	}

	cleanup := func() {
		dirFile.Close()
		os.RemoveAll(dir)
	}

	return sm, tm, tx, cleanup
}

func TestStatManagerGetStatInfo(t *testing.T) {
	sm, tm, tx, cleanup := setupTestStatManager(t)
	defer cleanup()

	ctx := context.Background()

	// Create a test table
	schema := dbrecord.NewSchema()
	schema.AddIntField("id")
	schema.AddStringField("name", 20)

	err := tm.CreateTable(ctx, "students", schema, tx)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	layout, err := tm.GetLayout(ctx, "students", tx)
	if err != nil {
		t.Fatalf("failed to get layout: %v", err)
	}

	// Insert some test data
	ts, err := dbrecord.NewTableScan(ctx, tx, "students", layout)
	if err != nil {
		t.Fatalf("failed to create table scan: %v", err)
	}

	for i := 0; i < 5; i++ {
		if err := ts.Insert(ctx); err != nil {
			t.Fatalf("failed to insert record: %v", err)
		}
		if err := ts.SetInt(ctx, "id", i); err != nil {
			t.Fatalf("failed to set id: %v", err)
		}
		if err := ts.SetString(ctx, "name", "student"); err != nil {
			t.Fatalf("failed to set name: %v", err)
		}
	}

	if err := ts.Close(); err != nil {
		t.Fatalf("failed to close table scan: %v", err)
	}

	// Get statistics
	statInfo, err := sm.GetStatInfo(ctx, "students", layout, tx)
	if err != nil {
		t.Fatalf("failed to get stat info: %v", err)
	}

	if statInfo.RecordsOutput() != 5 {
		t.Errorf("expected 5 records, got %d", statInfo.RecordsOutput())
	}

	if statInfo.BlockAccessed() <= 0 {
		t.Errorf("expected positive number of blocks, got %d", statInfo.BlockAccessed())
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}
}

func TestStatManagerMultipleTables(t *testing.T) {
	sm, tm, tx, cleanup := setupTestStatManager(t)
	defer cleanup()

	ctx := context.Background()

	// Create first table
	schema1 := dbrecord.NewSchema()
	schema1.AddIntField("id")

	err := tm.CreateTable(ctx, "table1", schema1, tx)
	if err != nil {
		t.Fatalf("failed to create table1: %v", err)
	}

	layout1, err := tm.GetLayout(ctx, "table1", tx)
	if err != nil {
		t.Fatalf("failed to get layout1: %v", err)
	}

	// Insert records into first table
	ts1, err := dbrecord.NewTableScan(ctx, tx, "table1", layout1)
	if err != nil {
		t.Fatalf("failed to create table scan for table1: %v", err)
	}

	for i := 0; i < 3; i++ {
		if err := ts1.Insert(ctx); err != nil {
			t.Fatalf("failed to insert record: %v", err)
		}
		if err := ts1.SetInt(ctx, "id", i); err != nil {
			t.Fatalf("failed to set id: %v", err)
		}
	}
	if err := ts1.Close(); err != nil {
		t.Fatalf("failed to close table scan: %v", err)
	}

	// Create second table
	schema2 := dbrecord.NewSchema()
	schema2.AddIntField("value")

	err = tm.CreateTable(ctx, "table2", schema2, tx)
	if err != nil {
		t.Fatalf("failed to create table2: %v", err)
	}

	layout2, err := tm.GetLayout(ctx, "table2", tx)
	if err != nil {
		t.Fatalf("failed to get layout2: %v", err)
	}

	// Insert records into second table
	ts2, err := dbrecord.NewTableScan(ctx, tx, "table2", layout2)
	if err != nil {
		t.Fatalf("failed to create table scan for table2: %v", err)
	}

	for i := 0; i < 7; i++ {
		if err := ts2.Insert(ctx); err != nil {
			t.Fatalf("failed to insert record: %v", err)
		}
		if err := ts2.SetInt(ctx, "value", i); err != nil {
			t.Fatalf("failed to set value: %v", err)
		}
	}
	if err := ts2.Close(); err != nil {
		t.Fatalf("failed to close table scan: %v", err)
	}

	// Get statistics for both tables
	stat1, err := sm.GetStatInfo(ctx, "table1", layout1, tx)
	if err != nil {
		t.Fatalf("failed to get stat info for table1: %v", err)
	}

	stat2, err := sm.GetStatInfo(ctx, "table2", layout2, tx)
	if err != nil {
		t.Fatalf("failed to get stat info for table2: %v", err)
	}

	if stat1.RecordsOutput() != 3 {
		t.Errorf("expected 3 records in table1, got %d", stat1.RecordsOutput())
	}

	if stat2.RecordsOutput() != 7 {
		t.Errorf("expected 7 records in table2, got %d", stat2.RecordsOutput())
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}
}

func TestStatManagerEmptyTable(t *testing.T) {
	sm, tm, tx, cleanup := setupTestStatManager(t)
	defer cleanup()

	ctx := context.Background()

	// Create an empty table
	schema := dbrecord.NewSchema()
	schema.AddIntField("id")

	err := tm.CreateTable(ctx, "empty_table", schema, tx)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	layout, err := tm.GetLayout(ctx, "empty_table", tx)
	if err != nil {
		t.Fatalf("failed to get layout: %v", err)
	}

	// Get statistics for empty table
	statInfo, err := sm.GetStatInfo(ctx, "empty_table", layout, tx)
	if err != nil {
		t.Fatalf("failed to get stat info: %v", err)
	}

	if statInfo.RecordsOutput() != 0 {
		t.Errorf("expected 0 records for empty table, got %d", statInfo.RecordsOutput())
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}
}
