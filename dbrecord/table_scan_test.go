package dbrecord_test

import (
	"context"
	"os"
	"testing"

	"github.com/teru01/simpledb-go/dbbuffer"
	"github.com/teru01/simpledb-go/dbfile"
	"github.com/teru01/simpledb-go/dblog"
	"github.com/teru01/simpledb-go/dbrecord"
	"github.com/teru01/simpledb-go/dbtx"
)

func setupTestTableScan(t *testing.T) (*dbtx.Transaction, *dbrecord.Layout, string, func()) {
	return setupTestTableScanWithBlockSize(t, 400)
}

func setupTestTableScanWithBlockSize(t *testing.T, blockSize int) (*dbtx.Transaction, *dbrecord.Layout, string, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "tablescan_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	dirFile, err := os.Open(dir)
	if err != nil {
		t.Fatalf("failed to open temp dir: %v", err)
	}

	fm, err := dbfile.NewFileManager(dirFile, blockSize)
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

	schema := dbrecord.NewSchema()
	schema.AddIntField("id")
	schema.AddStringField("name", 20)
	schema.AddIntField("age")

	layout := dbrecord.NewLayout(schema)

	cleanup := func() {
		tx.Commit()
		dirFile.Close()
		os.RemoveAll(dir)
	}

	return tx, layout, "testtable", cleanup
}

func TestTableScanInsertAndGetInt(t *testing.T) {
	tx, layout, tableName, cleanup := setupTestTableScan(t)
	defer cleanup()

	ctx := context.Background()
	ts, err := dbrecord.NewTableScan(ctx, tx, tableName, layout)
	if err != nil {
		t.Fatalf("failed to create table scan: %v", err)
	}

	// Insert and set int value
	if err := ts.Insert(ctx); err != nil {
		t.Fatalf("failed to insert: %v", err)
	}
	if err := ts.SetInt(ctx, "id", 100); err != nil {
		t.Fatalf("failed to set int: %v", err)
	}

	// Reset to first record
	if err := ts.SetStateToBeforeFirst(ctx); err != nil {
		t.Fatalf("failed to reset: %v", err)
	}
	ok, err := ts.Next(ctx)
	if err != nil || !ok {
		t.Fatalf("failed to move to next: %v", err)
	}

	// Get and verify int value
	val, err := ts.GetInt(ctx, "id")
	if err != nil {
		t.Fatalf("failed to get int: %v", err)
	}
	if val != 100 {
		t.Errorf("expected 100, got %d", val)
	}
}

func TestTableScanInsertAndGetString(t *testing.T) {
	tx, layout, tableName, cleanup := setupTestTableScan(t)
	defer cleanup()

	ctx := context.Background()
	ts, err := dbrecord.NewTableScan(ctx, tx, tableName, layout)
	if err != nil {
		t.Fatalf("failed to create table scan: %v", err)
	}

	// Insert and set string value
	if err := ts.Insert(ctx); err != nil {
		t.Fatalf("failed to insert: %v", err)
	}
	if err := ts.SetString(ctx, "name", "Alice"); err != nil {
		t.Fatalf("failed to set string: %v", err)
	}

	// Reset to first record
	if err := ts.SetStateToBeforeFirst(ctx); err != nil {
		t.Fatalf("failed to reset: %v", err)
	}
	ok, err := ts.Next(ctx)
	if err != nil || !ok {
		t.Fatalf("failed to move to next: %v", err)
	}

	// Get and verify string value
	val, err := ts.GetString(ctx, "name")
	if err != nil {
		t.Fatalf("failed to get string: %v", err)
	}
	if val != "Alice" {
		t.Errorf("expected 'Alice', got '%s'", val)
	}
}

func TestTableScanInsertMultipleFields(t *testing.T) {
	tx, layout, tableName, cleanup := setupTestTableScan(t)
	defer cleanup()

	ctx := context.Background()
	ts, err := dbrecord.NewTableScan(ctx, tx, tableName, layout)
	if err != nil {
		t.Fatalf("failed to create table scan: %v", err)
	}

	// Insert and set multiple fields
	if err := ts.Insert(ctx); err != nil {
		t.Fatalf("failed to insert: %v", err)
	}
	if err := ts.SetInt(ctx, "id", 42); err != nil {
		t.Fatalf("failed to set id: %v", err)
	}
	if err := ts.SetString(ctx, "name", "Bob"); err != nil {
		t.Fatalf("failed to set name: %v", err)
	}
	if err := ts.SetInt(ctx, "age", 30); err != nil {
		t.Fatalf("failed to set age: %v", err)
	}

	// Reset to first record
	if err := ts.SetStateToBeforeFirst(ctx); err != nil {
		t.Fatalf("failed to reset: %v", err)
	}
	ok, err := ts.Next(ctx)
	if err != nil || !ok {
		t.Fatalf("failed to move to next: %v", err)
	}

	// Verify all fields
	id, err := ts.GetInt(ctx, "id")
	if err != nil {
		t.Fatalf("failed to get id: %v", err)
	}
	if id != 42 {
		t.Errorf("expected id=42, got %d", id)
	}

	name, err := ts.GetString(ctx, "name")
	if err != nil {
		t.Fatalf("failed to get name: %v", err)
	}
	if name != "Bob" {
		t.Errorf("expected name='Bob', got '%s'", name)
	}

	age, err := ts.GetInt(ctx, "age")
	if err != nil {
		t.Fatalf("failed to get age: %v", err)
	}
	if age != 30 {
		t.Errorf("expected age=30, got %d", age)
	}
}

func TestTableScanDelete(t *testing.T) {
	tx, layout, tableName, cleanup := setupTestTableScan(t)
	defer cleanup()

	ctx := context.Background()
	ts, err := dbrecord.NewTableScan(ctx, tx, tableName, layout)
	if err != nil {
		t.Fatalf("failed to create table scan: %v", err)
	}

	// Insert a record
	if err := ts.Insert(ctx); err != nil {
		t.Fatalf("failed to insert: %v", err)
	}
	if err := ts.SetInt(ctx, "id", 1); err != nil {
		t.Fatalf("failed to set int: %v", err)
	}

	// Delete the record
	if err := ts.Delete(ctx); err != nil {
		t.Fatalf("failed to delete: %v", err)
	}

	// Verify no records exist
	if err := ts.SetStateToBeforeFirst(ctx); err != nil {
		t.Fatalf("failed to reset: %v", err)
	}
	ok, err := ts.Next(ctx)
	if err != nil {
		t.Fatalf("failed to move to next: %v", err)
	}
	if ok {
		t.Errorf("expected no records after delete, but found one")
	}
}

func TestTableScanDeleteIntField(t *testing.T) {
	tx, layout, tableName, cleanup := setupTestTableScan(t)
	defer cleanup()

	ctx := context.Background()
	ts, err := dbrecord.NewTableScan(ctx, tx, tableName, layout)
	if err != nil {
		t.Fatalf("failed to create table scan: %v", err)
	}

	// Insert multiple records
	for i := 1; i <= 3; i++ {
		if err := ts.Insert(ctx); err != nil {
			t.Fatalf("failed to insert: %v", err)
		}
		if err := ts.SetInt(ctx, "id", i*10); err != nil {
			t.Fatalf("failed to set int: %v", err)
		}
	}

	// Delete second record
	if err := ts.SetStateToBeforeFirst(ctx); err != nil {
		t.Fatalf("failed to reset: %v", err)
	}
	ts.Next(ctx)
	ts.Next(ctx)
	if err := ts.Delete(ctx); err != nil {
		t.Fatalf("failed to delete: %v", err)
	}

	// Verify remaining records
	if err := ts.SetStateToBeforeFirst(ctx); err != nil {
		t.Fatalf("failed to reset: %v", err)
	}

	expectedValues := []int{10, 30}
	count := 0
	for {
		ok, err := ts.Next(ctx)
		if err != nil {
			t.Fatalf("failed to move to next: %v", err)
		}
		if !ok {
			break
		}
		val, err := ts.GetInt(ctx, "id")
		if err != nil {
			t.Fatalf("failed to get int: %v", err)
		}
		if count >= len(expectedValues) || val != expectedValues[count] {
			t.Errorf("unexpected value at position %d: got %d, expected %d", count, val, expectedValues[count])
		}
		count++
	}

	if count != len(expectedValues) {
		t.Errorf("expected %d records, got %d", len(expectedValues), count)
	}
}

func TestTableScanDeleteStringField(t *testing.T) {
	tx, layout, tableName, cleanup := setupTestTableScan(t)
	defer cleanup()

	ctx := context.Background()
	ts, err := dbrecord.NewTableScan(ctx, tx, tableName, layout)
	if err != nil {
		t.Fatalf("failed to create table scan: %v", err)
	}

	// Insert multiple records
	names := []string{"Alice", "Bob", "Charlie"}
	for _, name := range names {
		if err := ts.Insert(ctx); err != nil {
			t.Fatalf("failed to insert: %v", err)
		}
		if err := ts.SetString(ctx, "name", name); err != nil {
			t.Fatalf("failed to set string: %v", err)
		}
	}

	// Delete first record
	if err := ts.SetStateToBeforeFirst(ctx); err != nil {
		t.Fatalf("failed to reset: %v", err)
	}
	ts.Next(ctx)
	if err := ts.Delete(ctx); err != nil {
		t.Fatalf("failed to delete: %v", err)
	}

	// Verify remaining records
	if err := ts.SetStateToBeforeFirst(ctx); err != nil {
		t.Fatalf("failed to reset: %v", err)
	}

	expectedNames := []string{"Bob", "Charlie"}
	count := 0
	for {
		ok, err := ts.Next(ctx)
		if err != nil {
			t.Fatalf("failed to move to next: %v", err)
		}
		if !ok {
			break
		}
		val, err := ts.GetString(ctx, "name")
		if err != nil {
			t.Fatalf("failed to get string: %v", err)
		}
		if count >= len(expectedNames) || val != expectedNames[count] {
			t.Errorf("unexpected value at position %d: got %s, expected %s", count, val, expectedNames[count])
		}
		count++
	}

	if count != len(expectedNames) {
		t.Errorf("expected %d records, got %d", len(expectedNames), count)
	}
}

func TestTableScanMultipleRecords(t *testing.T) {
	tx, layout, tableName, cleanup := setupTestTableScan(t)
	defer cleanup()

	ctx := context.Background()
	ts, err := dbrecord.NewTableScan(ctx, tx, tableName, layout)
	if err != nil {
		t.Fatalf("failed to create table scan: %v", err)
	}

	// Insert multiple records
	testData := []struct {
		id   int
		name string
		age  int
	}{
		{1, "Alice", 25},
		{2, "Bob", 30},
		{3, "Charlie", 35},
	}

	for _, data := range testData {
		if err := ts.Insert(ctx); err != nil {
			t.Fatalf("failed to insert: %v", err)
		}
		if err := ts.SetInt(ctx, "id", data.id); err != nil {
			t.Fatalf("failed to set id: %v", err)
		}
		if err := ts.SetString(ctx, "name", data.name); err != nil {
			t.Fatalf("failed to set name: %v", err)
		}
		if err := ts.SetInt(ctx, "age", data.age); err != nil {
			t.Fatalf("failed to set age: %v", err)
		}
	}

	// Verify all records
	if err := ts.SetStateToBeforeFirst(ctx); err != nil {
		t.Fatalf("failed to reset: %v", err)
	}

	count := 0
	for {
		ok, err := ts.Next(ctx)
		if err != nil {
			t.Fatalf("failed to move to next: %v", err)
		}
		if !ok {
			break
		}

		id, err := ts.GetInt(ctx, "id")
		if err != nil {
			t.Fatalf("failed to get id: %v", err)
		}
		name, err := ts.GetString(ctx, "name")
		if err != nil {
			t.Fatalf("failed to get name: %v", err)
		}
		age, err := ts.GetInt(ctx, "age")
		if err != nil {
			t.Fatalf("failed to get age: %v", err)
		}

		expected := testData[count]
		if id != expected.id || name != expected.name || age != expected.age {
			t.Errorf("record %d: expected {%d, %s, %d}, got {%d, %s, %d}",
				count, expected.id, expected.name, expected.age, id, name, age)
		}
		count++
	}

	if count != len(testData) {
		t.Errorf("expected %d records, got %d", len(testData), count)
	}
}

func TestTableScanMultipleBlocks(t *testing.T) {
	// Use small block size to force multiple blocks
	// Slot size is 112 bytes, so block size 240 allows 2 records per block
	tx, layout, tableName, cleanup := setupTestTableScanWithBlockSize(t, 240)
	defer cleanup()

	ctx := context.Background()
	ts, err := dbrecord.NewTableScan(ctx, tx, tableName, layout)
	if err != nil {
		t.Fatalf("failed to create table scan: %v", err)
	}

	// Insert enough records to span multiple blocks
	numRecords := 10
	for i := 1; i <= numRecords; i++ {
		if err := ts.Insert(ctx); err != nil {
			t.Fatalf("failed to insert record %d: %v", i, err)
		}
		if err := ts.SetInt(ctx, "id", i); err != nil {
			t.Fatalf("failed to set id for record %d: %v", i, err)
		}
	}

	// Verify all records across multiple blocks
	if err := ts.SetStateToBeforeFirst(ctx); err != nil {
		t.Fatalf("failed to reset: %v", err)
	}

	count := 0
	for {
		ok, err := ts.Next(ctx)
		if err != nil {
			t.Fatalf("failed to move to next: %v", err)
		}
		if !ok {
			break
		}

		id, err := ts.GetInt(ctx, "id")
		if err != nil {
			t.Fatalf("failed to get id: %v", err)
		}

		expectedID := count + 1
		if id != expectedID {
			t.Errorf("record %d: expected id=%d, got %d", count, expectedID, id)
		}
		count++
	}

	if count != numRecords {
		t.Errorf("expected %d records, got %d", numRecords, count)
	}
}

func TestTableScanDeleteAcrossBlocks(t *testing.T) {
	// Use small block size to force multiple blocks
	// Slot size is 112 bytes, so block size 240 allows 2 records per block
	tx, layout, tableName, cleanup := setupTestTableScanWithBlockSize(t, 240)
	defer cleanup()

	ctx := context.Background()
	ts, err := dbrecord.NewTableScan(ctx, tx, tableName, layout)
	if err != nil {
		t.Fatalf("failed to create table scan: %v", err)
	}

	// Insert records across multiple blocks
	numRecords := 10
	for i := 1; i <= numRecords; i++ {
		if err := ts.Insert(ctx); err != nil {
			t.Fatalf("failed to insert record %d: %v", i, err)
		}
		if err := ts.SetInt(ctx, "id", i*10); err != nil {
			t.Fatalf("failed to set id for record %d: %v", i, err)
		}
	}

	// Delete every other record
	if err := ts.SetStateToBeforeFirst(ctx); err != nil {
		t.Fatalf("failed to reset: %v", err)
	}

	deleteCount := 0
	for {
		ok, err := ts.Next(ctx)
		if err != nil {
			t.Fatalf("failed to move to next: %v", err)
		}
		if !ok {
			break
		}

		id, err := ts.GetInt(ctx, "id")
		if err != nil {
			t.Fatalf("failed to get id: %v", err)
		}

		// Delete records with even id/10 values (20, 40, 60, 80, 100)
		if (id/10)%2 == 0 {
			if err := ts.Delete(ctx); err != nil {
				t.Fatalf("failed to delete record with id=%d: %v", id, err)
			}
			deleteCount++
		}
	}

	// Verify remaining records
	if err := ts.SetStateToBeforeFirst(ctx); err != nil {
		t.Fatalf("failed to reset: %v", err)
	}

	expectedIDs := []int{10, 30, 50, 70, 90}
	count := 0
	for {
		ok, err := ts.Next(ctx)
		if err != nil {
			t.Fatalf("failed to move to next: %v", err)
		}
		if !ok {
			break
		}

		id, err := ts.GetInt(ctx, "id")
		if err != nil {
			t.Fatalf("failed to get id: %v", err)
		}

		if count >= len(expectedIDs) || id != expectedIDs[count] {
			t.Errorf("unexpected record at position %d: got id=%d, expected %d", count, id, expectedIDs[count])
		}
		count++
	}

	if count != len(expectedIDs) {
		t.Errorf("expected %d remaining records, got %d", len(expectedIDs), count)
	}

	if deleteCount != 5 {
		t.Errorf("expected to delete 5 records, deleted %d", deleteCount)
	}
}

func TestTableScanInsertAfterDeleteAcrossBlocks(t *testing.T) {
	// Use small block size to force multiple blocks
	// Slot size is 112 bytes, so block size 240 allows 2 records per block
	tx, layout, tableName, cleanup := setupTestTableScanWithBlockSize(t, 240)
	defer cleanup()

	ctx := context.Background()
	ts, err := dbrecord.NewTableScan(ctx, tx, tableName, layout)
	if err != nil {
		t.Fatalf("failed to create table scan: %v", err)
	}

	// Insert initial records
	for i := 1; i <= 10; i++ {
		if err := ts.Insert(ctx); err != nil {
			t.Fatalf("failed to insert record %d: %v", i, err)
		}
		if err := ts.SetInt(ctx, "id", i); err != nil {
			t.Fatalf("failed to set id for record %d: %v", i, err)
		}
	}

	// Delete some records to create gaps
	if err := ts.SetStateToBeforeFirst(ctx); err != nil {
		t.Fatalf("failed to reset: %v", err)
	}

	for {
		ok, err := ts.Next(ctx)
		if err != nil {
			t.Fatalf("failed to move to next: %v", err)
		}
		if !ok {
			break
		}

		id, err := ts.GetInt(ctx, "id")
		if err != nil {
			t.Fatalf("failed to get id: %v", err)
		}

		// Delete records with id 3, 5, 7
		if id == 3 || id == 5 || id == 7 {
			if err := ts.Delete(ctx); err != nil {
				t.Fatalf("failed to delete record with id=%d: %v", id, err)
			}
		}
	}

	// Insert new records - should reuse deleted slots
	for i := 100; i <= 102; i++ {
		if err := ts.Insert(ctx); err != nil {
			t.Fatalf("failed to insert new record %d: %v", i, err)
		}
		if err := ts.SetInt(ctx, "id", i); err != nil {
			t.Fatalf("failed to set id for new record %d: %v", i, err)
		}
	}

	// Count total records
	if err := ts.SetStateToBeforeFirst(ctx); err != nil {
		t.Fatalf("failed to reset: %v", err)
	}

	count := 0
	for {
		ok, err := ts.Next(ctx)
		if err != nil {
			t.Fatalf("failed to move to next: %v", err)
		}
		if !ok {
			break
		}
		count++
	}

	expectedCount := 10 // 10 original - 3 deleted + 3 inserted
	if count != expectedCount {
		t.Errorf("expected %d total records, got %d", expectedCount, count)
	}
}
