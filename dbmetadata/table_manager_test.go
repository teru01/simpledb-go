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

func setupTestTableManager(t *testing.T) (*dbmetadata.TableManager, *dbtx.Transaction, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "table_manager_test")
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

	ctx := context.Background()
	tm, err := dbmetadata.NewTableManager(ctx, true, tx)
	if err != nil {
		t.Fatalf("failed to create table manager: %v", err)
	}

	cleanup := func() {
		dirFile.Close()
		os.RemoveAll(dir)
	}

	return tm, tx, cleanup
}

func TestTableManagerCreateTableAndGetLayout(t *testing.T) {
	tm, tx, cleanup := setupTestTableManager(t)
	defer cleanup()

	ctx := context.Background()

	// Create a test schema
	schema := dbrecord.NewSchema()
	schema.AddIntField("id")
	schema.AddStringField("name", 20)
	schema.AddIntField("age")

	// Create table
	err := tm.CreateTable(ctx, "students", schema, tx)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Get layout
	layout, err := tm.GetLayout(ctx, "students", tx)
	if err != nil {
		t.Fatalf("failed to get layout: %v", err)
	}

	// Verify schema has all fields
	expectedFields := []string{"id", "name", "age"}
	actualFields := layout.Schema().Fields()
	if len(actualFields) != len(expectedFields) {
		t.Errorf("expected %d fields, got %d", len(expectedFields), len(actualFields))
	}

	for _, field := range expectedFields {
		if !contains(actualFields, field) {
			t.Errorf("expected field %s not found in layout", field)
		}
	}

	// Verify field types
	if layout.Schema().FieldType("id") != dbrecord.FieldTypeInt {
		t.Errorf("expected id to be INTEGER")
	}
	if layout.Schema().FieldType("name") != dbrecord.FieldTypeString {
		t.Errorf("expected name to be VARCHAR")
	}
	if layout.Schema().FieldType("age") != dbrecord.FieldTypeInt {
		t.Errorf("expected age to be INTEGER")
	}

	// Verify string field length
	if layout.Schema().Length("name") != 20 {
		t.Errorf("expected name length to be 20, got %d", layout.Schema().Length("name"))
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}
}

func TestTableManagerMultipleTables(t *testing.T) {
	tm, tx, cleanup := setupTestTableManager(t)
	defer cleanup()

	ctx := context.Background()

	// Create first table
	schema1 := dbrecord.NewSchema()
	schema1.AddIntField("id")
	schema1.AddStringField("title", 30)

	err := tm.CreateTable(ctx, "books", schema1, tx)
	if err != nil {
		t.Fatalf("failed to create books table: %v", err)
	}

	// Create second table
	schema2 := dbrecord.NewSchema()
	schema2.AddIntField("employee_id")
	schema2.AddStringField("department", 15)
	schema2.AddIntField("salary")

	err = tm.CreateTable(ctx, "employees", schema2, tx)
	if err != nil {
		t.Fatalf("failed to create employees table: %v", err)
	}

	// Verify first table layout
	layout1, err := tm.GetLayout(ctx, "books", tx)
	if err != nil {
		t.Fatalf("failed to get books layout: %v", err)
	}

	if !contains(layout1.Schema().Fields(), "id") || !contains(layout1.Schema().Fields(), "title") {
		t.Errorf("books table fields mismatch")
	}

	// Verify second table layout
	layout2, err := tm.GetLayout(ctx, "employees", tx)
	if err != nil {
		t.Fatalf("failed to get employees layout: %v", err)
	}

	expectedFields := []string{"employee_id", "department", "salary"}
	for _, field := range expectedFields {
		if !contains(layout2.Schema().Fields(), field) {
			t.Errorf("expected field %s not found in employees layout", field)
		}
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}
}

func TestTableManagerGetLayoutNonexistentTable(t *testing.T) {
	tm, tx, cleanup := setupTestTableManager(t)
	defer cleanup()

	ctx := context.Background()

	// Try to get layout for a table that doesn't exist
	layout, err := tm.GetLayout(ctx, "nonexistent", tx)
	if err != nil {
		t.Fatalf("failed to get layout: %v", err)
	}

	// Should return an empty schema
	if len(layout.Schema().Fields()) != 0 {
		t.Errorf("expected empty schema for nonexistent table, got %d fields", len(layout.Schema().Fields()))
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}
}

func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}
