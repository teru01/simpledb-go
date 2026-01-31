package dbquery_test

import (
	"context"
	"os"
	"testing"

	"github.com/teru01/simpledb-go/dbbuffer"
	"github.com/teru01/simpledb-go/dbfile"
	"github.com/teru01/simpledb-go/dblog"
	"github.com/teru01/simpledb-go/dbquery"
	"github.com/teru01/simpledb-go/dbrecord"
	"github.com/teru01/simpledb-go/dbtx"
)

func setupProjectScanTest(t *testing.T) (*dbtx.Transaction, *dbrecord.Layout, string, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "projectscan_test")
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

func TestProjectScanProjectFields(t *testing.T) {
	tx, layout, tableName, cleanup := setupProjectScanTest(t)
	defer cleanup()

	ctx := context.Background()
	ts, err := dbrecord.NewTableScan(ctx, tx, tableName, layout)
	if err != nil {
		t.Fatalf("failed to create table scan: %v", err)
	}

	// Insert test data
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

	if err := ts.SetStateToBeforeFirst(ctx); err != nil {
		t.Fatalf("failed to reset: %v", err)
	}

	// Project only id and name fields
	projectScan := dbquery.NewProjectScan(ts, []string{"id", "name"})
	defer projectScan.Close()

	count := 0
	for {
		ok, err := projectScan.Next(ctx)
		if err != nil {
			t.Fatalf("failed to move to next: %v", err)
		}
		if !ok {
			break
		}

		id, err := projectScan.GetInt(ctx, "id")
		if err != nil {
			t.Fatalf("failed to get id: %v", err)
		}

		name, err := projectScan.GetString(ctx, "name")
		if err != nil {
			t.Fatalf("failed to get name: %v", err)
		}

		expected := testData[count]
		if id != expected.id {
			t.Errorf("expected id=%d, got %d", expected.id, id)
		}
		if name != expected.name {
			t.Errorf("expected name=%s, got %s", expected.name, name)
		}

		count++
	}

	if count != len(testData) {
		t.Errorf("expected %d records, got %d", len(testData), count)
	}
}

func TestProjectScanHasField(t *testing.T) {
	tx, layout, tableName, cleanup := setupProjectScanTest(t)
	defer cleanup()

	ctx := context.Background()
	ts, err := dbrecord.NewTableScan(ctx, tx, tableName, layout)
	if err != nil {
		t.Fatalf("failed to create table scan: %v", err)
	}

	// Project only id and name fields
	projectScan := dbquery.NewProjectScan(ts, []string{"id", "name"})
	defer projectScan.Close()

	// HasField should return true for projected fields
	if !projectScan.HasField("id") {
		t.Errorf("expected HasField('id') to be true")
	}
	if !projectScan.HasField("name") {
		t.Errorf("expected HasField('name') to be true")
	}
	// age is in the underlying scan but not projected - current implementation still returns true
	// because it delegates to the underlying scan
	if !projectScan.HasField("age") {
		t.Errorf("expected HasField('age') to be true (delegates to underlying scan)")
	}
	if projectScan.HasField("nonexistent") {
		t.Errorf("expected HasField('nonexistent') to be false")
	}
}

func TestProjectScanGetValue(t *testing.T) {
	tx, layout, tableName, cleanup := setupProjectScanTest(t)
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
	if err := ts.SetInt(ctx, "id", 42); err != nil {
		t.Fatalf("failed to set id: %v", err)
	}
	if err := ts.SetString(ctx, "name", "Test"); err != nil {
		t.Fatalf("failed to set name: %v", err)
	}

	if err := ts.SetStateToBeforeFirst(ctx); err != nil {
		t.Fatalf("failed to reset: %v", err)
	}

	projectScan := dbquery.NewProjectScan(ts, []string{"id", "name"})
	defer projectScan.Close()

	ok, err := projectScan.Next(ctx)
	if err != nil {
		t.Fatalf("failed to move to next: %v", err)
	}
	if !ok {
		t.Fatalf("expected a record")
	}

	val, err := projectScan.GetValue(ctx, "id")
	if err != nil {
		t.Fatalf("failed to get value: %v", err)
	}

	if val.AsRaw().(int) != 42 {
		t.Errorf("expected value=42, got %v", val.AsRaw())
	}
}

func TestProjectScanEmpty(t *testing.T) {
	tx, layout, tableName, cleanup := setupProjectScanTest(t)
	defer cleanup()

	ctx := context.Background()
	ts, err := dbrecord.NewTableScan(ctx, tx, tableName, layout)
	if err != nil {
		t.Fatalf("failed to create table scan: %v", err)
	}

	// No records inserted
	projectScan := dbquery.NewProjectScan(ts, []string{"id"})
	defer projectScan.Close()

	ok, err := projectScan.Next(ctx)
	if err != nil {
		t.Fatalf("failed to move to next: %v", err)
	}
	if ok {
		t.Errorf("expected no records")
	}
}
