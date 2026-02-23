package main

import (
	"context"
	"os"
	"strconv"
	"testing"

	"github.com/teru01/simpledb-go/dbrecord"
	"github.com/teru01/simpledb-go/dbtx"
)

func setupTestDB(t *testing.T) (*SimpleDB, context.Context, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "simpledb_integration_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	db, cleanup, err := NewSimpleDB(dir, 4000, 100)
	if err != nil {
		os.RemoveAll(dir)
		t.Fatalf("failed to create simpledb: %v", err)
	}

	ctx := context.Background()
	if err := db.Init(ctx); err != nil {
		cleanup()
		os.RemoveAll(dir)
		t.Fatalf("failed to init simpledb: %v", err)
	}

	return db, ctx, func() {
		cleanup()
		os.RemoveAll(dir)
	}
}

// queryRows executes a SELECT and returns rows as [][]string.
func queryRows(t *testing.T, db *SimpleDB, ctx context.Context, sql string) [][]string {
	t.Helper()

	var tx *dbtx.Transaction
	ownTx := false
	if db.explicitTx != nil {
		tx = db.explicitTx
	} else {
		var err error
		tx, err = dbtx.NewTransaction(db.fileManager, db.logManager, db.bufferManager)
		if err != nil {
			t.Fatalf("failed to create transaction: %v", err)
		}
		ownTx = true
	}

	plan, err := db.planner.CreateQueryPlan(ctx, sql, tx)
	if err != nil {
		t.Fatalf("failed to create query plan: %v", err)
	}
	scan, err := plan.Open(ctx)
	if err != nil {
		t.Fatalf("failed to open plan: %v", err)
	}
	defer scan.Close(ctx)

	schema := plan.Schema()
	fields := schema.Fields()

	var rows [][]string
	for {
		ok, err := scan.Next(ctx)
		if err != nil {
			t.Fatalf("failed to scan next: %v", err)
		}
		if !ok {
			break
		}
		row := make([]string, 0, len(fields))
		for _, f := range fields {
			switch schema.FieldType(f) {
			case dbrecord.FieldTypeInt:
				v, err := scan.GetInt(ctx, f)
				if err != nil {
					t.Fatalf("failed to get int %q: %v", f, err)
				}
				row = append(row, strconv.Itoa(v))
			case dbrecord.FieldTypeString:
				v, err := scan.GetString(ctx, f)
				if err != nil {
					t.Fatalf("failed to get string %q: %v", f, err)
				}
				row = append(row, v)
			}
		}
		rows = append(rows, row)
	}
	if ownTx {
		if err := tx.Commit(); err != nil {
			t.Fatalf("failed to commit: %v", err)
		}
	}
	return rows
}

func execUpdate(t *testing.T, db *SimpleDB, ctx context.Context, sql string) {
	t.Helper()
	if err := db.Execute(ctx, sql); err != nil {
		t.Fatalf("failed to execute %q: %v", sql, err)
	}
}

func assertRowsUnordered(t *testing.T, got [][]string, want [][]string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("expected %d rows, got %d\ngot: %v", len(want), len(got), got)
	}
	used := make([]bool, len(want))
	for _, gotRow := range got {
		found := false
		for j, wantRow := range want {
			if used[j] || len(gotRow) != len(wantRow) {
				continue
			}
			match := true
			for k := range wantRow {
				if gotRow[k] != wantRow[k] {
					match = false
					break
				}
			}
			if match {
				used[j] = true
				found = true
				break
			}
		}
		if !found {
			t.Errorf("unexpected row: %v\nwant one of: %v", gotRow, want)
		}
	}
}

func assertRows(t *testing.T, got [][]string, want [][]string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("expected %d rows, got %d\ngot: %v", len(want), len(got), got)
	}
	for i, wantRow := range want {
		if len(got[i]) != len(wantRow) {
			t.Fatalf("row %d: expected %d columns, got %d", i, len(wantRow), len(got[i]))
		}
		for j, wantVal := range wantRow {
			if got[i][j] != wantVal {
				t.Errorf("row %d, col %d: expected %q, got %q", i, j, wantVal, got[i][j])
			}
		}
	}
}

func TestInsertAndSelect(t *testing.T) {
	db, ctx, cleanup := setupTestDB(t)
	defer cleanup()

	execUpdate(t, db, ctx, `CREATE TABLE students (id INT, name VARCHAR(10), class VARCHAR(1))`)
	execUpdate(t, db, ctx, `INSERT INTO students (id, name, class) VALUES (1, "sheep", "A")`)
	execUpdate(t, db, ctx, `INSERT INTO students (id, name, class) VALUES (2, "goat", "B")`)
	execUpdate(t, db, ctx, `INSERT INTO students (id, name, class) VALUES (3, "cow", "B")`)
	execUpdate(t, db, ctx, `INSERT INTO students (id, name, class) VALUES (4, "cat", "C")`)

	rows := queryRows(t, db, ctx, `SELECT id, name, class FROM students`)
	assertRows(t, rows, [][]string{
		{"1", "sheep", "A"},
		{"2", "goat", "B"},
		{"3", "cow", "B"},
		{"4", "cat", "C"},
	})
}

func TestSelectWithWhere(t *testing.T) {
	db, ctx, cleanup := setupTestDB(t)
	defer cleanup()

	execUpdate(t, db, ctx, `CREATE TABLE students (id INT, name VARCHAR(10), class VARCHAR(1))`)
	execUpdate(t, db, ctx, `INSERT INTO students (id, name, class) VALUES (1, "sheep", "A")`)
	execUpdate(t, db, ctx, `INSERT INTO students (id, name, class) VALUES (2, "goat", "B")`)
	execUpdate(t, db, ctx, `INSERT INTO students (id, name, class) VALUES (3, "cow", "B")`)
	execUpdate(t, db, ctx, `INSERT INTO students (id, name, class) VALUES (4, "cat", "C")`)

	rows := queryRows(t, db, ctx, `SELECT id, name, class FROM students WHERE class = "B"`)
	assertRows(t, rows, [][]string{
		{"2", "goat", "B"},
		{"3", "cow", "B"},
	})
}

func TestJoin(t *testing.T) {
	db, ctx, cleanup := setupTestDB(t)
	defer cleanup()

	execUpdate(t, db, ctx, `CREATE TABLE students (id INT, name VARCHAR(10), class VARCHAR(1))`)
	execUpdate(t, db, ctx, `INSERT INTO students (id, name, class) VALUES (1, "sheep", "A")`)
	execUpdate(t, db, ctx, `INSERT INTO students (id, name, class) VALUES (2, "goat", "B")`)
	execUpdate(t, db, ctx, `INSERT INTO students (id, name, class) VALUES (3, "cow", "B")`)
	execUpdate(t, db, ctx, `INSERT INTO students (id, name, class) VALUES (4, "cat", "C")`)

	execUpdate(t, db, ctx, `CREATE TABLE results (student_id INT, score INT)`)
	execUpdate(t, db, ctx, `INSERT INTO results (student_id, score) VALUES (1, 100)`)
	execUpdate(t, db, ctx, `INSERT INTO results (student_id, score) VALUES (2, 70)`)
	execUpdate(t, db, ctx, `INSERT INTO results (student_id, score) VALUES (3, 80)`)

	rows := queryRows(t, db, ctx, `SELECT id, name, score FROM students, results WHERE id = student_id AND score > 70`)
	assertRows(t, rows, [][]string{
		{"1", "sheep", "100"},
		{"3", "cow", "80"},
	})
}

func TestUpdate(t *testing.T) {
	db, ctx, cleanup := setupTestDB(t)
	defer cleanup()

	execUpdate(t, db, ctx, `CREATE TABLE students (id INT, name VARCHAR(10), class VARCHAR(1))`)
	execUpdate(t, db, ctx, `INSERT INTO students (id, name, class) VALUES (1, "sheep", "A")`)
	execUpdate(t, db, ctx, `INSERT INTO students (id, name, class) VALUES (2, "goat", "B")`)
	execUpdate(t, db, ctx, `INSERT INTO students (id, name, class) VALUES (3, "cow", "B")`)
	execUpdate(t, db, ctx, `INSERT INTO students (id, name, class) VALUES (4, "cat", "C")`)

	execUpdate(t, db, ctx, `UPDATE students SET class = "F" WHERE id = 4`)

	rows := queryRows(t, db, ctx, `SELECT id, name, class FROM students`)
	assertRows(t, rows, [][]string{
		{"1", "sheep", "A"},
		{"2", "goat", "B"},
		{"3", "cow", "B"},
		{"4", "cat", "F"},
	})
}

func TestDelete(t *testing.T) {
	db, ctx, cleanup := setupTestDB(t)
	defer cleanup()

	execUpdate(t, db, ctx, `CREATE TABLE students (id INT, name VARCHAR(10), class VARCHAR(1))`)
	execUpdate(t, db, ctx, `INSERT INTO students (id, name, class) VALUES (1, "sheep", "A")`)
	execUpdate(t, db, ctx, `INSERT INTO students (id, name, class) VALUES (2, "goat", "B")`)
	execUpdate(t, db, ctx, `INSERT INTO students (id, name, class) VALUES (3, "cow", "B")`)
	execUpdate(t, db, ctx, `INSERT INTO students (id, name, class) VALUES (4, "cat", "C")`)

	execUpdate(t, db, ctx, `DELETE FROM students WHERE class = "B"`)

	rows := queryRows(t, db, ctx, `SELECT id, name, class FROM students`)
	assertRows(t, rows, [][]string{
		{"1", "sheep", "A"},
		{"4", "cat", "C"},
	})
}

func TestTransaction(t *testing.T) {
	db, ctx, cleanup := setupTestDB(t)
	defer cleanup()

	execUpdate(t, db, ctx, `CREATE TABLE students (id INT, name VARCHAR(10), class VARCHAR(1))`)
	execUpdate(t, db, ctx, `INSERT INTO students (id, name, class) VALUES (1, "sheep", "A")`)
	execUpdate(t, db, ctx, `INSERT INTO students (id, name, class) VALUES (2, "goat", "B")`)
	execUpdate(t, db, ctx, `INSERT INTO students (id, name, class) VALUES (3, "cow", "B")`)
	execUpdate(t, db, ctx, `INSERT INTO students (id, name, class) VALUES (4, "cat", "C")`)

	// start transaction, insert, then rollback
	execUpdate(t, db, ctx, `START TRANSACTION`)
	execUpdate(t, db, ctx, `INSERT INTO students (id, name, class) VALUES (5, "gorilla", "D")`)
	execUpdate(t, db, ctx, `INSERT INTO students (id, name, class) VALUES (6, "monkey", "E")`)

	// within transaction, should see 6 rows
	rows := queryRows(t, db, ctx, `SELECT id, name, class FROM students`)
	if len(rows) != 6 {
		t.Fatalf("expected 6 rows within transaction, got %d", len(rows))
	}

	execUpdate(t, db, ctx, `ROLLBACK`)

	// after rollback, should see only 4 rows
	rows = queryRows(t, db, ctx, `SELECT id, name, class FROM students`)
	assertRows(t, rows, [][]string{
		{"1", "sheep", "A"},
		{"2", "goat", "B"},
		{"3", "cow", "B"},
		{"4", "cat", "C"},
	})
}

func TestCreateIndexAndSelect(t *testing.T) {
	db, ctx, cleanup := setupTestDB(t)
	defer cleanup()

	execUpdate(t, db, ctx, `CREATE TABLE students (id INT, name VARCHAR(10), class VARCHAR(1))`)
	execUpdate(t, db, ctx, `CREATE INDEX idx_class ON students (class)`)

	// insert after index creation so index entries are created
	execUpdate(t, db, ctx, `INSERT INTO students (id, name, class) VALUES (1, "sheep", "A")`)
	execUpdate(t, db, ctx, `INSERT INTO students (id, name, class) VALUES (2, "goat", "B")`)
	execUpdate(t, db, ctx, `INSERT INTO students (id, name, class) VALUES (3, "cow", "B")`)
	execUpdate(t, db, ctx, `INSERT INTO students (id, name, class) VALUES (4, "cat", "C")`)

	// select using the indexed field
	rows := queryRows(t, db, ctx, `SELECT id, name, class FROM students WHERE class = "B"`)
	assertRowsUnordered(t, rows, [][]string{
		{"2", "goat", "B"},
		{"3", "cow", "B"},
	})

	// select with a different value
	rows = queryRows(t, db, ctx, `SELECT id, name, class FROM students WHERE class = "A"`)
	assertRowsUnordered(t, rows, [][]string{
		{"1", "sheep", "A"},
	})

	// select with no match
	rows = queryRows(t, db, ctx, `SELECT id, name, class FROM students WHERE class = "Z"`)
	assertRows(t, rows, [][]string{})
}

func TestCreateIndexOnExistingData(t *testing.T) {
	db, ctx, cleanup := setupTestDB(t)
	defer cleanup()

	execUpdate(t, db, ctx, `CREATE TABLE students (id INT, name VARCHAR(10), class VARCHAR(1))`)

	// insert data before creating index
	execUpdate(t, db, ctx, `INSERT INTO students (id, name, class) VALUES (1, "sheep", "A")`)
	execUpdate(t, db, ctx, `INSERT INTO students (id, name, class) VALUES (2, "goat", "B")`)
	execUpdate(t, db, ctx, `INSERT INTO students (id, name, class) VALUES (3, "cow", "B")`)
	execUpdate(t, db, ctx, `INSERT INTO students (id, name, class) VALUES (4, "cat", "C")`)

	// create index on existing data
	execUpdate(t, db, ctx, `CREATE INDEX idx_class ON students (class)`)

	// existing data should be searchable via index
	rows := queryRows(t, db, ctx, `SELECT id, name, class FROM students WHERE class = "B"`)
	assertRowsUnordered(t, rows, [][]string{
		{"2", "goat", "B"},
		{"3", "cow", "B"},
	})

	rows = queryRows(t, db, ctx, `SELECT id, name, class FROM students WHERE class = "A"`)
	assertRowsUnordered(t, rows, [][]string{
		{"1", "sheep", "A"},
	})

	rows = queryRows(t, db, ctx, `SELECT id, name, class FROM students WHERE class = "Z"`)
	assertRows(t, rows, [][]string{})
}

func TestCreateIndexAndSelectAll(t *testing.T) {
	db, ctx, cleanup := setupTestDB(t)
	defer cleanup()

	execUpdate(t, db, ctx, `CREATE TABLE students (id INT, name VARCHAR(10), class VARCHAR(1))`)
	execUpdate(t, db, ctx, `CREATE INDEX idx_class ON students (class)`)

	execUpdate(t, db, ctx, `INSERT INTO students (id, name, class) VALUES (1, "sheep", "A")`)
	execUpdate(t, db, ctx, `INSERT INTO students (id, name, class) VALUES (2, "goat", "B")`)
	execUpdate(t, db, ctx, `INSERT INTO students (id, name, class) VALUES (3, "cow", "B")`)

	// select all (no WHERE on indexed field) should still work via full scan
	rows := queryRows(t, db, ctx, `SELECT id, name, class FROM students`)
	assertRowsUnordered(t, rows, [][]string{
		{"1", "sheep", "A"},
		{"2", "goat", "B"},
		{"3", "cow", "B"},
	})
}
