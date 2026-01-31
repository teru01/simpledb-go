package dbquery_test

import (
	"context"
	"os"
	"testing"

	"github.com/teru01/simpledb-go/dbbuffer"
	"github.com/teru01/simpledb-go/dbconstant"
	"github.com/teru01/simpledb-go/dbfile"
	"github.com/teru01/simpledb-go/dblog"
	"github.com/teru01/simpledb-go/dbquery"
	"github.com/teru01/simpledb-go/dbrecord"
	"github.com/teru01/simpledb-go/dbtx"
)

func setupSelectScanTest(t *testing.T) (*dbtx.Transaction, *dbrecord.Layout, string, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "selectscan_test")
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

func TestSelectScanFilterByInt(t *testing.T) {
	tx, layout, tableName, cleanup := setupSelectScanTest(t)
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
		{3, "Charlie", 25},
		{4, "David", 35},
		{5, "Eve", 25},
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

	// Create predicate: age = 25
	lhs := dbquery.NewExpressionFromFieldName("age")
	rhs := dbquery.NewExpressionFromValue(dbconstant.NewIntConstant(25))
	term := dbquery.NewTerm(lhs, rhs)
	pred := dbquery.NewPredicate(term)

	selectScan := dbquery.NewSelectScan(ts, pred)
	defer selectScan.Close()

	// Verify filtered results
	expectedIDs := []int{1, 3, 5}
	count := 0
	for {
		ok, err := selectScan.Next(ctx)
		if err != nil {
			t.Fatalf("failed to move to next: %v", err)
		}
		if !ok {
			break
		}

		id, err := selectScan.GetInt(ctx, "id")
		if err != nil {
			t.Fatalf("failed to get id: %v", err)
		}

		age, err := selectScan.GetInt(ctx, "age")
		if err != nil {
			t.Fatalf("failed to get age: %v", err)
		}

		if age != 25 {
			t.Errorf("expected age=25, got %d", age)
		}

		if count < len(expectedIDs) && id != expectedIDs[count] {
			t.Errorf("expected id=%d, got %d", expectedIDs[count], id)
		}
		count++
	}

	if count != len(expectedIDs) {
		t.Errorf("expected %d records, got %d", len(expectedIDs), count)
	}
}

func TestSelectScanFilterByString(t *testing.T) {
	tx, layout, tableName, cleanup := setupSelectScanTest(t)
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
		{3, "Alice", 35},
		{4, "David", 40},
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

	// Create predicate: name = "Alice"
	lhs := dbquery.NewExpressionFromFieldName("name")
	rhs := dbquery.NewExpressionFromValue(dbconstant.NewStringConstant("Alice"))
	term := dbquery.NewTerm(lhs, rhs)
	pred := dbquery.NewPredicate(term)

	selectScan := dbquery.NewSelectScan(ts, pred)
	defer selectScan.Close()

	// Verify filtered results
	expectedIDs := []int{1, 3}
	count := 0
	for {
		ok, err := selectScan.Next(ctx)
		if err != nil {
			t.Fatalf("failed to move to next: %v", err)
		}
		if !ok {
			break
		}

		id, err := selectScan.GetInt(ctx, "id")
		if err != nil {
			t.Fatalf("failed to get id: %v", err)
		}

		name, err := selectScan.GetString(ctx, "name")
		if err != nil {
			t.Fatalf("failed to get name: %v", err)
		}

		if name != "Alice" {
			t.Errorf("expected name='Alice', got '%s'", name)
		}

		if count < len(expectedIDs) && id != expectedIDs[count] {
			t.Errorf("expected id=%d, got %d", expectedIDs[count], id)
		}
		count++
	}

	if count != len(expectedIDs) {
		t.Errorf("expected %d records, got %d", len(expectedIDs), count)
	}
}

func TestSelectScanMultipleTerms(t *testing.T) {
	tx, layout, tableName, cleanup := setupSelectScanTest(t)
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
		{2, "Bob", 25},
		{3, "Alice", 30},
		{4, "Alice", 25},
		{5, "Charlie", 25},
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

	// Create predicate: name = "Alice" AND age = 25
	lhs1 := dbquery.NewExpressionFromFieldName("name")
	rhs1 := dbquery.NewExpressionFromValue(dbconstant.NewStringConstant("Alice"))
	term1 := dbquery.NewTerm(lhs1, rhs1)

	lhs2 := dbquery.NewExpressionFromFieldName("age")
	rhs2 := dbquery.NewExpressionFromValue(dbconstant.NewIntConstant(25))
	term2 := dbquery.NewTerm(lhs2, rhs2)

	pred := dbquery.NewPredicate(term1, term2)

	selectScan := dbquery.NewSelectScan(ts, pred)
	defer selectScan.Close()

	// Verify filtered results: only id=1 and id=4 match name="Alice" AND age=25
	expectedIDs := []int{1, 4}
	count := 0
	for {
		ok, err := selectScan.Next(ctx)
		if err != nil {
			t.Fatalf("failed to move to next: %v", err)
		}
		if !ok {
			break
		}

		id, err := selectScan.GetInt(ctx, "id")
		if err != nil {
			t.Fatalf("failed to get id: %v", err)
		}

		name, err := selectScan.GetString(ctx, "name")
		if err != nil {
			t.Fatalf("failed to get name: %v", err)
		}

		age, err := selectScan.GetInt(ctx, "age")
		if err != nil {
			t.Fatalf("failed to get age: %v", err)
		}

		if name != "Alice" || age != 25 {
			t.Errorf("expected name='Alice' and age=25, got name='%s' and age=%d", name, age)
		}

		if count < len(expectedIDs) && id != expectedIDs[count] {
			t.Errorf("expected id=%d, got %d", expectedIDs[count], id)
		}
		count++
	}

	if count != len(expectedIDs) {
		t.Errorf("expected %d records, got %d", len(expectedIDs), count)
	}
}

func TestSelectScanNoMatches(t *testing.T) {
	tx, layout, tableName, cleanup := setupSelectScanTest(t)
	defer cleanup()

	ctx := context.Background()
	ts, err := dbrecord.NewTableScan(ctx, tx, tableName, layout)
	if err != nil {
		t.Fatalf("failed to create table scan: %v", err)
	}

	// Insert test data
	for i := 1; i <= 5; i++ {
		if err := ts.Insert(ctx); err != nil {
			t.Fatalf("failed to insert: %v", err)
		}
		if err := ts.SetInt(ctx, "age", 20+i); err != nil {
			t.Fatalf("failed to set age: %v", err)
		}
	}

	if err := ts.SetStateToBeforeFirst(ctx); err != nil {
		t.Fatalf("failed to reset: %v", err)
	}

	// Create predicate: age = 100 (no match)
	lhs := dbquery.NewExpressionFromFieldName("age")
	rhs := dbquery.NewExpressionFromValue(dbconstant.NewIntConstant(100))
	term := dbquery.NewTerm(lhs, rhs)
	pred := dbquery.NewPredicate(term)

	selectScan := dbquery.NewSelectScan(ts, pred)
	defer selectScan.Close()

	// Verify no results
	ok, err := selectScan.Next(ctx)
	if err != nil {
		t.Fatalf("failed to move to next: %v", err)
	}
	if ok {
		t.Errorf("expected no matching records, but found one")
	}
}

func TestSelectScanHasField(t *testing.T) {
	tx, layout, tableName, cleanup := setupSelectScanTest(t)
	defer cleanup()

	ctx := context.Background()
	ts, err := dbrecord.NewTableScan(ctx, tx, tableName, layout)
	if err != nil {
		t.Fatalf("failed to create table scan: %v", err)
	}

	lhs := dbquery.NewExpressionFromFieldName("id")
	rhs := dbquery.NewExpressionFromValue(dbconstant.NewIntConstant(1))
	term := dbquery.NewTerm(lhs, rhs)
	pred := dbquery.NewPredicate(term)

	selectScan := dbquery.NewSelectScan(ts, pred)
	defer selectScan.Close()

	if !selectScan.HasField("id") {
		t.Errorf("expected HasField('id') to be true")
	}
	if !selectScan.HasField("name") {
		t.Errorf("expected HasField('name') to be true")
	}
	if !selectScan.HasField("age") {
		t.Errorf("expected HasField('age') to be true")
	}
	if selectScan.HasField("nonexistent") {
		t.Errorf("expected HasField('nonexistent') to be false")
	}
}

func TestSelectScanUpdate(t *testing.T) {
	tx, layout, tableName, cleanup := setupSelectScanTest(t)
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
		{3, "Charlie", 25},
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

	// Create predicate: age = 25
	lhs := dbquery.NewExpressionFromFieldName("age")
	rhs := dbquery.NewExpressionFromValue(dbconstant.NewIntConstant(25))
	term := dbquery.NewTerm(lhs, rhs)
	pred := dbquery.NewPredicate(term)

	selectScan := dbquery.NewSelectScan(ts, pred)

	// Update age to 99 for records where age=25
	for {
		ok, err := selectScan.Next(ctx)
		if err != nil {
			t.Fatalf("failed to move to next: %v", err)
		}
		if !ok {
			break
		}
		if err := selectScan.SetInt(ctx, "age", 99); err != nil {
			t.Fatalf("failed to set age: %v", err)
		}
	}
	selectScan.Close()

	// Create a new TableScan for verification
	ts2, err := dbrecord.NewTableScan(ctx, tx, tableName, layout)
	if err != nil {
		t.Fatalf("failed to create table scan for verification: %v", err)
	}
	defer ts2.Close()

	expectedAges := map[int]int{
		1: 99, // was 25, updated to 99
		2: 30, // unchanged
		3: 99, // was 25, updated to 99
	}

	for {
		ok, err := ts2.Next(ctx)
		if err != nil {
			t.Fatalf("failed to move to next: %v", err)
		}
		if !ok {
			break
		}

		id, err := ts2.GetInt(ctx, "id")
		if err != nil {
			t.Fatalf("failed to get id: %v", err)
		}

		age, err := ts2.GetInt(ctx, "age")
		if err != nil {
			t.Fatalf("failed to get age: %v", err)
		}

		if expectedAge, ok := expectedAges[id]; ok {
			if age != expectedAge {
				t.Errorf("id=%d: expected age=%d, got %d", id, expectedAge, age)
			}
		}
	}
}

func TestSelectScanDelete(t *testing.T) {
	tx, layout, tableName, cleanup := setupSelectScanTest(t)
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
		{3, "Charlie", 25},
		{4, "David", 35},
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

	// Create predicate: age = 25
	lhs := dbquery.NewExpressionFromFieldName("age")
	rhs := dbquery.NewExpressionFromValue(dbconstant.NewIntConstant(25))
	term := dbquery.NewTerm(lhs, rhs)
	pred := dbquery.NewPredicate(term)

	selectScan := dbquery.NewSelectScan(ts, pred)

	// Delete records where age=25
	for {
		ok, err := selectScan.Next(ctx)
		if err != nil {
			t.Fatalf("failed to move to next: %v", err)
		}
		if !ok {
			break
		}
		if err := selectScan.Delete(ctx); err != nil {
			t.Fatalf("failed to delete: %v", err)
		}
	}
	selectScan.Close()

	// Create a new TableScan for verification
	ts2, err := dbrecord.NewTableScan(ctx, tx, tableName, layout)
	if err != nil {
		t.Fatalf("failed to create table scan for verification: %v", err)
	}
	defer ts2.Close()

	expectedIDs := []int{2, 4} // Only Bob(30) and David(35) should remain
	count := 0
	for {
		ok, err := ts2.Next(ctx)
		if err != nil {
			t.Fatalf("failed to move to next: %v", err)
		}
		if !ok {
			break
		}

		id, err := ts2.GetInt(ctx, "id")
		if err != nil {
			t.Fatalf("failed to get id: %v", err)
		}

		if count < len(expectedIDs) && id != expectedIDs[count] {
			t.Errorf("expected id=%d, got %d", expectedIDs[count], id)
		}
		count++
	}

	if count != len(expectedIDs) {
		t.Errorf("expected %d records, got %d", len(expectedIDs), count)
	}
}
