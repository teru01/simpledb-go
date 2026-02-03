package dbplan_test

import (
	"context"
	"os"
	"testing"

	"github.com/teru01/simpledb-go/dbbuffer"
	"github.com/teru01/simpledb-go/dbfile"
	"github.com/teru01/simpledb-go/dblog"
	"github.com/teru01/simpledb-go/dbmetadata"
	"github.com/teru01/simpledb-go/dbparse"
	"github.com/teru01/simpledb-go/dbplan"
	"github.com/teru01/simpledb-go/dbrecord"
	"github.com/teru01/simpledb-go/dbtx"
)

func setupUpdatePlannerTest(t *testing.T) (*dbmetadata.MetadataManager, *dbtx.Transaction, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "update_planner_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	dirFile, err := os.Open(dir)
	if err != nil {
		t.Fatalf("failed to open temp dir: %v", err)
	}

	fm, err := dbfile.NewFileManager(dirFile, 4000)
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
	mm, err := dbmetadata.NewMetadataManager(ctx, true, tx)
	if err != nil {
		t.Fatalf("failed to create metadata manager: %v", err)
	}

	cleanup := func() {
		tx.Commit()
		dirFile.Close()
		os.RemoveAll(dir)
	}

	return mm, tx, cleanup
}

func TestUpdatePlannerExecuteInsert(t *testing.T) {
	mm, tx, cleanup := setupUpdatePlannerTest(t)
	defer cleanup()

	ctx := context.Background()

	// Create table first
	schema := dbrecord.NewSchema()
	schema.AddIntField("id")
	schema.AddStringField("name", 20)
	schema.AddIntField("age")

	if err := mm.CreateTable(ctx, "users", schema, tx); err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Execute insert
	up := dbplan.NewUpdatePlanner(mm)

	p := dbparse.NewParser(`INSERT INTO users (id, name, age) VALUES (1, "Alice", 25)`)
	insertData, err := p.Insert()
	if err != nil {
		t.Fatalf("failed to parse insert: %v", err)
	}

	affectedRows, err := up.ExecuteInsert(ctx, insertData, tx)
	if err != nil {
		t.Fatalf("failed to execute insert: %v", err)
	}

	if affectedRows != 1 {
		t.Errorf("expected 1 affected row, got %d", affectedRows)
	}

	// Verify the inserted data
	layout, err := mm.GetLayout(ctx, "users", tx)
	if err != nil {
		t.Fatalf("failed to get layout: %v", err)
	}

	ts, err := dbrecord.NewTableScan(ctx, tx, "users", layout)
	if err != nil {
		t.Fatalf("failed to create table scan: %v", err)
	}
	defer ts.Close()

	ok, err := ts.Next(ctx)
	if err != nil {
		t.Fatalf("failed to get next: %v", err)
	}
	if !ok {
		t.Fatalf("expected record, got none")
	}

	id, err := ts.GetInt(ctx, "id")
	if err != nil {
		t.Fatalf("failed to get id: %v", err)
	}
	if id != 1 {
		t.Errorf("expected id=1, got %d", id)
	}

	name, err := ts.GetString(ctx, "name")
	if err != nil {
		t.Fatalf("failed to get name: %v", err)
	}
	if name != "Alice" {
		t.Errorf("expected name='Alice', got '%s'", name)
	}

	age, err := ts.GetInt(ctx, "age")
	if err != nil {
		t.Fatalf("failed to get age: %v", err)
	}
	if age != 25 {
		t.Errorf("expected age=25, got %d", age)
	}
}

func TestUpdatePlannerExecuteDelete(t *testing.T) {
	mm, tx, cleanup := setupUpdatePlannerTest(t)
	defer cleanup()

	ctx := context.Background()

	// Create table and insert data
	schema := dbrecord.NewSchema()
	schema.AddIntField("id")
	schema.AddStringField("name", 20)
	schema.AddIntField("age")

	if err := mm.CreateTable(ctx, "users", schema, tx); err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	layout, err := mm.GetLayout(ctx, "users", tx)
	if err != nil {
		t.Fatalf("failed to get layout: %v", err)
	}

	ts, err := dbrecord.NewTableScan(ctx, tx, "users", layout)
	if err != nil {
		t.Fatalf("failed to create table scan: %v", err)
	}

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
	ts.Close()

	// Execute delete
	up := dbplan.NewUpdatePlanner(mm)

	p := dbparse.NewParser("DELETE FROM users WHERE age = 25")
	deleteData, err := p.Delete()
	if err != nil {
		t.Fatalf("failed to parse delete: %v", err)
	}

	affectedRows, err := up.ExecuteDelete(ctx, deleteData, tx)
	if err != nil {
		t.Fatalf("failed to execute delete: %v", err)
	}

	// Alice(25) and Charlie(25) should be deleted
	if affectedRows != 2 {
		t.Errorf("expected 2 affected rows, got %d", affectedRows)
	}

	// Verify remaining data
	ts2, err := dbrecord.NewTableScan(ctx, tx, "users", layout)
	if err != nil {
		t.Fatalf("failed to create table scan: %v", err)
	}
	defer ts2.Close()

	remainingCount := 0
	for {
		ok, err := ts2.Next(ctx)
		if err != nil {
			t.Fatalf("failed to get next: %v", err)
		}
		if !ok {
			break
		}
		remainingCount++
	}

	// Bob(30) and David(35) should remain
	if remainingCount != 2 {
		t.Errorf("expected 2 remaining records, got %d", remainingCount)
	}
}

func TestUpdatePlannerExecuteModify(t *testing.T) {
	mm, tx, cleanup := setupUpdatePlannerTest(t)
	defer cleanup()

	ctx := context.Background()

	// Create table and insert data
	schema := dbrecord.NewSchema()
	schema.AddIntField("id")
	schema.AddStringField("name", 20)
	schema.AddIntField("age")

	if err := mm.CreateTable(ctx, "users", schema, tx); err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	layout, err := mm.GetLayout(ctx, "users", tx)
	if err != nil {
		t.Fatalf("failed to get layout: %v", err)
	}

	ts, err := dbrecord.NewTableScan(ctx, tx, "users", layout)
	if err != nil {
		t.Fatalf("failed to create table scan: %v", err)
	}

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
	ts.Close()

	// Execute modify
	up := dbplan.NewUpdatePlanner(mm)

	p := dbparse.NewParser("UPDATE users SET age = 99 WHERE age = 25")
	modifyData, err := p.Modify()
	if err != nil {
		t.Fatalf("failed to parse modify: %v", err)
	}

	affectedRows, err := up.ExecuteModify(ctx, modifyData, tx)
	if err != nil {
		t.Fatalf("failed to execute modify: %v", err)
	}

	// Alice(25) and Charlie(25) should be updated
	if affectedRows != 2 {
		t.Errorf("expected 2 affected rows, got %d", affectedRows)
	}

	// Verify updated data
	ts2, err := dbrecord.NewTableScan(ctx, tx, "users", layout)
	if err != nil {
		t.Fatalf("failed to create table scan: %v", err)
	}
	defer ts2.Close()

	expectedAges := map[int]int{
		1: 99, // Alice: was 25, now 99
		2: 30, // Bob: unchanged
		3: 99, // Charlie: was 25, now 99
	}

	for {
		ok, err := ts2.Next(ctx)
		if err != nil {
			t.Fatalf("failed to get next: %v", err)
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

func TestUpdatePlannerExecuteCreateTable(t *testing.T) {
	mm, tx, cleanup := setupUpdatePlannerTest(t)
	defer cleanup()

	ctx := context.Background()

	up := dbplan.NewUpdatePlanner(mm)

	p := dbparse.NewParser("CREATE TABLE products (id INT, name VARCHAR(30), price INT)")
	createData, err := p.Create()
	if err != nil {
		t.Fatalf("failed to parse create table: %v", err)
	}

	createTableData, ok := createData.(*dbparse.CreateTableData)
	if !ok {
		t.Fatalf("expected *CreateTableData, got %T", createData)
	}

	affectedRows, err := up.ExecuteCreateTable(ctx, createTableData, tx)
	if err != nil {
		t.Fatalf("failed to execute create table: %v", err)
	}

	if affectedRows != 0 {
		t.Errorf("expected 0 affected rows for create table, got %d", affectedRows)
	}

	// Verify the table was created
	layout, err := mm.GetLayout(ctx, "products", tx)
	if err != nil {
		t.Fatalf("failed to get layout: %v", err)
	}

	fields := layout.Schema().Fields()
	if len(fields) != 3 {
		t.Errorf("expected 3 fields, got %d", len(fields))
	}

	expectedFields := []string{"id", "name", "price"}
	for _, field := range expectedFields {
		found := false
		for _, f := range fields {
			if f == field {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("expected field %q not found", field)
		}
	}
}

func TestUpdatePlannerExecuteCreateView(t *testing.T) {
	mm, tx, cleanup := setupUpdatePlannerTest(t)
	defer cleanup()

	ctx := context.Background()

	// Create base table first
	schema := dbrecord.NewSchema()
	schema.AddIntField("id")
	schema.AddStringField("name", 20)
	schema.AddIntField("active")

	if err := mm.CreateTable(ctx, "users", schema, tx); err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	up := dbplan.NewUpdatePlanner(mm)

	p := dbparse.NewParser("CREATE VIEW active_users AS SELECT id, name FROM users WHERE active = 1")
	createData, err := p.Create()
	if err != nil {
		t.Fatalf("failed to parse create view: %v", err)
	}

	createViewData, ok := createData.(*dbparse.CreateViewData)
	if !ok {
		t.Fatalf("expected *CreateViewData, got %T", createData)
	}

	affectedRows, err := up.ExecuteCreateView(ctx, createViewData, tx)
	if err != nil {
		t.Fatalf("failed to execute create view: %v", err)
	}

	if affectedRows != 0 {
		t.Errorf("expected 0 affected rows for create view, got %d", affectedRows)
	}

	// Verify the view was created
	viewDef, err := mm.GetViewDef(ctx, "active_users", tx)
	if err != nil {
		t.Fatalf("failed to get view def: %v", err)
	}

	if viewDef == "" {
		t.Errorf("expected view definition, got empty string")
	}
}

func TestUpdatePlannerExecuteCreateIndex(t *testing.T) {
	mm, tx, cleanup := setupUpdatePlannerTest(t)
	defer cleanup()

	ctx := context.Background()

	// Create base table first
	schema := dbrecord.NewSchema()
	schema.AddIntField("id")
	schema.AddStringField("name", 20)

	if err := mm.CreateTable(ctx, "users", schema, tx); err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	up := dbplan.NewUpdatePlanner(mm)

	p := dbparse.NewParser("CREATE INDEX idx_name ON users (name)")
	createData, err := p.Create()
	if err != nil {
		t.Fatalf("failed to parse create index: %v", err)
	}

	createIndexData, ok := createData.(*dbparse.CreateIndexData)
	if !ok {
		t.Fatalf("expected *CreateIndexData, got %T", createData)
	}

	affectedRows, err := up.ExecuteCreateIndex(ctx, createIndexData, tx)
	if err != nil {
		t.Fatalf("failed to execute create index: %v", err)
	}

	if affectedRows != 0 {
		t.Errorf("expected 0 affected rows for create index, got %d", affectedRows)
	}

	// Verify the index was created
	indexInfos, err := mm.GetIndexInfo(ctx, "users", tx)
	if err != nil {
		t.Fatalf("failed to get index info: %v", err)
	}

	indexInfo, ok := indexInfos["name"]
	if !ok {
		t.Errorf("expected index 'name' to exist")
	}
	if indexInfo.IndexName() != "idx_name" {
		t.Errorf("expected index name 'idx_name', got %q", indexInfo.IndexName())
	}
	if indexInfo.TableName() != "users" {
		t.Errorf("expected table name 'users', got %q", indexInfo.TableName())
	}
	if indexInfo.FieldName() != "name" {
		t.Errorf("expected field name 'name', got %q", indexInfo.FieldName())
	}
}

func TestUpdatePlannerExecuteDeleteAll(t *testing.T) {
	mm, tx, cleanup := setupUpdatePlannerTest(t)
	defer cleanup()

	ctx := context.Background()

	// Create table and insert data
	schema := dbrecord.NewSchema()
	schema.AddIntField("id")
	schema.AddStringField("name", 20)

	if err := mm.CreateTable(ctx, "users", schema, tx); err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	layout, err := mm.GetLayout(ctx, "users", tx)
	if err != nil {
		t.Fatalf("failed to get layout: %v", err)
	}

	ts, err := dbrecord.NewTableScan(ctx, tx, "users", layout)
	if err != nil {
		t.Fatalf("failed to create table scan: %v", err)
	}

	for i := 1; i <= 5; i++ {
		if err := ts.Insert(ctx); err != nil {
			t.Fatalf("failed to insert: %v", err)
		}
		if err := ts.SetInt(ctx, "id", i); err != nil {
			t.Fatalf("failed to set id: %v", err)
		}
		if err := ts.SetString(ctx, "name", "user"); err != nil {
			t.Fatalf("failed to set name: %v", err)
		}
	}
	ts.Close()

	// Execute delete without WHERE (delete all)
	up := dbplan.NewUpdatePlanner(mm)

	p := dbparse.NewParser("DELETE FROM users")
	deleteData, err := p.Delete()
	if err != nil {
		t.Fatalf("failed to parse delete: %v", err)
	}

	affectedRows, err := up.ExecuteDelete(ctx, deleteData, tx)
	if err != nil {
		t.Fatalf("failed to execute delete: %v", err)
	}

	if affectedRows != 5 {
		t.Errorf("expected 5 affected rows, got %d", affectedRows)
	}

	// Verify all data is deleted
	ts2, err := dbrecord.NewTableScan(ctx, tx, "users", layout)
	if err != nil {
		t.Fatalf("failed to create table scan: %v", err)
	}
	defer ts2.Close()

	ok, err := ts2.Next(ctx)
	if err != nil {
		t.Fatalf("failed to get next: %v", err)
	}
	if ok {
		t.Errorf("expected no records, but found some")
	}
}

func TestUpdatePlannerExecuteModifyAll(t *testing.T) {
	mm, tx, cleanup := setupUpdatePlannerTest(t)
	defer cleanup()

	ctx := context.Background()

	// Create table and insert data
	schema := dbrecord.NewSchema()
	schema.AddIntField("id")
	schema.AddIntField("status")

	if err := mm.CreateTable(ctx, "items", schema, tx); err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	layout, err := mm.GetLayout(ctx, "items", tx)
	if err != nil {
		t.Fatalf("failed to get layout: %v", err)
	}

	ts, err := dbrecord.NewTableScan(ctx, tx, "items", layout)
	if err != nil {
		t.Fatalf("failed to create table scan: %v", err)
	}

	for i := 1; i <= 3; i++ {
		if err := ts.Insert(ctx); err != nil {
			t.Fatalf("failed to insert: %v", err)
		}
		if err := ts.SetInt(ctx, "id", i); err != nil {
			t.Fatalf("failed to set id: %v", err)
		}
		if err := ts.SetInt(ctx, "status", 0); err != nil {
			t.Fatalf("failed to set status: %v", err)
		}
	}
	ts.Close()

	// Execute modify without WHERE (update all)
	up := dbplan.NewUpdatePlanner(mm)

	p := dbparse.NewParser("UPDATE items SET status = 1")
	modifyData, err := p.Modify()
	if err != nil {
		t.Fatalf("failed to parse modify: %v", err)
	}

	affectedRows, err := up.ExecuteModify(ctx, modifyData, tx)
	if err != nil {
		t.Fatalf("failed to execute modify: %v", err)
	}

	if affectedRows != 3 {
		t.Errorf("expected 3 affected rows, got %d", affectedRows)
	}

	// Verify all data is updated
	ts2, err := dbrecord.NewTableScan(ctx, tx, "items", layout)
	if err != nil {
		t.Fatalf("failed to create table scan: %v", err)
	}
	defer ts2.Close()

	for {
		ok, err := ts2.Next(ctx)
		if err != nil {
			t.Fatalf("failed to get next: %v", err)
		}
		if !ok {
			break
		}

		status, err := ts2.GetInt(ctx, "status")
		if err != nil {
			t.Fatalf("failed to get status: %v", err)
		}

		if status != 1 {
			t.Errorf("expected status=1, got %d", status)
		}
	}
}
