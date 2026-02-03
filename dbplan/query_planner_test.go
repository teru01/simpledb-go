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

func setupQueryPlannerTest(t *testing.T) (*dbmetadata.MetadataManager, *dbtx.Transaction, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "query_planner_test")
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

func TestQueryPlannerCreatePlanSingleTable(t *testing.T) {
	mm, tx, cleanup := setupQueryPlannerTest(t)
	defer cleanup()

	ctx := context.Background()

	// Create a test table
	schema := dbrecord.NewSchema()
	schema.AddIntField("id")
	schema.AddStringField("name", 20)
	schema.AddIntField("age")

	if err := mm.CreateTable(ctx, "users", schema, tx); err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Insert test data
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

	// Create query planner and execute query
	qp := dbplan.NewQueryPlanner(mm)

	p := dbparse.NewParser("SELECT id, name FROM users")
	queryData, err := p.Query()
	if err != nil {
		t.Fatalf("failed to parse query: %v", err)
	}

	plan, err := qp.CreatePlan(ctx, queryData, tx)
	if err != nil {
		t.Fatalf("failed to create plan: %v", err)
	}

	// Verify the plan schema
	planSchema := plan.Schema()
	if !contains(planSchema.Fields(), "id") {
		t.Errorf("expected 'id' in schema fields")
	}
	if !contains(planSchema.Fields(), "name") {
		t.Errorf("expected 'name' in schema fields")
	}
	if contains(planSchema.Fields(), "age") {
		t.Errorf("expected 'age' NOT in schema fields (not projected)")
	}

	// Execute the plan and verify results
	scan, err := plan.Open(ctx)
	if err != nil {
		t.Fatalf("failed to open plan: %v", err)
	}
	defer scan.Close()

	count := 0
	for {
		ok, err := scan.Next(ctx)
		if err != nil {
			t.Fatalf("failed to get next: %v", err)
		}
		if !ok {
			break
		}
		count++
	}

	if count != 3 {
		t.Errorf("expected 3 records, got %d", count)
	}
}

func TestQueryPlannerCreatePlanWithWhere(t *testing.T) {
	mm, tx, cleanup := setupQueryPlannerTest(t)
	defer cleanup()

	ctx := context.Background()

	// Create a test table
	schema := dbrecord.NewSchema()
	schema.AddIntField("id")
	schema.AddStringField("name", 20)
	schema.AddIntField("age")

	if err := mm.CreateTable(ctx, "users", schema, tx); err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Insert test data
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

	// Create query planner and execute query with WHERE
	qp := dbplan.NewQueryPlanner(mm)

	p := dbparse.NewParser("SELECT id, name FROM users WHERE age = 25")
	queryData, err := p.Query()
	if err != nil {
		t.Fatalf("failed to parse query: %v", err)
	}

	plan, err := qp.CreatePlan(ctx, queryData, tx)
	if err != nil {
		t.Fatalf("failed to create plan: %v", err)
	}

	// Execute the plan and verify results
	scan, err := plan.Open(ctx)
	if err != nil {
		t.Fatalf("failed to open plan: %v", err)
	}
	defer scan.Close()

	count := 0
	for {
		ok, err := scan.Next(ctx)
		if err != nil {
			t.Fatalf("failed to get next: %v", err)
		}
		if !ok {
			break
		}
		count++
	}

	// Only Alice(25) and Charlie(25) should match
	if count != 2 {
		t.Errorf("expected 2 records, got %d", count)
	}
}

func TestQueryPlannerCreatePlanMultipleTables(t *testing.T) {
	mm, tx, cleanup := setupQueryPlannerTest(t)
	defer cleanup()

	ctx := context.Background()

	// Create users table
	usersSchema := dbrecord.NewSchema()
	usersSchema.AddIntField("id")
	usersSchema.AddStringField("name", 20)

	if err := mm.CreateTable(ctx, "users", usersSchema, tx); err != nil {
		t.Fatalf("failed to create users table: %v", err)
	}

	// Create orders table
	ordersSchema := dbrecord.NewSchema()
	ordersSchema.AddIntField("orderid")
	ordersSchema.AddIntField("userid")
	ordersSchema.AddIntField("amount")

	if err := mm.CreateTable(ctx, "orders", ordersSchema, tx); err != nil {
		t.Fatalf("failed to create orders table: %v", err)
	}

	// Insert users data
	usersLayout, err := mm.GetLayout(ctx, "users", tx)
	if err != nil {
		t.Fatalf("failed to get users layout: %v", err)
	}

	usersScan, err := dbrecord.NewTableScan(ctx, tx, "users", usersLayout)
	if err != nil {
		t.Fatalf("failed to create users scan: %v", err)
	}

	usersData := []struct {
		id   int
		name string
	}{
		{1, "Alice"},
		{2, "Bob"},
	}

	for _, data := range usersData {
		if err := usersScan.Insert(ctx); err != nil {
			t.Fatalf("failed to insert user: %v", err)
		}
		if err := usersScan.SetInt(ctx, "id", data.id); err != nil {
			t.Fatalf("failed to set user id: %v", err)
		}
		if err := usersScan.SetString(ctx, "name", data.name); err != nil {
			t.Fatalf("failed to set user name: %v", err)
		}
	}
	usersScan.Close()

	// Insert orders data
	ordersLayout, err := mm.GetLayout(ctx, "orders", tx)
	if err != nil {
		t.Fatalf("failed to get orders layout: %v", err)
	}

	ordersScan, err := dbrecord.NewTableScan(ctx, tx, "orders", ordersLayout)
	if err != nil {
		t.Fatalf("failed to create orders scan: %v", err)
	}

	ordersData := []struct {
		orderid int
		userid  int
		amount  int
	}{
		{1, 1, 100},
		{2, 2, 200},
		{3, 1, 150},
	}

	for _, data := range ordersData {
		if err := ordersScan.Insert(ctx); err != nil {
			t.Fatalf("failed to insert order: %v", err)
		}
		if err := ordersScan.SetInt(ctx, "orderid", data.orderid); err != nil {
			t.Fatalf("failed to set orderid: %v", err)
		}
		if err := ordersScan.SetInt(ctx, "userid", data.userid); err != nil {
			t.Fatalf("failed to set userid: %v", err)
		}
		if err := ordersScan.SetInt(ctx, "amount", data.amount); err != nil {
			t.Fatalf("failed to set amount: %v", err)
		}
	}
	ordersScan.Close()

	// Create query planner and execute join query
	qp := dbplan.NewQueryPlanner(mm)

	p := dbparse.NewParser("SELECT name, amount FROM users, orders WHERE id = userid")
	queryData, err := p.Query()
	if err != nil {
		t.Fatalf("failed to parse query: %v", err)
	}

	plan, err := qp.CreatePlan(ctx, queryData, tx)
	if err != nil {
		t.Fatalf("failed to create plan: %v", err)
	}

	// Verify the plan schema
	planSchema := plan.Schema()
	if !contains(planSchema.Fields(), "name") {
		t.Errorf("expected 'name' in schema fields")
	}
	if !contains(planSchema.Fields(), "amount") {
		t.Errorf("expected 'amount' in schema fields")
	}

	// Execute the plan and verify results
	scan, err := plan.Open(ctx)
	if err != nil {
		t.Fatalf("failed to open plan: %v", err)
	}
	defer scan.Close()

	count := 0
	for {
		ok, err := scan.Next(ctx)
		if err != nil {
			t.Fatalf("failed to get next: %v", err)
		}
		if !ok {
			break
		}
		count++
	}

	// 3 matching rows (Alice-100, Alice-150, Bob-200)
	if count != 3 {
		t.Errorf("expected 3 records, got %d", count)
	}
}

func TestQueryPlannerCreatePlanWithView(t *testing.T) {
	mm, tx, cleanup := setupQueryPlannerTest(t)
	defer cleanup()

	ctx := context.Background()

	// Create a test table
	schema := dbrecord.NewSchema()
	schema.AddIntField("id")
	schema.AddStringField("name", 20)
	schema.AddIntField("age")

	if err := mm.CreateTable(ctx, "users", schema, tx); err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Insert test data
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

	// Create view
	if err := mm.CreateView(ctx, "young_users", "SELECT id, name FROM users WHERE age = 25", tx); err != nil {
		t.Fatalf("failed to create view: %v", err)
	}

	// Create query planner and execute query on view
	qp := dbplan.NewQueryPlanner(mm)

	p := dbparse.NewParser("SELECT id, name FROM young_users")
	queryData, err := p.Query()
	if err != nil {
		t.Fatalf("failed to parse query: %v", err)
	}

	plan, err := qp.CreatePlan(ctx, queryData, tx)
	if err != nil {
		t.Fatalf("failed to create plan: %v", err)
	}

	// Execute the plan and verify results
	scan, err := plan.Open(ctx)
	if err != nil {
		t.Fatalf("failed to open plan: %v", err)
	}
	defer scan.Close()

	count := 0
	for {
		ok, err := scan.Next(ctx)
		if err != nil {
			t.Fatalf("failed to get next: %v", err)
		}
		if !ok {
			break
		}
		count++
	}

	// Only Alice(25) and Charlie(25) should be in the view
	if count != 2 {
		t.Errorf("expected 2 records from view, got %d", count)
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
