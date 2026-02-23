package dbplan_test

import (
	"context"
	"testing"

	"github.com/teru01/simpledb-go/dbconstant"
	"github.com/teru01/simpledb-go/dbparse"
	"github.com/teru01/simpledb-go/dbplan"
	"github.com/teru01/simpledb-go/dbrecord"
)

// --- IndexSelectPlan tests ---

func TestIndexSelectPlanOpen(t *testing.T) {
	mm, tx, cleanup := setupQueryPlannerTest(t)
	defer cleanup()

	ctx := context.Background()

	// Create table
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

	for _, d := range testData {
		if err := ts.Insert(ctx); err != nil {
			t.Fatalf("failed to insert: %v", err)
		}
		if err := ts.SetInt(ctx, "id", d.id); err != nil {
			t.Fatalf("failed to set id: %v", err)
		}
		if err := ts.SetString(ctx, "name", d.name); err != nil {
			t.Fatalf("failed to set name: %v", err)
		}
		if err := ts.SetInt(ctx, "age", d.age); err != nil {
			t.Fatalf("failed to set age: %v", err)
		}
	}
	ts.Close(ctx)

	// Create index on "id"
	if err := mm.CreateIndex(ctx, "idx_users_id", "users", "id", tx); err != nil {
		t.Fatalf("failed to create index: %v", err)
	}

	// Index entries are automatically built by CreateIndex

	indexInfos, err := mm.GetIndexInfo(ctx, "users", tx)
	if err != nil {
		t.Fatalf("failed to get index info: %v", err)
	}
	idxInfo := indexInfos["id"]

	// Use IndexSelectPlan to search for id=3
	tablePlan, err := dbplan.NewTablePlan(ctx, tx, "users", mm)
	if err != nil {
		t.Fatalf("failed to create table plan: %v", err)
	}

	indexSelectPlan := dbplan.NewIndexSelectPlan(tablePlan, *idxInfo, dbconstant.NewIntConstant(3))
	scan, err := indexSelectPlan.Open(ctx)
	if err != nil {
		t.Fatalf("failed to open index select plan: %v", err)
	}
	defer scan.Close(ctx)

	count := 0
	for {
		ok, err := scan.Next(ctx)
		if err != nil {
			t.Fatalf("failed to next: %v", err)
		}
		if !ok {
			break
		}
		id, err := scan.GetInt(ctx, "id")
		if err != nil {
			t.Fatalf("failed to get id: %v", err)
		}
		if id != 3 {
			t.Errorf("expected id=3, got %d", id)
		}
		name, err := scan.GetString(ctx, "name")
		if err != nil {
			t.Fatalf("failed to get name: %v", err)
		}
		if name != "Charlie" {
			t.Errorf("expected name=Charlie, got %s", name)
		}
		count++
	}

	if count != 1 {
		t.Errorf("expected 1 record, got %d", count)
	}
}

func TestIndexSelectPlanNotFound(t *testing.T) {
	mm, tx, cleanup := setupQueryPlannerTest(t)
	defer cleanup()

	ctx := context.Background()

	schema := dbrecord.NewSchema()
	schema.AddIntField("id")
	schema.AddStringField("name", 20)

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
	if err := ts.Insert(ctx); err != nil {
		t.Fatalf("failed to insert: %v", err)
	}
	if err := ts.SetInt(ctx, "id", 1); err != nil {
		t.Fatalf("failed to set id: %v", err)
	}
	if err := ts.SetString(ctx, "name", "Item1"); err != nil {
		t.Fatalf("failed to set name: %v", err)
	}
	ts.Close(ctx)

	// Create index (entries are automatically built)
	if err := mm.CreateIndex(ctx, "idx_items_id", "items", "id", tx); err != nil {
		t.Fatalf("failed to create index: %v", err)
	}

	indexInfos, err := mm.GetIndexInfo(ctx, "items", tx)
	if err != nil {
		t.Fatalf("failed to get index info: %v", err)
	}
	idxInfo := indexInfos["id"]

	// Search for non-existent key
	tablePlan, err := dbplan.NewTablePlan(ctx, tx, "items", mm)
	if err != nil {
		t.Fatalf("failed to create table plan: %v", err)
	}

	indexSelectPlan := dbplan.NewIndexSelectPlan(tablePlan, *idxInfo, dbconstant.NewIntConstant(999))
	scan, err := indexSelectPlan.Open(ctx)
	if err != nil {
		t.Fatalf("failed to open: %v", err)
	}
	defer scan.Close(ctx)

	ok, err := scan.Next(ctx)
	if err != nil {
		t.Fatalf("failed to next: %v", err)
	}
	if ok {
		t.Error("expected no records for non-existent key")
	}
}

func TestIndexSelectPlanSchema(t *testing.T) {
	mm, tx, cleanup := setupQueryPlannerTest(t)
	defer cleanup()

	ctx := context.Background()

	schema := dbrecord.NewSchema()
	schema.AddIntField("id")
	schema.AddStringField("name", 20)

	if err := mm.CreateTable(ctx, "items", schema, tx); err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	if err := mm.CreateIndex(ctx, "idx_items_id", "items", "id", tx); err != nil {
		t.Fatalf("failed to create index: %v", err)
	}

	indexInfos, err := mm.GetIndexInfo(ctx, "items", tx)
	if err != nil {
		t.Fatalf("failed to get index info: %v", err)
	}
	idxInfo := indexInfos["id"]

	tablePlan, err := dbplan.NewTablePlan(ctx, tx, "items", mm)
	if err != nil {
		t.Fatalf("failed to create table plan: %v", err)
	}

	plan := dbplan.NewIndexSelectPlan(tablePlan, *idxInfo, dbconstant.NewIntConstant(1))
	planSchema := plan.Schema()

	if !contains(planSchema.Fields(), "id") {
		t.Error("expected 'id' in schema")
	}
	if !contains(planSchema.Fields(), "name") {
		t.Error("expected 'name' in schema")
	}
}

// --- IndexJoinPlan tests ---

func TestIndexJoinPlanOpen(t *testing.T) {
	mm, tx, cleanup := setupQueryPlannerTest(t)
	defer cleanup()

	ctx := context.Background()

	// Create users table
	usersSchema := dbrecord.NewSchema()
	usersSchema.AddIntField("uid")
	usersSchema.AddStringField("uname", 20)

	if err := mm.CreateTable(ctx, "jusers", usersSchema, tx); err != nil {
		t.Fatalf("failed to create users table: %v", err)
	}

	// Create orders table
	ordersSchema := dbrecord.NewSchema()
	ordersSchema.AddIntField("oid")
	ordersSchema.AddIntField("ouid")
	ordersSchema.AddIntField("amount")

	if err := mm.CreateTable(ctx, "jorders", ordersSchema, tx); err != nil {
		t.Fatalf("failed to create orders table: %v", err)
	}

	// Insert users
	usersLayout, err := mm.GetLayout(ctx, "jusers", tx)
	if err != nil {
		t.Fatalf("failed to get users layout: %v", err)
	}
	uTs, err := dbrecord.NewTableScan(ctx, tx, "jusers", usersLayout)
	if err != nil {
		t.Fatalf("failed to create users scan: %v", err)
	}

	usersData := []struct {
		uid   int
		uname string
	}{
		{1, "Alice"},
		{2, "Bob"},
		{3, "Charlie"},
	}
	for _, d := range usersData {
		if err := uTs.Insert(ctx); err != nil {
			t.Fatalf("failed to insert user: %v", err)
		}
		if err := uTs.SetInt(ctx, "uid", d.uid); err != nil {
			t.Fatalf("failed to set uid: %v", err)
		}
		if err := uTs.SetString(ctx, "uname", d.uname); err != nil {
			t.Fatalf("failed to set uname: %v", err)
		}
	}
	uTs.Close(ctx)

	// Insert orders
	ordersLayout, err := mm.GetLayout(ctx, "jorders", tx)
	if err != nil {
		t.Fatalf("failed to get orders layout: %v", err)
	}
	oTs, err := dbrecord.NewTableScan(ctx, tx, "jorders", ordersLayout)
	if err != nil {
		t.Fatalf("failed to create orders scan: %v", err)
	}

	ordersData := []struct {
		oid    int
		ouid   int
		amount int
	}{
		{1, 1, 100},
		{2, 2, 200},
		{3, 1, 150},
		{4, 3, 300},
	}
	for _, d := range ordersData {
		if err := oTs.Insert(ctx); err != nil {
			t.Fatalf("failed to insert order: %v", err)
		}
		if err := oTs.SetInt(ctx, "oid", d.oid); err != nil {
			t.Fatalf("failed to set oid: %v", err)
		}
		if err := oTs.SetInt(ctx, "ouid", d.ouid); err != nil {
			t.Fatalf("failed to set ouid: %v", err)
		}
		if err := oTs.SetInt(ctx, "amount", d.amount); err != nil {
			t.Fatalf("failed to set amount: %v", err)
		}
	}
	oTs.Close(ctx)

	// Create index on orders.ouid
	if err := mm.CreateIndex(ctx, "idx_jorders_ouid", "jorders", "ouid", tx); err != nil {
		t.Fatalf("failed to create index: %v", err)
	}

	// Index entries are automatically built by CreateIndex
	indexInfos, err := mm.GetIndexInfo(ctx, "jorders", tx)
	if err != nil {
		t.Fatalf("failed to get index info: %v", err)
	}
	idxInfo := indexInfos["ouid"]

	// Create IndexJoinPlan: join jusers.uid = jorders.ouid
	p1, err := dbplan.NewTablePlan(ctx, tx, "jusers", mm)
	if err != nil {
		t.Fatalf("failed to create users plan: %v", err)
	}
	p2, err := dbplan.NewTablePlan(ctx, tx, "jorders", mm)
	if err != nil {
		t.Fatalf("failed to create orders plan: %v", err)
	}

	joinPlan := dbplan.NewIndexJoinPlan(p1, p2, idxInfo, "uid")
	scan, err := joinPlan.Open(ctx)
	if err != nil {
		t.Fatalf("failed to open join plan: %v", err)
	}
	defer scan.Close(ctx)

	// Verify results: each user should join with their orders
	// Alice(uid=1): order 100, 150
	// Bob(uid=2): order 200
	// Charlie(uid=3): order 300
	type result struct {
		uname  string
		amount int
	}
	var results []result
	for {
		ok, err := scan.Next(ctx)
		if err != nil {
			t.Fatalf("failed to next: %v", err)
		}
		if !ok {
			break
		}
		uname, err := scan.GetString(ctx, "uname")
		if err != nil {
			t.Fatalf("failed to get uname: %v", err)
		}
		amount, err := scan.GetInt(ctx, "amount")
		if err != nil {
			t.Fatalf("failed to get amount: %v", err)
		}
		results = append(results, result{uname, amount})
	}

	if len(results) != 4 {
		t.Fatalf("expected 4 join results, got %d", len(results))
	}

	// Check that Alice has 2 orders, Bob has 1, Charlie has 1
	countByUser := make(map[string]int)
	for _, r := range results {
		countByUser[r.uname]++
	}
	if countByUser["Alice"] != 2 {
		t.Errorf("expected 2 orders for Alice, got %d", countByUser["Alice"])
	}
	if countByUser["Bob"] != 1 {
		t.Errorf("expected 1 order for Bob, got %d", countByUser["Bob"])
	}
	if countByUser["Charlie"] != 1 {
		t.Errorf("expected 1 order for Charlie, got %d", countByUser["Charlie"])
	}
}

func TestIndexJoinPlanNoMatch(t *testing.T) {
	mm, tx, cleanup := setupQueryPlannerTest(t)
	defer cleanup()

	ctx := context.Background()

	// Create left table
	leftSchema := dbrecord.NewSchema()
	leftSchema.AddIntField("lid")

	if err := mm.CreateTable(ctx, "left_t", leftSchema, tx); err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Create right table
	rightSchema := dbrecord.NewSchema()
	rightSchema.AddIntField("rid")

	if err := mm.CreateTable(ctx, "right_t", rightSchema, tx); err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Insert data with no matching keys
	leftLayout, err := mm.GetLayout(ctx, "left_t", tx)
	if err != nil {
		t.Fatalf("failed to get layout: %v", err)
	}
	lTs, err := dbrecord.NewTableScan(ctx, tx, "left_t", leftLayout)
	if err != nil {
		t.Fatalf("failed to create scan: %v", err)
	}
	if err := lTs.Insert(ctx); err != nil {
		t.Fatalf("failed to insert: %v", err)
	}
	if err := lTs.SetInt(ctx, "lid", 100); err != nil {
		t.Fatalf("failed to set: %v", err)
	}
	lTs.Close(ctx)

	rightLayout, err := mm.GetLayout(ctx, "right_t", tx)
	if err != nil {
		t.Fatalf("failed to get layout: %v", err)
	}
	rTs, err := dbrecord.NewTableScan(ctx, tx, "right_t", rightLayout)
	if err != nil {
		t.Fatalf("failed to create scan: %v", err)
	}
	if err := rTs.Insert(ctx); err != nil {
		t.Fatalf("failed to insert: %v", err)
	}
	if err := rTs.SetInt(ctx, "rid", 999); err != nil {
		t.Fatalf("failed to set: %v", err)
	}
	rTs.Close(ctx)

	// Create index on right_t.rid
	if err := mm.CreateIndex(ctx, "idx_right_rid", "right_t", "rid", tx); err != nil {
		t.Fatalf("failed to create index: %v", err)
	}

	// Index entries are automatically built by CreateIndex
	indexInfos, err := mm.GetIndexInfo(ctx, "right_t", tx)
	if err != nil {
		t.Fatalf("failed to get index info: %v", err)
	}
	idxInfo := indexInfos["rid"]

	p1, err := dbplan.NewTablePlan(ctx, tx, "left_t", mm)
	if err != nil {
		t.Fatalf("failed to create plan: %v", err)
	}
	p2, err := dbplan.NewTablePlan(ctx, tx, "right_t", mm)
	if err != nil {
		t.Fatalf("failed to create plan: %v", err)
	}

	joinPlan := dbplan.NewIndexJoinPlan(p1, p2, idxInfo, "lid")
	scan, err := joinPlan.Open(ctx)
	if err != nil {
		t.Fatalf("failed to open: %v", err)
	}
	defer scan.Close(ctx)

	ok, err := scan.Next(ctx)
	if err != nil {
		t.Fatalf("failed to next: %v", err)
	}
	if ok {
		t.Error("expected no results for non-matching join")
	}
}

// --- IndexUpdatePlanner tests ---

func TestIndexUpdatePlannerInsert(t *testing.T) {
	mm, tx, cleanup := setupQueryPlannerTest(t)
	defer cleanup()

	ctx := context.Background()

	// Create table
	schema := dbrecord.NewSchema()
	schema.AddIntField("id")
	schema.AddStringField("name", 20)

	if err := mm.CreateTable(ctx, "products", schema, tx); err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Create index on "id"
	if err := mm.CreateIndex(ctx, "idx_products_id", "products", "id", tx); err != nil {
		t.Fatalf("failed to create index: %v", err)
	}

	// Use IndexUpdatePlanner to insert
	planner := dbplan.NewIndexUpdatePlanner(mm)

	insertData := dbparse.NewInsertData("products",
		[]string{"id", "name"},
		[]dbconstant.Constant{
			dbconstant.NewIntConstant(42),
			dbconstant.NewStringConstant("Widget"),
		},
	)

	n, err := planner.ExecuteInsert(ctx, insertData, tx)
	if err != nil {
		t.Fatalf("failed to execute insert: %v", err)
	}
	if n != 1 {
		t.Errorf("expected 1 affected row, got %d", n)
	}

	// Verify data was inserted into the table
	layout, err := mm.GetLayout(ctx, "products", tx)
	if err != nil {
		t.Fatalf("failed to get layout: %v", err)
	}
	ts, err := dbrecord.NewTableScan(ctx, tx, "products", layout)
	if err != nil {
		t.Fatalf("failed to create table scan: %v", err)
	}
	defer ts.Close(ctx)

	ok, err := ts.Next(ctx)
	if err != nil {
		t.Fatalf("failed to next: %v", err)
	}
	if !ok {
		t.Fatal("expected to find inserted record")
	}

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
	if name != "Widget" {
		t.Errorf("expected name=Widget, got %s", name)
	}

	// Verify the index was updated
	indexInfos, err := mm.GetIndexInfo(ctx, "products", tx)
	if err != nil {
		t.Fatalf("failed to get index info: %v", err)
	}
	idxInfo := indexInfos["id"]
	idx, err := idxInfo.Open(ctx)
	if err != nil {
		t.Fatalf("failed to open index: %v", err)
	}
	defer idx.Close(ctx)

	if err := idx.BeforeFirst(ctx, dbconstant.NewIntConstant(42)); err != nil {
		t.Fatalf("failed to before first: %v", err)
	}

	found, err := idx.Next(ctx)
	if err != nil {
		t.Fatalf("failed to next: %v", err)
	}
	if !found {
		t.Error("expected to find key 42 in index")
	}
}

func TestIndexUpdatePlannerDelete(t *testing.T) {
	mm, tx, cleanup := setupQueryPlannerTest(t)
	defer cleanup()

	ctx := context.Background()

	schema := dbrecord.NewSchema()
	schema.AddIntField("id")
	schema.AddStringField("name", 20)

	if err := mm.CreateTable(ctx, "items", schema, tx); err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	if err := mm.CreateIndex(ctx, "idx_items_id", "items", "id", tx); err != nil {
		t.Fatalf("failed to create index: %v", err)
	}

	planner := dbplan.NewIndexUpdatePlanner(mm)

	// Insert 3 records
	for i, name := range []string{"A", "B", "C"} {
		insertData := dbparse.NewInsertData("items",
			[]string{"id", "name"},
			[]dbconstant.Constant{
				dbconstant.NewIntConstant(i + 1),
				dbconstant.NewStringConstant(name),
			},
		)
		if _, err := planner.ExecuteInsert(ctx, insertData, tx); err != nil {
			t.Fatalf("failed to insert: %v", err)
		}
	}

	// Delete where id = 2
	p := dbparse.NewParser("DELETE FROM items WHERE id = 2")
	deleteData, err := p.Delete()
	if err != nil {
		t.Fatalf("failed to parse delete: %v", err)
	}

	n, err := planner.ExecuteDelete(ctx, deleteData, tx)
	if err != nil {
		t.Fatalf("failed to execute delete: %v", err)
	}
	if n != 1 {
		t.Errorf("expected 1 deleted row, got %d", n)
	}

	// Verify only 2 records remain
	layout, err := mm.GetLayout(ctx, "items", tx)
	if err != nil {
		t.Fatalf("failed to get layout: %v", err)
	}
	ts, err := dbrecord.NewTableScan(ctx, tx, "items", layout)
	if err != nil {
		t.Fatalf("failed to create scan: %v", err)
	}
	defer ts.Close(ctx)

	count := 0
	for {
		ok, err := ts.Next(ctx)
		if err != nil {
			t.Fatalf("failed to next: %v", err)
		}
		if !ok {
			break
		}
		id, err := ts.GetInt(ctx, "id")
		if err != nil {
			t.Fatalf("failed to get id: %v", err)
		}
		if id == 2 {
			t.Error("deleted record (id=2) should not exist")
		}
		count++
	}

	if count != 2 {
		t.Errorf("expected 2 records, got %d", count)
	}
}

func TestIndexUpdatePlannerModify(t *testing.T) {
	mm, tx, cleanup := setupQueryPlannerTest(t)
	defer cleanup()

	ctx := context.Background()

	schema := dbrecord.NewSchema()
	schema.AddIntField("id")
	schema.AddIntField("val")

	if err := mm.CreateTable(ctx, "things", schema, tx); err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	if err := mm.CreateIndex(ctx, "idx_things_val", "things", "val", tx); err != nil {
		t.Fatalf("failed to create index: %v", err)
	}

	planner := dbplan.NewIndexUpdatePlanner(mm)

	// Insert records
	for i := 1; i <= 3; i++ {
		insertData := dbparse.NewInsertData("things",
			[]string{"id", "val"},
			[]dbconstant.Constant{
				dbconstant.NewIntConstant(i),
				dbconstant.NewIntConstant(i * 10),
			},
		)
		if _, err := planner.ExecuteInsert(ctx, insertData, tx); err != nil {
			t.Fatalf("failed to insert: %v", err)
		}
	}

	// Update val where id = 2
	p := dbparse.NewParser("UPDATE things SET val = 99 WHERE id = 2")
	modifyData, err := p.Modify()
	if err != nil {
		t.Fatalf("failed to parse modify: %v", err)
	}

	n, err := planner.ExecuteModify(ctx, modifyData, tx)
	if err != nil {
		t.Fatalf("failed to execute modify: %v", err)
	}
	if n != 1 {
		t.Errorf("expected 1 modified row, got %d", n)
	}

	// Verify the update
	layout, err := mm.GetLayout(ctx, "things", tx)
	if err != nil {
		t.Fatalf("failed to get layout: %v", err)
	}
	ts, err := dbrecord.NewTableScan(ctx, tx, "things", layout)
	if err != nil {
		t.Fatalf("failed to create scan: %v", err)
	}
	defer ts.Close(ctx)

	for {
		ok, err := ts.Next(ctx)
		if err != nil {
			t.Fatalf("failed to next: %v", err)
		}
		if !ok {
			break
		}
		id, err := ts.GetInt(ctx, "id")
		if err != nil {
			t.Fatalf("failed to get id: %v", err)
		}
		val, err := ts.GetInt(ctx, "val")
		if err != nil {
			t.Fatalf("failed to get val: %v", err)
		}
		if id == 1 && val != 10 {
			t.Errorf("expected val=10 for id=1, got %d", val)
		}
		if id == 2 && val != 99 {
			t.Errorf("expected val=99 for id=2, got %d", val)
		}
		if id == 3 && val != 30 {
			t.Errorf("expected val=30 for id=3, got %d", val)
		}
	}
}

func TestIndexUpdatePlannerInsertMultiple(t *testing.T) {
	mm, tx, cleanup := setupQueryPlannerTest(t)
	defer cleanup()

	ctx := context.Background()

	schema := dbrecord.NewSchema()
	schema.AddIntField("id")
	schema.AddIntField("val")

	if err := mm.CreateTable(ctx, "data_t", schema, tx); err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	if err := mm.CreateIndex(ctx, "idx_data_id", "data_t", "id", tx); err != nil {
		t.Fatalf("failed to create index: %v", err)
	}

	planner := dbplan.NewIndexUpdatePlanner(mm)

	// Insert multiple records
	for i := range 10 {
		insertData := dbparse.NewInsertData("data_t",
			[]string{"id", "val"},
			[]dbconstant.Constant{
				dbconstant.NewIntConstant(i),
				dbconstant.NewIntConstant(i * 100),
			},
		)
		if _, err := planner.ExecuteInsert(ctx, insertData, tx); err != nil {
			t.Fatalf("failed to insert: %v", err)
		}
	}

	// Verify all records via index
	indexInfos, err := mm.GetIndexInfo(ctx, "data_t", tx)
	if err != nil {
		t.Fatalf("failed to get index info: %v", err)
	}
	idxInfo := indexInfos["id"]

	tablePlan, err := dbplan.NewTablePlan(ctx, tx, "data_t", mm)
	if err != nil {
		t.Fatalf("failed to create table plan: %v", err)
	}

	// Search for each key
	for i := range 10 {
		plan := dbplan.NewIndexSelectPlan(tablePlan, *idxInfo, dbconstant.NewIntConstant(i))
		scan, err := plan.Open(ctx)
		if err != nil {
			t.Fatalf("failed to open for key %d: %v", i, err)
		}

		ok, err := scan.Next(ctx)
		if err != nil {
			t.Fatalf("failed to next for key %d: %v", i, err)
		}
		if !ok {
			t.Errorf("expected to find key %d", i)
			scan.Close(ctx)
			continue
		}

		val, err := scan.GetInt(ctx, "val")
		if err != nil {
			t.Fatalf("failed to get val for key %d: %v", i, err)
		}
		if val != i*100 {
			t.Errorf("key %d: expected val=%d, got %d", i, i*100, val)
		}
		scan.Close(ctx)
	}
}
