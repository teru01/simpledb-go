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

func setupProductScanTest(t *testing.T) (*dbtx.Transaction, *dbrecord.Layout, *dbrecord.Layout, string, string, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "productscan_test")
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

	// Table 1: users
	schema1 := dbrecord.NewSchema()
	schema1.AddIntField("user_id")
	schema1.AddStringField("user_name", 20)
	layout1 := dbrecord.NewLayout(schema1)

	// Table 2: orders
	schema2 := dbrecord.NewSchema()
	schema2.AddIntField("order_id")
	schema2.AddIntField("amount")
	layout2 := dbrecord.NewLayout(schema2)

	cleanup := func() {
		tx.Commit()
		dirFile.Close()
		os.RemoveAll(dir)
	}

	return tx, layout1, layout2, "users", "orders", cleanup
}

func TestProductScanCartesianProduct(t *testing.T) {
	tx, layout1, layout2, tableName1, tableName2, cleanup := setupProductScanTest(t)
	defer cleanup()

	ctx := context.Background()

	// Create and populate users table
	ts1, err := dbrecord.NewTableScan(ctx, tx, tableName1, layout1)
	if err != nil {
		t.Fatalf("failed to create table scan 1: %v", err)
	}

	users := []struct {
		id   int
		name string
	}{
		{1, "Alice"},
		{2, "Bob"},
	}

	for _, u := range users {
		if err := ts1.Insert(ctx); err != nil {
			t.Fatalf("failed to insert user: %v", err)
		}
		if err := ts1.SetInt(ctx, "user_id", u.id); err != nil {
			t.Fatalf("failed to set user_id: %v", err)
		}
		if err := ts1.SetString(ctx, "user_name", u.name); err != nil {
			t.Fatalf("failed to set user_name: %v", err)
		}
	}

	// Create and populate orders table
	ts2, err := dbrecord.NewTableScan(ctx, tx, tableName2, layout2)
	if err != nil {
		t.Fatalf("failed to create table scan 2: %v", err)
	}

	orders := []struct {
		id     int
		amount int
	}{
		{101, 1000},
		{102, 2000},
		{103, 3000},
	}

	for _, o := range orders {
		if err := ts2.Insert(ctx); err != nil {
			t.Fatalf("failed to insert order: %v", err)
		}
		if err := ts2.SetInt(ctx, "order_id", o.id); err != nil {
			t.Fatalf("failed to set order_id: %v", err)
		}
		if err := ts2.SetInt(ctx, "amount", o.amount); err != nil {
			t.Fatalf("failed to set amount: %v", err)
		}
	}

	// Create product scan (cartesian product)
	productScan := dbquery.NewProductScan(ts1, ts2)
	if err := productScan.SetStateToBeforeFirst(ctx); err != nil {
		t.Fatalf("failed to set state to before first: %v", err)
	}
	defer productScan.Close()

	// Cartesian product should have 2 * 3 = 6 records
	count := 0
	for {
		ok, err := productScan.Next(ctx)
		if err != nil {
			t.Fatalf("failed to move to next: %v", err)
		}
		if !ok {
			break
		}

		userID, err := productScan.GetInt(ctx, "user_id")
		if err != nil {
			t.Fatalf("failed to get user_id: %v", err)
		}

		userName, err := productScan.GetString(ctx, "user_name")
		if err != nil {
			t.Fatalf("failed to get user_name: %v", err)
		}

		orderID, err := productScan.GetInt(ctx, "order_id")
		if err != nil {
			t.Fatalf("failed to get order_id: %v", err)
		}

		amount, err := productScan.GetInt(ctx, "amount")
		if err != nil {
			t.Fatalf("failed to get amount: %v", err)
		}

		t.Logf("row %d: user_id=%d, user_name=%s, order_id=%d, amount=%d",
			count, userID, userName, orderID, amount)

		count++
	}

	expectedCount := len(users) * len(orders)
	if count != expectedCount {
		t.Errorf("expected %d records (cartesian product), got %d", expectedCount, count)
	}
}

func TestProductScanHasField(t *testing.T) {
	tx, layout1, layout2, tableName1, tableName2, cleanup := setupProductScanTest(t)
	defer cleanup()

	ctx := context.Background()

	ts1, err := dbrecord.NewTableScan(ctx, tx, tableName1, layout1)
	if err != nil {
		t.Fatalf("failed to create table scan 1: %v", err)
	}

	ts2, err := dbrecord.NewTableScan(ctx, tx, tableName2, layout2)
	if err != nil {
		t.Fatalf("failed to create table scan 2: %v", err)
	}

	productScan := dbquery.NewProductScan(ts1, ts2)
	defer productScan.Close()

	// Should have fields from both tables
	if !productScan.HasField("user_id") {
		t.Errorf("expected HasField('user_id') to be true")
	}
	if !productScan.HasField("user_name") {
		t.Errorf("expected HasField('user_name') to be true")
	}
	if !productScan.HasField("order_id") {
		t.Errorf("expected HasField('order_id') to be true")
	}
	if !productScan.HasField("amount") {
		t.Errorf("expected HasField('amount') to be true")
	}
	if productScan.HasField("nonexistent") {
		t.Errorf("expected HasField('nonexistent') to be false")
	}
}

func TestProductScanGetValue(t *testing.T) {
	tx, layout1, layout2, tableName1, tableName2, cleanup := setupProductScanTest(t)
	defer cleanup()

	ctx := context.Background()

	ts1, err := dbrecord.NewTableScan(ctx, tx, tableName1, layout1)
	if err != nil {
		t.Fatalf("failed to create table scan 1: %v", err)
	}

	if err := ts1.Insert(ctx); err != nil {
		t.Fatalf("failed to insert: %v", err)
	}
	if err := ts1.SetInt(ctx, "user_id", 1); err != nil {
		t.Fatalf("failed to set user_id: %v", err)
	}
	if err := ts1.SetString(ctx, "user_name", "Alice"); err != nil {
		t.Fatalf("failed to set user_name: %v", err)
	}

	if err := ts1.SetStateToBeforeFirst(ctx); err != nil {
		t.Fatalf("failed to reset ts1: %v", err)
	}

	ts2, err := dbrecord.NewTableScan(ctx, tx, tableName2, layout2)
	if err != nil {
		t.Fatalf("failed to create table scan 2: %v", err)
	}

	if err := ts2.Insert(ctx); err != nil {
		t.Fatalf("failed to insert: %v", err)
	}
	if err := ts2.SetInt(ctx, "order_id", 100); err != nil {
		t.Fatalf("failed to set order_id: %v", err)
	}
	if err := ts2.SetInt(ctx, "amount", 5000); err != nil {
		t.Fatalf("failed to set amount: %v", err)
	}

	if err := ts2.SetStateToBeforeFirst(ctx); err != nil {
		t.Fatalf("failed to reset ts2: %v", err)
	}

	productScan := dbquery.NewProductScan(ts1, ts2)
	if err := productScan.SetStateToBeforeFirst(ctx); err != nil {
		t.Fatalf("failed to set state to before first: %v", err)
	}
	defer productScan.Close()

	ok, err := productScan.Next(ctx)
	if err != nil {
		t.Fatalf("failed to move to next: %v", err)
	}
	if !ok {
		t.Fatalf("expected a record")
	}

	val, err := productScan.GetValue(ctx, "user_id")
	if err != nil {
		t.Fatalf("failed to get value for user_id: %v", err)
	}
	if val.AsRaw().(int) != 1 {
		t.Errorf("expected user_id=1, got %v", val.AsRaw())
	}

	val, err = productScan.GetValue(ctx, "amount")
	if err != nil {
		t.Fatalf("failed to get value for amount: %v", err)
	}
	if val.AsRaw().(int) != 5000 {
		t.Errorf("expected amount=5000, got %v", val.AsRaw())
	}
}

func TestProductScanEmptyTable(t *testing.T) {
	tx, layout1, layout2, tableName1, tableName2, cleanup := setupProductScanTest(t)
	defer cleanup()

	ctx := context.Background()

	// ts1 has data, ts2 is empty
	ts1, err := dbrecord.NewTableScan(ctx, tx, tableName1, layout1)
	if err != nil {
		t.Fatalf("failed to create table scan 1: %v", err)
	}

	if err := ts1.Insert(ctx); err != nil {
		t.Fatalf("failed to insert: %v", err)
	}
	if err := ts1.SetInt(ctx, "user_id", 1); err != nil {
		t.Fatalf("failed to set user_id: %v", err)
	}

	if err := ts1.SetStateToBeforeFirst(ctx); err != nil {
		t.Fatalf("failed to reset ts1: %v", err)
	}

	ts2, err := dbrecord.NewTableScan(ctx, tx, tableName2, layout2)
	if err != nil {
		t.Fatalf("failed to create table scan 2: %v", err)
	}

	// ts2 is empty - no inserts

	productScan := dbquery.NewProductScan(ts1, ts2)
	if err := productScan.SetStateToBeforeFirst(ctx); err != nil {
		t.Fatalf("failed to set state to before first: %v", err)
	}
	defer productScan.Close()

	// Cartesian product with empty table should yield no results
	ok, err := productScan.Next(ctx)
	if err != nil {
		t.Fatalf("failed to move to next: %v", err)
	}
	if ok {
		t.Errorf("expected no records when one table is empty")
	}
}
