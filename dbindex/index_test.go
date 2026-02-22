package dbindex_test

import (
	"context"
	"os"
	"testing"

	"github.com/teru01/simpledb-go/dbbuffer"
	"github.com/teru01/simpledb-go/dbconstant"
	"github.com/teru01/simpledb-go/dbfile"
	"github.com/teru01/simpledb-go/dbindex"
	"github.com/teru01/simpledb-go/dblog"
	"github.com/teru01/simpledb-go/dbname"
	"github.com/teru01/simpledb-go/dbrecord"
	"github.com/teru01/simpledb-go/dbtx"
)

func setupIndexWithBlockSize(t *testing.T, blockSize int, stringDataValue bool) (*dbtx.Transaction, *dbrecord.Layout, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "index_test")
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
	schema.AddIntField(dbname.IndexFieldBlock)
	schema.AddIntField(dbname.IndexFieldID)
	if stringDataValue {
		schema.AddStringField(dbname.IndexFieldDataValue, 20)
	} else {
		schema.AddIntField(dbname.IndexFieldDataValue)
	}
	layout := dbrecord.NewLayout(schema)

	cleanup := func() {
		tx.Commit()
		dirFile.Close()
		os.RemoveAll(dir)
	}

	return tx, layout, cleanup
}

func setupIndex(t *testing.T) (*dbtx.Transaction, *dbrecord.Layout, func()) {
	return setupIndexWithBlockSize(t, 400, false)
}

func setupBTreeIndex(t *testing.T) (*dbtx.Transaction, *dbrecord.Layout, func()) {
	return setupIndexWithBlockSize(t, 4000, false)
}

func setupBTreeStringIndex(t *testing.T) (*dbtx.Transaction, *dbrecord.Layout, func()) {
	return setupIndexWithBlockSize(t, 4000, true)
}

// --- HashIndex tests ---

func TestHashIndexInsertAndSearch(t *testing.T) {
	tx, layout, cleanup := setupIndex(t)
	defer cleanup()

	ctx := context.Background()
	idx := dbindex.NewHashIndex(tx, "testhashidx", layout)

	rid := *dbrecord.NewRID(1, 2)
	val := dbconstant.NewIntConstant(42)

	if err := idx.Insert(ctx, val, rid); err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	if err := idx.BeforeFirst(ctx, val); err != nil {
		t.Fatalf("failed to before first: %v", err)
	}

	ok, err := idx.Next(ctx)
	if err != nil {
		t.Fatalf("failed to next: %v", err)
	}
	if !ok {
		t.Fatal("expected to find a record")
	}

	gotRID, err := idx.GetDataRID(ctx)
	if err != nil {
		t.Fatalf("failed to get data rid: %v", err)
	}
	if gotRID != rid {
		t.Errorf("expected RID %v, got %v", rid, gotRID)
	}

	ok, err = idx.Next(ctx)
	if err != nil {
		t.Fatalf("failed to next: %v", err)
	}
	if ok {
		t.Error("expected no more records")
	}

	if err := idx.Close(ctx); err != nil {
		t.Fatalf("failed to close: %v", err)
	}
}

func TestHashIndexMultipleInserts(t *testing.T) {
	tx, layout, cleanup := setupIndex(t)
	defer cleanup()

	ctx := context.Background()
	idx := dbindex.NewHashIndex(tx, "testhashidx", layout)

	rids := []dbrecord.RID{
		*dbrecord.NewRID(0, 0),
		*dbrecord.NewRID(0, 1),
		*dbrecord.NewRID(1, 0),
	}
	val := dbconstant.NewIntConstant(10)

	for _, rid := range rids {
		if err := idx.Insert(ctx, val, rid); err != nil {
			t.Fatalf("failed to insert %v: %v", rid, err)
		}
	}

	if err := idx.BeforeFirst(ctx, val); err != nil {
		t.Fatalf("failed to before first: %v", err)
	}

	found := make(map[dbrecord.RID]bool)
	for {
		ok, err := idx.Next(ctx)
		if err != nil {
			t.Fatalf("failed to next: %v", err)
		}
		if !ok {
			break
		}
		rid, err := idx.GetDataRID(ctx)
		if err != nil {
			t.Fatalf("failed to get data rid: %v", err)
		}
		found[rid] = true
	}

	if len(found) != len(rids) {
		t.Errorf("expected %d records, found %d", len(rids), len(found))
	}
	for _, rid := range rids {
		if !found[rid] {
			t.Errorf("missing RID %v", rid)
		}
	}

	if err := idx.Close(ctx); err != nil {
		t.Fatalf("failed to close: %v", err)
	}
}

func TestHashIndexSearchNotFound(t *testing.T) {
	tx, layout, cleanup := setupIndex(t)
	defer cleanup()

	ctx := context.Background()
	idx := dbindex.NewHashIndex(tx, "testhashidx", layout)

	rid := *dbrecord.NewRID(0, 0)
	if err := idx.Insert(ctx, dbconstant.NewIntConstant(10), rid); err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	if err := idx.BeforeFirst(ctx, dbconstant.NewIntConstant(99)); err != nil {
		t.Fatalf("failed to before first: %v", err)
	}

	ok, err := idx.Next(ctx)
	if err != nil {
		t.Fatalf("failed to next: %v", err)
	}
	if ok {
		t.Error("expected no records for non-existent key")
	}

	if err := idx.Close(ctx); err != nil {
		t.Fatalf("failed to close: %v", err)
	}
}

func TestHashIndexDelete(t *testing.T) {
	tx, layout, cleanup := setupIndex(t)
	defer cleanup()

	ctx := context.Background()
	idx := dbindex.NewHashIndex(tx, "testhashidx", layout)

	val := dbconstant.NewIntConstant(42)
	rid1 := *dbrecord.NewRID(0, 0)
	rid2 := *dbrecord.NewRID(0, 1)

	if err := idx.Insert(ctx, val, rid1); err != nil {
		t.Fatalf("failed to insert: %v", err)
	}
	if err := idx.Insert(ctx, val, rid2); err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	if err := idx.Delete(ctx, val, rid1); err != nil {
		t.Fatalf("failed to delete: %v", err)
	}

	if err := idx.BeforeFirst(ctx, val); err != nil {
		t.Fatalf("failed to before first: %v", err)
	}

	count := 0
	for {
		ok, err := idx.Next(ctx)
		if err != nil {
			t.Fatalf("failed to next: %v", err)
		}
		if !ok {
			break
		}
		rid, err := idx.GetDataRID(ctx)
		if err != nil {
			t.Fatalf("failed to get data rid: %v", err)
		}
		if rid == rid1 {
			t.Error("deleted RID should not be found")
		}
		count++
	}

	if count != 1 {
		t.Errorf("expected 1 record after delete, got %d", count)
	}

	if err := idx.Close(ctx); err != nil {
		t.Fatalf("failed to close: %v", err)
	}
}

func TestHashIndexDifferentKeys(t *testing.T) {
	tx, layout, cleanup := setupIndex(t)
	defer cleanup()

	ctx := context.Background()
	idx := dbindex.NewHashIndex(tx, "testhashidx", layout)

	val1 := dbconstant.NewIntConstant(10)
	val2 := dbconstant.NewIntConstant(20)
	rid1 := *dbrecord.NewRID(0, 0)
	rid2 := *dbrecord.NewRID(0, 1)

	if err := idx.Insert(ctx, val1, rid1); err != nil {
		t.Fatalf("failed to insert: %v", err)
	}
	if err := idx.Insert(ctx, val2, rid2); err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	// Search for val1
	if err := idx.BeforeFirst(ctx, val1); err != nil {
		t.Fatalf("failed to before first: %v", err)
	}
	ok, err := idx.Next(ctx)
	if err != nil {
		t.Fatalf("failed to next: %v", err)
	}
	if !ok {
		t.Fatal("expected to find record for val1")
	}
	gotRID, err := idx.GetDataRID(ctx)
	if err != nil {
		t.Fatalf("failed to get data rid: %v", err)
	}
	if gotRID != rid1 {
		t.Errorf("expected RID %v for val1, got %v", rid1, gotRID)
	}

	// Search for val2
	if err := idx.BeforeFirst(ctx, val2); err != nil {
		t.Fatalf("failed to before first: %v", err)
	}
	ok, err = idx.Next(ctx)
	if err != nil {
		t.Fatalf("failed to next: %v", err)
	}
	if !ok {
		t.Fatal("expected to find record for val2")
	}
	gotRID, err = idx.GetDataRID(ctx)
	if err != nil {
		t.Fatalf("failed to get data rid: %v", err)
	}
	if gotRID != rid2 {
		t.Errorf("expected RID %v for val2, got %v", rid2, gotRID)
	}

	if err := idx.Close(ctx); err != nil {
		t.Fatalf("failed to close: %v", err)
	}
}

// --- BTreeIndex tests ---

func TestBTreeIndexInsertAndSearch(t *testing.T) {
	tx, layout, cleanup := setupBTreeIndex(t)
	defer cleanup()

	ctx := context.Background()
	idx, err := dbindex.NewBTreeIndex(ctx, tx, "testbtreeidx", layout)
	if err != nil {
		t.Fatalf("failed to create btree index: %v", err)
	}

	rid := *dbrecord.NewRID(1, 2)
	val := dbconstant.NewIntConstant(42)

	if err := idx.Insert(ctx, val, rid); err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	if err := idx.BeforeFirst(ctx, val); err != nil {
		t.Fatalf("failed to before first: %v", err)
	}

	ok, err := idx.Next(ctx)
	if err != nil {
		t.Fatalf("failed to next: %v", err)
	}
	if !ok {
		t.Fatal("expected to find a record")
	}

	gotRID, err := idx.GetDataRID(ctx)
	if err != nil {
		t.Fatalf("failed to get data rid: %v", err)
	}
	if *gotRID != rid {
		t.Errorf("expected RID %v, got %v", rid, *gotRID)
	}

	ok, err = idx.Next(ctx)
	if err != nil {
		t.Fatalf("failed to next: %v", err)
	}
	if ok {
		t.Error("expected no more records")
	}

	if err := idx.Close(ctx); err != nil {
		t.Fatalf("failed to close: %v", err)
	}
}

func TestBTreeIndexMultipleInserts(t *testing.T) {
	tx, layout, cleanup := setupBTreeIndex(t)
	defer cleanup()

	ctx := context.Background()
	idx, err := dbindex.NewBTreeIndex(ctx, tx, "testbtreeidx", layout)
	if err != nil {
		t.Fatalf("failed to create btree index: %v", err)
	}

	val := dbconstant.NewIntConstant(10)
	rids := []dbrecord.RID{
		*dbrecord.NewRID(0, 0),
		*dbrecord.NewRID(0, 1),
		*dbrecord.NewRID(1, 0),
	}

	for _, rid := range rids {
		if err := idx.Insert(ctx, val, rid); err != nil {
			t.Fatalf("failed to insert %v: %v", rid, err)
		}
	}

	if err := idx.BeforeFirst(ctx, val); err != nil {
		t.Fatalf("failed to before first: %v", err)
	}

	found := make(map[dbrecord.RID]bool)
	for {
		ok, err := idx.Next(ctx)
		if err != nil {
			t.Fatalf("failed to next: %v", err)
		}
		if !ok {
			break
		}
		rid, err := idx.GetDataRID(ctx)
		if err != nil {
			t.Fatalf("failed to get data rid: %v", err)
		}
		found[*rid] = true
	}

	if len(found) != len(rids) {
		t.Errorf("expected %d records, found %d", len(rids), len(found))
	}
	for _, rid := range rids {
		if !found[rid] {
			t.Errorf("missing RID %v", rid)
		}
	}

	if err := idx.Close(ctx); err != nil {
		t.Fatalf("failed to close: %v", err)
	}
}

func TestBTreeIndexSearchNotFound(t *testing.T) {
	tx, layout, cleanup := setupBTreeIndex(t)
	defer cleanup()

	ctx := context.Background()
	idx, err := dbindex.NewBTreeIndex(ctx, tx, "testbtreeidx", layout)
	if err != nil {
		t.Fatalf("failed to create btree index: %v", err)
	}

	rid := *dbrecord.NewRID(0, 0)
	if err := idx.Insert(ctx, dbconstant.NewIntConstant(10), rid); err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	if err := idx.BeforeFirst(ctx, dbconstant.NewIntConstant(99)); err != nil {
		t.Fatalf("failed to before first: %v", err)
	}

	ok, err := idx.Next(ctx)
	if err != nil {
		t.Fatalf("failed to next: %v", err)
	}
	if ok {
		t.Error("expected no records for non-existent key")
	}

	if err := idx.Close(ctx); err != nil {
		t.Fatalf("failed to close: %v", err)
	}
}

func TestBTreeIndexDelete(t *testing.T) {
	tx, layout, cleanup := setupBTreeIndex(t)
	defer cleanup()

	ctx := context.Background()
	idx, err := dbindex.NewBTreeIndex(ctx, tx, "testbtreeidx", layout)
	if err != nil {
		t.Fatalf("failed to create btree index: %v", err)
	}

	val := dbconstant.NewIntConstant(42)
	rid1 := *dbrecord.NewRID(0, 0)
	rid2 := *dbrecord.NewRID(0, 1)

	if err := idx.Insert(ctx, val, rid1); err != nil {
		t.Fatalf("failed to insert: %v", err)
	}
	if err := idx.Insert(ctx, val, rid2); err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	if err := idx.Delete(ctx, val, rid1); err != nil {
		t.Fatalf("failed to delete: %v", err)
	}

	if err := idx.BeforeFirst(ctx, val); err != nil {
		t.Fatalf("failed to before first: %v", err)
	}

	count := 0
	for {
		ok, err := idx.Next(ctx)
		if err != nil {
			t.Fatalf("failed to next: %v", err)
		}
		if !ok {
			break
		}
		rid, err := idx.GetDataRID(ctx)
		if err != nil {
			t.Fatalf("failed to get data rid: %v", err)
		}
		if *rid == rid1 {
			t.Error("deleted RID should not be found")
		}
		count++
	}

	if count != 1 {
		t.Errorf("expected 1 record after delete, got %d", count)
	}

	if err := idx.Close(ctx); err != nil {
		t.Fatalf("failed to close: %v", err)
	}
}

func TestBTreeIndexDifferentKeys(t *testing.T) {
	tx, layout, cleanup := setupBTreeIndex(t)
	defer cleanup()

	ctx := context.Background()
	idx, err := dbindex.NewBTreeIndex(ctx, tx, "testbtreeidx", layout)
	if err != nil {
		t.Fatalf("failed to create btree index: %v", err)
	}

	val1 := dbconstant.NewIntConstant(10)
	val2 := dbconstant.NewIntConstant(20)
	val3 := dbconstant.NewIntConstant(30)
	rid1 := *dbrecord.NewRID(0, 0)
	rid2 := *dbrecord.NewRID(0, 1)
	rid3 := *dbrecord.NewRID(1, 0)

	if err := idx.Insert(ctx, val1, rid1); err != nil {
		t.Fatalf("failed to insert: %v", err)
	}
	if err := idx.Insert(ctx, val2, rid2); err != nil {
		t.Fatalf("failed to insert: %v", err)
	}
	if err := idx.Insert(ctx, val3, rid3); err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	// Search for each key and verify
	for _, tc := range []struct {
		val dbconstant.Constant
		rid dbrecord.RID
	}{
		{val1, rid1},
		{val2, rid2},
		{val3, rid3},
	} {
		if err := idx.BeforeFirst(ctx, tc.val); err != nil {
			t.Fatalf("failed to before first for %v: %v", tc.val, err)
		}
		ok, err := idx.Next(ctx)
		if err != nil {
			t.Fatalf("failed to next for %v: %v", tc.val, err)
		}
		if !ok {
			t.Fatalf("expected to find record for %v", tc.val)
		}
		gotRID, err := idx.GetDataRID(ctx)
		if err != nil {
			t.Fatalf("failed to get data rid for %v: %v", tc.val, err)
		}
		if *gotRID != tc.rid {
			t.Errorf("for key %v: expected RID %v, got %v", tc.val, tc.rid, *gotRID)
		}
	}

	if err := idx.Close(ctx); err != nil {
		t.Fatalf("failed to close: %v", err)
	}
}

func TestBTreeIndexManyInserts(t *testing.T) {
	tx, layout, cleanup := setupBTreeIndex(t)
	defer cleanup()

	ctx := context.Background()
	idx, err := dbindex.NewBTreeIndex(ctx, tx, "testbtreeidx", layout)
	if err != nil {
		t.Fatalf("failed to create btree index: %v", err)
	}

	numRecords := 50
	for i := 0; i < numRecords; i++ {
		val := dbconstant.NewIntConstant(i)
		rid := *dbrecord.NewRID(i/10, i%10)
		if err := idx.Insert(ctx, val, rid); err != nil {
			t.Fatalf("failed to insert key %d: %v", i, err)
		}
	}

	// Verify all records can be found
	for i := 0; i < numRecords; i++ {
		val := dbconstant.NewIntConstant(i)
		expectedRID := *dbrecord.NewRID(i/10, i%10)

		if err := idx.BeforeFirst(ctx, val); err != nil {
			t.Fatalf("failed to before first for key %d: %v", i, err)
		}
		ok, err := idx.Next(ctx)
		if err != nil {
			t.Fatalf("failed to next for key %d: %v", i, err)
		}
		if !ok {
			t.Fatalf("expected to find record for key %d", i)
		}
		gotRID, err := idx.GetDataRID(ctx)
		if err != nil {
			t.Fatalf("failed to get data rid for key %d: %v", i, err)
		}
		if *gotRID != expectedRID {
			t.Errorf("key %d: expected RID %v, got %v", i, expectedRID, *gotRID)
		}
	}

	if err := idx.Close(ctx); err != nil {
		t.Fatalf("failed to close: %v", err)
	}
}

func TestBTreeIndexStringKey(t *testing.T) {
	tx, layout, cleanup := setupBTreeStringIndex(t)
	defer cleanup()

	ctx := context.Background()
	idx, err := dbindex.NewBTreeIndex(ctx, tx, "testbtreeidx", layout)
	if err != nil {
		t.Fatalf("failed to create btree index: %v", err)
	}

	keys := []string{"alice", "bob", "charlie"}
	for i, key := range keys {
		val := dbconstant.NewStringConstant(key)
		rid := *dbrecord.NewRID(0, i)
		if err := idx.Insert(ctx, val, rid); err != nil {
			t.Fatalf("failed to insert %q: %v", key, err)
		}
	}

	for i, key := range keys {
		val := dbconstant.NewStringConstant(key)
		expectedRID := *dbrecord.NewRID(0, i)

		if err := idx.BeforeFirst(ctx, val); err != nil {
			t.Fatalf("failed to before first for %q: %v", key, err)
		}
		ok, err := idx.Next(ctx)
		if err != nil {
			t.Fatalf("failed to next for %q: %v", key, err)
		}
		if !ok {
			t.Fatalf("expected to find record for %q", key)
		}
		gotRID, err := idx.GetDataRID(ctx)
		if err != nil {
			t.Fatalf("failed to get data rid for %q: %v", key, err)
		}
		if *gotRID != expectedRID {
			t.Errorf("key %q: expected RID %v, got %v", key, expectedRID, *gotRID)
		}
	}

	if err := idx.Close(ctx); err != nil {
		t.Fatalf("failed to close: %v", err)
	}
}

func TestHashIndexSearchCost(t *testing.T) {
	cost := dbindex.HashIndexSearchCost(1000, 10)
	expected := 1000 / dbindex.HashIndexNumBuckets
	if cost != expected {
		t.Errorf("expected search cost %d, got %d", expected, cost)
	}
}

func TestBTreeIndexSearchCost(t *testing.T) {
	cost := dbindex.BTreeIndexSearchCost(1000, 10)
	if cost <= 0 {
		t.Errorf("expected positive search cost, got %d", cost)
	}
}
