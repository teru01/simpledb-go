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

func setupTestRecordPage(t *testing.T) (*dbtx.Transaction, *dbrecord.Schema, *dbrecord.Layout, dbfile.BlockID, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "recordpage_test")
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

	blk, err := fm.Append("testfile")
	if err != nil {
		t.Fatalf("failed to append block: %v", err)
	}

	cleanup := func() {
		tx.Commit()
		dirFile.Close()
		os.RemoveAll(dir)
	}

	return tx, schema, layout, blk, cleanup
}

func TestRecordPageFormat(t *testing.T) {
	tx, _, layout, blk, cleanup := setupTestRecordPage(t)
	defer cleanup()

	ctx := context.Background()
	if err := tx.Pin(ctx, blk); err != nil {
		t.Fatalf("failed to pin block: %v", err)
	}

	rp := dbrecord.NewRecordPage(tx, blk, layout)
	if err := rp.Format(ctx); err != nil {
		t.Fatalf("failed to format: %v", err)
	}

	// 全スロットがEmptyであることを確認
	for i := 0; i < rp.SlotLengthInBlock(); i++ {
		slot, err := rp.NextInUseSlotAfter(ctx, i-1)
		if err != nil {
			t.Fatalf("failed to get next after: %v", err)
		}
		if slot != -1 && i == 0 {
			t.Errorf("all slots should be empty after format")
		}
	}
}

func TestRecordPageInsertAfter(t *testing.T) {
	tx, _, layout, blk, cleanup := setupTestRecordPage(t)
	defer cleanup()

	ctx := context.Background()
	if err := tx.Pin(ctx, blk); err != nil {
		t.Fatalf("failed to pin block: %v", err)
	}

	rp := dbrecord.NewRecordPage(tx, blk, layout)
	if err := rp.Format(ctx); err != nil {
		t.Fatalf("failed to format: %v", err)
	}

	// 最初の空きスロットを探す
	slot, err := rp.InsertNextAvabilableSlotAfter(ctx, -1)
	if err != nil {
		t.Fatalf("failed to insert after: %v", err)
	}
	if slot != 0 {
		t.Errorf("expected first slot to be 0, got %d", slot)
	}

	// 2つ目の空きスロットを探す
	slot, err = rp.InsertNextAvabilableSlotAfter(ctx, slot)
	if err != nil {
		t.Fatalf("failed to insert after: %v", err)
	}
	if slot != 1 {
		t.Errorf("expected second slot to be 1, got %d", slot)
	}
}

func TestRecordPageSetGetInt(t *testing.T) {
	tx, _, layout, blk, cleanup := setupTestRecordPage(t)
	defer cleanup()

	ctx := context.Background()
	if err := tx.Pin(ctx, blk); err != nil {
		t.Fatalf("failed to pin block: %v", err)
	}

	rp := dbrecord.NewRecordPage(tx, blk, layout)
	if err := rp.Format(ctx); err != nil {
		t.Fatalf("failed to format: %v", err)
	}

	slot, err := rp.InsertNextAvabilableSlotAfter(ctx, -1)
	if err != nil {
		t.Fatalf("failed to insert after: %v", err)
	}

	// Intフィールドに値を設定
	if err := rp.SetInt(ctx, slot, "id", 123); err != nil {
		t.Fatalf("failed to set int: %v", err)
	}

	// 値を取得して確認
	val, err := rp.GetInt(ctx, slot, "id")
	if err != nil {
		t.Fatalf("failed to get int: %v", err)
	}
	if val != 123 {
		t.Errorf("expected 123, got %d", val)
	}
}

func TestRecordPageSetGetString(t *testing.T) {
	tx, _, layout, blk, cleanup := setupTestRecordPage(t)
	defer cleanup()

	ctx := context.Background()
	if err := tx.Pin(ctx, blk); err != nil {
		t.Fatalf("failed to pin block: %v", err)
	}

	rp := dbrecord.NewRecordPage(tx, blk, layout)
	if err := rp.Format(ctx); err != nil {
		t.Fatalf("failed to format: %v", err)
	}

	slot, err := rp.InsertNextAvabilableSlotAfter(ctx, -1)
	if err != nil {
		t.Fatalf("failed to insert after: %v", err)
	}

	// Stringフィールドに値を設定
	if err := rp.SetString(ctx, slot, "name", "Alice"); err != nil {
		t.Fatalf("failed to set string: %v", err)
	}

	// 値を取得して確認
	val, err := rp.GetString(ctx, slot, "name")
	if err != nil {
		t.Fatalf("failed to get string: %v", err)
	}
	if val != "Alice" {
		t.Errorf("expected 'Alice', got '%s'", val)
	}
}

func TestRecordPageDelete(t *testing.T) {
	tx, _, layout, blk, cleanup := setupTestRecordPage(t)
	defer cleanup()

	ctx := context.Background()
	if err := tx.Pin(ctx, blk); err != nil {
		t.Fatalf("failed to pin block: %v", err)
	}

	rp := dbrecord.NewRecordPage(tx, blk, layout)
	if err := rp.Format(ctx); err != nil {
		t.Fatalf("failed to format: %v", err)
	}

	// レコードを挿入
	slot, err := rp.InsertNextAvabilableSlotAfter(ctx, -1)
	if err != nil {
		t.Fatalf("failed to insert after: %v", err)
	}

	if err := rp.SetInt(ctx, slot, "id", 1); err != nil {
		t.Fatalf("failed to set int: %v", err)
	}

	// レコードを削除
	if err := rp.Delete(ctx, slot); err != nil {
		t.Fatalf("failed to delete: %v", err)
	}

	// 削除後は使用中のスロットが見つからないはず
	nextSlot, err := rp.NextInUseSlotAfter(ctx, -1)
	if err != nil {
		t.Fatalf("failed to get next after: %v", err)
	}
	if nextSlot != -1 {
		t.Errorf("expected no used slots after delete, got slot %d", nextSlot)
	}
}

func TestRecordPageNextAfter(t *testing.T) {
	tx, _, layout, blk, cleanup := setupTestRecordPage(t)
	defer cleanup()

	ctx := context.Background()
	if err := tx.Pin(ctx, blk); err != nil {
		t.Fatalf("failed to pin block: %v", err)
	}

	rp := dbrecord.NewRecordPage(tx, blk, layout)
	if err := rp.Format(ctx); err != nil {
		t.Fatalf("failed to format: %v", err)
	}

	// 複数のレコードを挿入
	slot1, err := rp.InsertNextAvabilableSlotAfter(ctx, -1)
	if err != nil {
		t.Fatalf("failed to insert after: %v", err)
	}

	slot2, err := rp.InsertNextAvabilableSlotAfter(ctx, slot1)
	if err != nil {
		t.Fatalf("failed to insert after: %v", err)
	}

	// NextAfterで使用中のスロットを順に取得
	next, err := rp.NextInUseSlotAfter(ctx, -1)
	if err != nil {
		t.Fatalf("failed to get next after: %v", err)
	}
	if next != slot1 {
		t.Errorf("expected slot %d, got %d", slot1, next)
	}

	next, err = rp.NextInUseSlotAfter(ctx, next)
	if err != nil {
		t.Fatalf("failed to get next after: %v", err)
	}
	if next != slot2 {
		t.Errorf("expected slot %d, got %d", slot2, next)
	}

	// これ以上使用中のスロットがないことを確認
	next, err = rp.NextInUseSlotAfter(ctx, next)
	if err != nil {
		t.Fatalf("failed to get next after: %v", err)
	}
	if next != -1 {
		t.Errorf("expected -1 (no more slots), got %d", next)
	}
}

func TestRecordPageMultipleFields(t *testing.T) {
	tx, _, layout, blk, cleanup := setupTestRecordPage(t)
	defer cleanup()

	ctx := context.Background()
	if err := tx.Pin(ctx, blk); err != nil {
		t.Fatalf("failed to pin block: %v", err)
	}

	rp := dbrecord.NewRecordPage(tx, blk, layout)
	if err := rp.Format(ctx); err != nil {
		t.Fatalf("failed to format: %v", err)
	}

	slot, err := rp.InsertNextAvabilableSlotAfter(ctx, -1)
	if err != nil {
		t.Fatalf("failed to insert after: %v", err)
	}

	// 複数フィールドに値を設定
	if err := rp.SetInt(ctx, slot, "id", 42); err != nil {
		t.Fatalf("failed to set id: %v", err)
	}
	if err := rp.SetString(ctx, slot, "name", "Bob"); err != nil {
		t.Fatalf("failed to set name: %v", err)
	}
	if err := rp.SetInt(ctx, slot, "age", 30); err != nil {
		t.Fatalf("failed to set age: %v", err)
	}

	// 値を確認
	id, err := rp.GetInt(ctx, slot, "id")
	if err != nil {
		t.Fatalf("failed to get id: %v", err)
	}
	if id != 42 {
		t.Errorf("expected id=42, got %d", id)
	}

	name, err := rp.GetString(ctx, slot, "name")
	if err != nil {
		t.Fatalf("failed to get name: %v", err)
	}
	if name != "Bob" {
		t.Errorf("expected name='Bob', got '%s'", name)
	}

	age, err := rp.GetInt(ctx, slot, "age")
	if err != nil {
		t.Fatalf("failed to get age: %v", err)
	}
	if age != 30 {
		t.Errorf("expected age=30, got %d", age)
	}
}

func TestRecordPageBlock(t *testing.T) {
	tx, _, layout, blk, cleanup := setupTestRecordPage(t)
	defer cleanup()

	rp := dbrecord.NewRecordPage(tx, blk, layout)

	if !rp.Block().Equals(blk) {
		t.Errorf("expected block %v, got %v", blk, rp.Block())
	}
}
