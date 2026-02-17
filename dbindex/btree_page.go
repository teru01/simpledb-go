package dbindex

import (
	"context"
	"fmt"

	"github.com/teru01/simpledb-go/dbconstant"
	"github.com/teru01/simpledb-go/dbfile"
	"github.com/teru01/simpledb-go/dbmetadata"
	"github.com/teru01/simpledb-go/dbrecord"
	"github.com/teru01/simpledb-go/dbsize"
	"github.com/teru01/simpledb-go/dbtx"
)

type BTreePage struct {
	tx           *dbtx.Transaction
	layout       *dbrecord.Layout
	currentBlock *dbfile.BlockID
}

func NewBTreePage(ctx context.Context, tx *dbtx.Transaction, blockID *dbfile.BlockID, layout *dbrecord.Layout) (*BTreePage, error) {
	if err := tx.Pin(ctx, *blockID); err != nil {
		return nil, fmt.Errorf("pin block %s: %w", blockID, err)
	}
	return &BTreePage{
		tx:           tx,
		currentBlock: blockID,
		layout:       layout,
	}, nil
}

// searchKeyが挿入されうる箇所を返す
func (b *BTreePage) FindSlotBefore(ctx context.Context, searchKey dbconstant.Constant) (int, error) {
	slot := 0
	for {
		n, err := b.GetNumRecords(ctx)
		if err != nil {
			return 0, fmt.Errorf("get number of records while finding slot of %q in the index: %w", searchKey, err)
		}
		if slot >= n {
			break
		}
		c, err := b.GetDataValue(ctx, slot)
		if err != nil {
			return 0, fmt.Errorf("get data value in the index: %w", err)
		}
		if c.Compare(searchKey) < 0 {
			slot++
		} else {
			break
		}
	}
	return slot - 1, nil
}

func (b *BTreePage) Close() error {
	if b.currentBlock != nil {
		if err := b.tx.UnPin(*b.currentBlock); err != nil {
			return fmt.Errorf("close: %w", err)
		}
	}
	b.currentBlock = nil
	return nil
}

// これ以上入らないならtrueを返す
// 2つ後のslotの先頭で比較
func (b *BTreePage) IsFull(ctx context.Context) (bool, error) {
	n, err := b.GetNumRecords(ctx)
	if err != nil {
		return false, fmt.Errorf("get number of records: %w", err)
	}
	return b.slotPosition(n+1) >= b.tx.BlockSize(), nil
}

func (b *BTreePage) Split(ctx context.Context, splitPos int, flag int) (dbfile.BlockID, error) {
	newBlk, err := b.AppendNew(ctx, flag)
	if err != nil {
		return dbfile.BlockID{}, fmt.Errorf("append new: %w", err)
	}
	newPage, err := NewBTreePage(ctx, b.tx, &newBlk, b.layout)
	if err != nil {
		return dbfile.BlockID{}, fmt.Errorf("new btree page: %w", err)
	}
	if err := b.transferRecords(ctx, splitPos, newPage); err != nil {
		return dbfile.BlockID{}, fmt.Errorf("transfer records splitpos:%q: %w", splitPos, err)
	}
	if err := newPage.SetFlag(ctx, flag); err != nil {
		return dbfile.BlockID{}, fmt.Errorf("set flag: %w", err)
	}
	if err := newPage.Close(); err != nil {
		return dbfile.BlockID{}, fmt.Errorf("close: %w", err)
	}
	return newBlk, nil
}

func (b *BTreePage) GetFlag(ctx context.Context) (int, error) {
	return b.tx.GetInt(ctx, *b.currentBlock, 0)
}

func (b *BTreePage) SetFlag(ctx context.Context, val int) error {
	return b.tx.SetInt(ctx, *b.currentBlock, 0, val, true)
}

// 新たなブロックを追加する
func (b *BTreePage) AppendNew(ctx context.Context, flag int) (dbfile.BlockID, error) {
	blk, err := b.tx.Append(ctx, b.currentBlock.FileName())
	if err != nil {
		return dbfile.BlockID{}, fmt.Errorf("append: %w", err)
	}
	if err := b.tx.Pin(ctx, blk); err != nil {
		return dbfile.BlockID{}, fmt.Errorf("pin: %w", err)
	}
	if err := b.Format(ctx, blk, flag); err != nil {
		return dbfile.BlockID{}, fmt.Errorf("format: %w", err)
	}
	return blk, nil
}

func (b *BTreePage) Format(ctx context.Context, blk dbfile.BlockID, flag int) error {
	if err := b.tx.SetInt(ctx, blk, 0, flag, false); err != nil {
		return fmt.Errorf("set flag: %w", err)
	}
	// set number of records
	if err := b.tx.SetInt(ctx, blk, dbsize.IntSize, 0, false); err != nil {
		return fmt.Errorf("set number of records: %w", err)
	}
	recordSize := b.layout.SlotSize()
	for i := dbsize.IntSize * 2; i+recordSize <= b.tx.BlockSize(); i += recordSize {
		if err := b.MakeDefaultRecord(ctx, blk, i); err != nil {
			return fmt.Errorf("make default record at position %d: %w", i, err)
		}
	}
	return nil
}

func (b *BTreePage) MakeDefaultRecord(ctx context.Context, blk dbfile.BlockID, position int) error {
	for _, field := range b.layout.Schema().Fields() {
		offset := b.layout.Offset(field)
		switch b.layout.Schema().FieldType(field) {
		case dbrecord.FieldTypeInt:
			if err := b.tx.SetInt(ctx, blk, position+offset, 0, false); err != nil {
				return fmt.Errorf("set int value 0 to field %q at position %d: %w", field, position, err)
			}
		case dbrecord.FieldTypeString:
			if err := b.tx.SetString(ctx, blk, position+offset, "", false); err != nil {
				return fmt.Errorf("set string value to field %q at position %d: %w", field, position, err)
			}
		default:
			return fmt.Errorf("invalid field type %q", field)
		}
	}
	return nil
}

func (b *BTreePage) GetChildNum(ctx context.Context, slot int) (int, error) {
	return b.getInt(ctx, slot, dbmetadata.IndexFieldBlock)
}

func (b *BTreePage) InsertDir(ctx context.Context, slot int, value dbconstant.Constant, blockNum int) error {
	if err := b.insert(ctx, slot); err != nil {
		return fmt.Errorf("insert to slot %q: %w", slot, err)
	}
	if err := b.setValue(ctx, slot, dbmetadata.IndexFieldDataValue, value); err != nil {
		return fmt.Errorf("set value: %w", err)
	}
	if err := b.setInt(ctx, slot, dbmetadata.IndexFieldBlock, blockNum); err != nil {
		return fmt.Errorf("set int: %w", err)
	}
	return nil
}

func (b *BTreePage) GetDataRID(ctx context.Context, slot int) (*dbrecord.RID, error) {
	blk, err := b.getInt(ctx, slot, dbmetadata.IndexFieldBlock)
	if err != nil {
		return nil, fmt.Errorf("get block: %w", err)
	}
	id, err := b.getInt(ctx, slot, dbmetadata.IndexFieldDataValue)
	if err != nil {
		return nil, fmt.Errorf("get data value: %w", err)
	}
	return dbrecord.NewRID(blk, id), nil
}

func (b *BTreePage) InsertLeaf(ctx context.Context, slot int, value dbconstant.Constant, rid *dbrecord.RID) error {
	if err := b.insert(ctx, slot); err != nil {
		return fmt.Errorf("insert for leaf: %w", err)
	}
	if err := b.setValue(ctx, slot, dbmetadata.IndexFieldDataValue, value); err != nil {
		return fmt.Errorf("set value: %w", err)
	}
	if err := b.setInt(ctx, slot, dbmetadata.IndexFieldBlock, rid.BlockNum()); err != nil {
		return fmt.Errorf("set block: %w", err)
	}
	if err := b.setInt(ctx, slot, dbmetadata.IndexFieldID, rid.Slot()); err != nil {
		return fmt.Errorf("set id: %w", err)
	}
	return nil
}

func (b *BTreePage) Delete(ctx context.Context, slot int) error {
	n, err := b.GetNumRecords(ctx)
	if err != nil {
		return fmt.Errorf("get num records while deleting %q: %w", slot, err)
	}
	for i := slot; i+1 < n; i++ {
		if err := b.copyRecord(ctx, i+1, i); err != nil {
			return fmt.Errorf("copy record from %q to %q: %w", i+1, i, err)
		}
	}
	if err := b.SetNumRecords(ctx, n-1); err != nil {
		return fmt.Errorf("set num records while deleting: %w", err)
	}
	return nil
}

func (b *BTreePage) GetDataValue(ctx context.Context, slot int) (dbconstant.Constant, error) {
	return b.getValue(ctx, slot, dbmetadata.IndexFieldDataValue)
}

// 現在見ているブロックにあるレコード数を返す
func (b *BTreePage) GetNumRecords(ctx context.Context) (int, error) {
	return b.tx.GetInt(ctx, *b.currentBlock, dbsize.IntSize)
}

func (b *BTreePage) SetNumRecords(ctx context.Context, num int) error {
	return b.tx.SetInt(ctx, *b.currentBlock, dbsize.IntSize, num, true)
}

func (b *BTreePage) getValue(ctx context.Context, slot int, fieldName string) (dbconstant.Constant, error) {
	switch b.layout.Schema().FieldType(fieldName) {
	case dbrecord.FieldTypeInt:
		i, err := b.getInt(ctx, slot, fieldName)
		if err != nil {
			return nil, fmt.Errorf("get int for slot %q field %q: %w", slot, fieldName, err)
		}
		return dbconstant.NewIntConstant(i), nil
	case dbrecord.FieldTypeString:
		s, err := b.getString(ctx, slot, fieldName)
		if err != nil {
			return nil, fmt.Errorf("get string for slot %q field %q: %w", slot, fieldName, err)
		}
		return dbconstant.NewStringConstant(s), nil
	default:
		return nil, fmt.Errorf("invalid field type %q", fieldName)
	}
}

func (b *BTreePage) getInt(ctx context.Context, slot int, fieldName string) (int, error) {
	return b.tx.GetInt(ctx, *b.currentBlock, b.fieldPosition(slot, fieldName))
}

func (b *BTreePage) getString(ctx context.Context, slot int, fieldName string) (string, error) {
	return b.tx.GetString(ctx, *b.currentBlock, b.fieldPosition(slot, fieldName))
}

func (b *BTreePage) setInt(ctx context.Context, slot int, fieldName string, value int) error {
	return b.tx.SetInt(ctx, *b.currentBlock, b.fieldPosition(slot, fieldName), value, true)
}

func (b *BTreePage) setString(ctx context.Context, slot int, fieldName string, value string) error {
	return b.tx.SetString(ctx, *b.currentBlock, b.fieldPosition(slot, fieldName), value, true)
}

func (b *BTreePage) setValue(ctx context.Context, slot int, fieldName string, value dbconstant.Constant) error {
	switch b.layout.Schema().FieldType(fieldName) {
	case dbrecord.FieldTypeInt:
		b.setInt(ctx, b.fieldPosition(slot, fieldName), fieldName, value.AsRaw().(int))
	case dbrecord.FieldTypeString:
		b.setString(ctx, b.fieldPosition(slot, fieldName), fieldName, value.AsRaw().(string))
	default:
		return fmt.Errorf("invalid field type %q", fieldName)
	}
	return nil
}

func (b *BTreePage) fieldPosition(slot int, fieldName string) int {
	return b.slotPosition(slot) + b.layout.Offset(fieldName)
}

// slotのoffsetを返す
func (b *BTreePage) slotPosition(slot int) int {
	return dbsize.IntSize + dbsize.IntSize + b.layout.SlotSize()*slot
}

func (b *BTreePage) insert(ctx context.Context, slot int) error {
	currentSize, err := b.GetNumRecords(ctx)
	if err != nil {
		return fmt.Errorf("get number of records in %q: %w", slot, err)
	}
	for i := currentSize; i > slot; i-- {
		if err := b.copyRecord(ctx, i-1, i); err != nil {
			return fmt.Errorf("copy record from %q to %q: %w", i, i+1, err)
		}
	}
	if err := b.SetNumRecords(ctx, currentSize+1); err != nil {
		return fmt.Errorf("set number of records while insert: %w", err)
	}
	return nil
}

func (b *BTreePage) copyRecord(ctx context.Context, fromSlot int, toSlot int) error {
	schema := b.layout.Schema()
	for _, field := range schema.Fields() {
		v, err := b.getValue(ctx, fromSlot, field)
		if err != nil {
			return fmt.Errorf("get value from %q: %w", fromSlot, err)
		}
		if err := b.setValue(ctx, toSlot, field, v); err != nil {
			return fmt.Errorf("set value to %q: %w", toSlot, err)
		}
	}
	return nil
}

// slot以降のスロットをdestに移動する
func (b *BTreePage) transferRecords(ctx context.Context, slot int, dest *BTreePage) error {
	destSlot := 0
	n, err := b.GetNumRecords(ctx)
	if err != nil {
		return fmt.Errorf("get number of recs: %w", err)
	}
	for i := slot; i < n; i++ {
		if err := dest.insert(ctx, destSlot); err != nil {
			return fmt.Errorf("insert to dest: %w", err)
		}
		sch := b.layout.Schema()
		for _, fieldName := range sch.Fields() {
			val, err := b.getValue(ctx, slot, fieldName)
			if err != nil {
				return fmt.Errorf("get value from %q: %w", slot, err)
			}
			if err := dest.setValue(ctx, destSlot, fieldName, val); err != nil {
				return fmt.Errorf("set value to %q: %w", destSlot, err)
			}
		}
		destSlot++
	}
	if err := b.SetNumRecords(ctx, slot); err != nil {
		return fmt.Errorf("set number of records: %w", err)
	}
	return nil
}
