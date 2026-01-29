package dbrecord

import (
	"context"
	"fmt"

	"github.com/teru01/simpledb-go/dbconstant"
	"github.com/teru01/simpledb-go/dbfile"
	"github.com/teru01/simpledb-go/dbtx"
)

type TableScan struct {
	tx        *dbtx.Transaction
	layout    *Layout
	fileName  string
	tableName string
	state     *TableScanState
}

type TableScanState struct {
	recordPage  *RecordPage
	currentSlot int
}

func TableFileName(tableName string) string {
	return fmt.Sprintf("%s.tbl", tableName)
}

func NewTableScan(ctx context.Context, tx *dbtx.Transaction, tableName string, layout *Layout) (*TableScan, error) {
	var (
		state *TableScanState
		err   error
	)
	fileName := TableFileName(tableName)
	t := &TableScan{
		tx:        tx,
		layout:    layout,
		tableName: tableName,
		fileName:  fileName,
	}
	size, err := tx.Size(ctx, fileName)
	if err != nil {
		return nil, fmt.Errorf("get table size for %q: %w", fileName, err)
	}
	if size == 0 {
		state, err = t.stateForNewBlock(ctx)
		if err != nil {
			return nil, fmt.Errorf("move to new block for table %q: %w", t.fileName, err)
		}
	} else {
		state, err = t.stateForBlock(ctx, 0)
		if err != nil {
			return nil, fmt.Errorf("move to block 0 for table %q: %w", t.fileName, err)
		}
	}
	t.state = state
	return t, nil
}

func (t *TableScan) Close() error {
	if t.state != nil && t.state.recordPage != nil {
		if err := t.tx.UnPin(t.state.recordPage.Block()); err != nil {
			return fmt.Errorf("unpin block %s: %w", t.state.recordPage.Block(), err)
		}
	}
	return nil
}

func (t *TableScan) SetStateToBeforeFirst(ctx context.Context) error {
	state, err := t.stateForBlock(ctx, 0)
	if err != nil {
		return fmt.Errorf("move to block 0 for table %q: %w", t.fileName, err)
	}
	t.state = state
	return nil
}

func (t *TableScan) GetInt(ctx context.Context, fieldName string) (int, error) {
	i, err := t.state.recordPage.GetInt(ctx, t.state.currentSlot, fieldName)
	if err != nil {
		return 0, fmt.Errorf("get int value from field %q at slot %d: %w", fieldName, t.state.currentSlot, err)
	}
	return i, nil
}

func (t *TableScan) GetString(ctx context.Context, fieldName string) (string, error) {
	s, err := t.state.recordPage.GetString(ctx, t.state.currentSlot, fieldName)
	if err != nil {
		return "", fmt.Errorf("get string value from field %q at slot %d: %w", fieldName, t.state.currentSlot, err)
	}
	return s, nil
}

func (t *TableScan) GetValue(ctx context.Context, fieldName string) (dbconstant.Constant, error) {
	switch t.layout.Schema().FieldType(fieldName) {
	case FieldTypeInt:
		i, err := t.GetInt(ctx, fieldName)
		if err != nil {
			return nil, err
		}
		return dbconstant.NewIntConstant(i), err
	case FieldTypeString:
		s, err := t.GetString(ctx, fieldName)
		if err != nil {
			return nil, err
		}
		return dbconstant.NewStringConstant(s), err
	}
	return nil, fmt.Errorf("unknown field type %d for field %q", t.layout.Schema().FieldType(fieldName), fieldName)
}

func (t *TableScan) HasField(fieldName string) bool {
	return t.layout.schema.HasField(fieldName)
}

func (t *TableScan) SetInt(ctx context.Context, fieldName string, value int) error {
	if err := t.state.recordPage.SetInt(ctx, t.state.currentSlot, fieldName, value); err != nil {
		return fmt.Errorf("set int value %d to field %q at slot %d: %w", value, fieldName, t.state.currentSlot, err)
	}
	return nil
}

// `rID`に移動する
func (t *TableScan) MoveToRID(ctx context.Context, rID RID) error {
	if err := t.Close(); err != nil {
		return fmt.Errorf("close current block before moving to RID %v: %w", rID, err)
	}
	blk := dbfile.NewBlockID(t.fileName, rID.BlockNum())
	rp, err := NewRecordPage(ctx, t.tx, blk, t.layout)
	if err != nil {
		return fmt.Errorf("create record page for block %s: %w", blk, err)
	}
	t.state.currentSlot = rID.Slot()
	t.state.recordPage = rp
	return nil
}

func (t *TableScan) RID() *RID {
	return NewRID(t.state.recordPage.Block().BlockNum(), t.state.currentSlot)
}

func (t *TableScan) SetString(ctx context.Context, fieldName string, value string) error {
	if err := t.state.recordPage.SetString(ctx, t.state.currentSlot, fieldName, value); err != nil {
		return fmt.Errorf("set string value %q to field %q at slot %d: %w", value, fieldName, t.state.currentSlot, err)
	}
	return nil
}

func (t *TableScan) SetValue(ctx context.Context, fieldName string, value dbconstant.Constant) error {
	switch t.layout.Schema().FieldType(fieldName) {
	case FieldTypeInt:
		val, ok := value.AsRaw().(int)
		if !ok {
			return fmt.Errorf("value type mismatch for field %q: expected int, got %T", fieldName, value.AsRaw())
		}
		return t.SetInt(ctx, fieldName, val)
	case FieldTypeString:
		val, ok := value.AsRaw().(string)
		if !ok {
			return fmt.Errorf("value type mismatch for field %q: expected string, got %T", fieldName, value.AsRaw())
		}
		return t.SetString(ctx, fieldName, val)
	}
	return fmt.Errorf("unknown field type %d for field %q", t.layout.Schema().FieldType(fieldName), fieldName)
}

// block中の利用可能slotまで移動する
func (t *TableScan) moveToNextAvailableSlotInBlock(ctx context.Context) error {
	slot, err := t.state.recordPage.InsertNextAvabilableSlotAfter(ctx, t.state.currentSlot)
	if err != nil {
		return fmt.Errorf("insert next available slot after slot %d in block %s: %w", t.state.currentSlot, t.state.recordPage.Block(), err)
	}
	t.state.currentSlot = slot
	return nil
}

// 利用可能なSlotをファイル全体から探しstateに反映する. 無ければ作る
func (t *TableScan) Insert(ctx context.Context) error {
	if err := t.moveToNextAvailableSlotInBlock(ctx); err != nil {
		return fmt.Errorf("move to next available slot in block: %w", err)
	}
	for t.state.currentSlot < 0 {
		isLast, err := t.atLastBlock(ctx)
		if err != nil {
			return fmt.Errorf("check if at last block for table %q: %w", t.fileName, err)
		}
		if isLast {
			state, err := t.stateForNewBlock(ctx)
			if err != nil {
				return fmt.Errorf("move to new block for table %q: %w", t.fileName, err)
			}
			t.state = state
		} else {
			state, err := t.stateForBlock(ctx, t.state.recordPage.blk.BlockNum()+1)
			if err != nil {
				return fmt.Errorf("move to block %d for table %q: %w", t.state.recordPage.blk.BlockNum()+1, t.fileName, err)
			}
			t.state = state
		}
		if err := t.moveToNextAvailableSlotInBlock(ctx); err != nil {
			return fmt.Errorf("move to next available slot in block: %w", err)
		}
	}
	return nil
}

// 現在のslotを削除
func (t *TableScan) Delete(ctx context.Context) error {
	if err := t.state.recordPage.Delete(ctx, t.state.currentSlot); err != nil {
		return err
	}
	return nil
}

// 次の使用中スロットを最後のブロックまで探してstateにセットする
func (t *TableScan) Next(ctx context.Context) (bool, error) {
	slot, err := t.state.recordPage.NextInUseSlotAfter(ctx, t.state.currentSlot)
	if err != nil {
		return false, fmt.Errorf("get next in-use slot after slot %d in block %s: %w", t.state.currentSlot, t.state.recordPage.Block(), err)
	}
	for slot < 0 {
		// 使用中slotが存在しないなら次のブロック
		isLast, err := t.atLastBlock(ctx)
		if err != nil {
			return false, fmt.Errorf("check if at last block for table %q: %w", t.fileName, err)
		}
		if isLast {
			return false, nil
		}
		state, err := t.stateForBlock(ctx, t.state.recordPage.Block().BlockNum()+1)
		if err != nil {
			return false, fmt.Errorf("move to block %d for table %q: %w", t.state.recordPage.Block().BlockNum()+1, t.fileName, err)
		}
		t.state = state
		slot, err = t.state.recordPage.NextInUseSlotAfter(ctx, t.state.currentSlot)
		if err != nil {
			return false, fmt.Errorf("get next in-use slot after slot %d in block %s: %w", t.state.currentSlot, t.state.recordPage.Block(), err)
		}
	}
	t.state.currentSlot = slot
	return true, nil
}

// blkNumに移動し、新たなstateを返す
func (t *TableScan) stateForBlock(ctx context.Context, blkNum int) (*TableScanState, error) {
	if err := t.Close(); err != nil {
		return nil, fmt.Errorf("close current block before moving to block %d: %w", blkNum, err)
	}
	blk := dbfile.NewBlockID(t.fileName, blkNum)
	rp, err := NewRecordPage(ctx, t.tx, blk, t.layout)
	if err != nil {
		return nil, fmt.Errorf("create record page for block %s: %w", blk, err)
	}
	return &TableScanState{
		recordPage:  rp,
		currentSlot: -1,
	}, nil
}

// blockを追加し、新たなstateを返す
func (t *TableScan) stateForNewBlock(ctx context.Context) (*TableScanState, error) {
	if err := t.Close(); err != nil {
		return nil, fmt.Errorf("close current block before appending new block: %w", err)
	}
	blk, err := t.tx.Append(ctx, t.fileName)
	if err != nil {
		return nil, fmt.Errorf("append new block to table %q: %w", t.fileName, err)
	}
	rp, err := NewRecordPage(ctx, t.tx, blk, t.layout)
	if err != nil {
		return nil, fmt.Errorf("create record page for block %s: %w", blk, err)
	}
	if err := rp.Format(ctx); err != nil {
		return nil, fmt.Errorf("format new block %s: %w", blk, err)
	}
	return &TableScanState{
		recordPage:  rp,
		currentSlot: -1,
	}, nil
}

// ファイル最後のブロックに存在していればtrue
func (t *TableScan) atLastBlock(ctx context.Context) (bool, error) {
	size, err := t.tx.Size(ctx, t.fileName)
	if err != nil {
		return false, fmt.Errorf("get table size for %q: %w", t.fileName, err)
	}
	return t.state.recordPage.Block().BlockNum() == size-1, nil
}

func (t *TableScan) TableName() string {
	return t.tableName
}
