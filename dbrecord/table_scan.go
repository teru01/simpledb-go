package dbrecord

import (
	"context"
	"fmt"

	"github.com/teru01/simpledb-go/dbconstant"
	"github.com/teru01/simpledb-go/dbfile"
	"github.com/teru01/simpledb-go/dbtx"
)

type TableScan struct {
	tx       *dbtx.Transaction
	layout   *Layout
	fileName string
	state    *TableScanState
}

type TableScanState struct {
	recordPage  *RecordPage
	currentSlot int
}

func NewTableScan(ctx context.Context, tx *dbtx.Transaction, tableName string, layout *Layout) (*TableScan, error) {
	var (
		state *TableScanState
		err   error
	)
	fileName := fmt.Sprintf("%s.tbl", tableName)
	t := &TableScan{
		tx:       tx,
		layout:   layout,
		fileName: fileName,
	}
	size, err := tx.Size(ctx, fileName)
	if err != nil {
		return nil, fmt.Errorf("failed to get size: %w", err)
	}
	if size == 0 {
		state, err = t.moveToNewBlock(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to move to new block: %w", err)
		}
	} else {
		state, err = t.moveToBlock(0)
		if err != nil {
			return nil, fmt.Errorf("failed to move to block 0: %w", err)
		}
	}
	t.state = state
	return t, nil
}

func (t *TableScan) close() error {
	if t.state.recordPage != nil {
		if err := t.tx.UnPin(t.state.recordPage.Block()); err != nil {
			return fmt.Errorf("faield to unpin: %w", err)
		}
	}
	return nil
}

func (t *TableScan) SetStateToBeforeFirst() error {
	state, err := t.moveToBlock(0)
	if err != nil {
		return fmt.Errorf("faied to move blk 0: %w", err)
	}
	t.state = state
	return nil
}

func (t *TableScan) GetInt(ctx context.Context, fieldName string) (int, error) {
	i, err := t.state.recordPage.GetInt(ctx, t.state.currentSlot, fieldName)
	if err != nil {
		return 0, fmt.Errorf("GetInt failed: %w", err)
	}
	return i, nil
}

func (t *TableScan) GetString(ctx context.Context, fieldName string) (string, error) {
	s, err := t.state.recordPage.GetString(ctx, t.state.currentSlot, fieldName)
	if err != nil {
		return "", fmt.Errorf("recordPage GetString failed: %w", err)
	}
	return s, nil
}

func (t *TableScan) GetVal(ctx context.Context, fieldName string) (dbconstant.Constant, error) {
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
	return nil, fmt.Errorf("unknown field type: %d", t.layout.Schema().FieldType(fieldName))
}

func (t *TableScan) HasField(fieldName string) bool {
	return t.layout.schema.HasField(fieldName)
}

func (t *TableScan) SetInt(ctx context.Context, fieldName string, value int) error {
	if err := t.state.recordPage.SetInt(ctx, t.state.currentSlot, fieldName, value); err != nil {
		return fmt.Errorf("recordPage SetInt failed: %w", err)
	}
	return nil
}

func (t *TableScan) SetString(ctx context.Context, fieldName string, value string) error {
	if err := t.state.recordPage.SetString(ctx, t.state.currentSlot, fieldName, value); err != nil {
		return fmt.Errorf("recordPage SetString failed: %w", err)
	}
	return nil
}

func (t *TableScan) SetValue(ctx context.Context, fieldName string, value dbconstant.Constant) error {
	switch t.layout.Schema().FieldType(fieldName) {
	case FieldTypeInt:
		val, ok := value.AsRaw().(int)
		if !ok {
			return fmt.Errorf("value type mismatch: %v", value)
		}
		return t.SetInt(ctx, fieldName, val)
	case FieldTypeString:
		val, ok := value.AsRaw().(string)
		if !ok {
			return fmt.Errorf("value type mismatch: %v", value)
		}
		return t.SetString(ctx, fieldName, val)
	}
	return fmt.Errorf("unknown field type: %d", t.layout.Schema().FieldType(fieldName))
}

func (t *TableScan) moveToNextAvailableSlotInBlock(ctx context.Context) error {
	slot, err := t.state.recordPage.InsertNextAvabilableSlotAfter(ctx, t.state.currentSlot)
	if err != nil {
		return fmt.Errorf("failed to insert next available slot after: %w", err)
	}
	t.state.currentSlot = slot
	return nil
}

// 利用可能なSlotをファイル全体から探しstateに反映する. 無ければ作る
func (t *TableScan) Insert(ctx context.Context) error {
	if err := t.moveToNextAvailableSlotInBlock(ctx); err != nil {
		return fmt.Errorf("moveToNextAvailableSlotInBlock failed: %w", err)
	}
	for t.state.currentSlot < 0 {
		isLast, err := t.atLastBlock(ctx)
		if err != nil {
			return fmt.Errorf("failed to get last block: %w", err)
		}
		if isLast {
			state, err := t.moveToNewBlock(ctx)
			if err != nil {
				return fmt.Errorf("failed to move to new block: %w", err)
			}
			t.state = state
		} else {
			state, err := t.moveToBlock(t.state.recordPage.blk.BlockNum() + 1)
			if err != nil {
				return fmt.Errorf("failed to move to %d: %w", t.state.recordPage.blk.BlockNum()+1, err)
			}
			t.state = state
		}
		if err := t.moveToNextAvailableSlotInBlock(ctx); err != nil {
			return fmt.Errorf("moveToNextAvailableSlotInBlock failed: %w", err)
		}
	}
	return nil
}

func (t *TableScan) Delete(ctx context.Context) error {
	if err := t.state.recordPage.Delete(ctx, t.state.currentSlot); err != nil {
		return err
	}
	return nil
}

// 次の使用中スロットを最後のブロックまで探す
func (t *TableScan) Next(ctx context.Context) (bool, error) {
	slot, err := t.state.recordPage.NextInUseSlotAfter(ctx, t.state.currentSlot)
	if err != nil {
		return false, fmt.Errorf("faield to get nextInUseSlotAfter: %w", err)
	}
	for slot < 0 {
		// 使用中slotが存在しないなら次のブロック
		isLast, err := t.atLastBlock(ctx)
		if err != nil {
			return false, fmt.Errorf("failed to check at last block: %w", err)
		}
		if isLast {
			return false, nil
		}
		state, err := t.moveToBlock(t.state.recordPage.Block().BlockNum() + 1)
		if err != nil {
			return false, fmt.Errorf("faield to moveToBlock: %w", err)
		}
		t.state = state
		slot, err = t.state.recordPage.NextInUseSlotAfter(ctx, t.state.currentSlot)
		if err != nil {
			return false, fmt.Errorf("faield to get nextInUseSlotAfter: %w", err)
		}
		t.state.currentSlot = slot
	}
	return true, nil
}

// blkNumに移動し、新たなstateを返す
func (t *TableScan) moveToBlock(blkNum int) (*TableScanState, error) {
	if err := t.close(); err != nil {
		return nil, fmt.Errorf("failed to close: %w", err)
	}
	rp := NewRecordPage(t.tx, dbfile.NewBlockID(t.fileName, blkNum), t.layout)
	return &TableScanState{
		recordPage:  rp,
		currentSlot: -1,
	}, nil
}

// blockを追加し、新たなstateを返す
func (t *TableScan) moveToNewBlock(ctx context.Context) (*TableScanState, error) {
	if err := t.close(); err != nil {
		return nil, fmt.Errorf("failed to close: %w", err)
	}
	blk, err := t.tx.Append(ctx, t.fileName)
	if err != nil {
		return nil, fmt.Errorf("failed to append: %w", err)
	}
	rp := NewRecordPage(t.tx, blk, t.layout)
	if err := rp.Format(ctx); err != nil {
		return nil, fmt.Errorf("failed to format: %w", err)
	}
	return &TableScanState{
		recordPage:  rp,
		currentSlot: -1,
	}, nil
}

func (t *TableScan) atLastBlock(ctx context.Context) (bool, error) {
	size, err := t.tx.Size(ctx, t.fileName)
	if err != nil {
		return false, fmt.Errorf("failed to get size: %w", err)
	}
	return t.state.recordPage.Block().BlockNum() == size-1, nil
}
