package dbrecord

import (
	"context"
	"fmt"

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

func BeforeFirst() {

}

// 次の使用中スロットを最後のブロックまで探す
func (t *TableScan) Next(ctx context.Context) (bool, error) {
	slot, err := t.state.recordPage.NextInUseSlotAfter(ctx, t.state.currentSlot)
	if err != nil {
		return false, fmt.Errorf("faield to get nextInUseSlotAfter: %w", err)
	}
	for slot < 0 {
		// 使用中slotが存在しない

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

// blockを追加し新たなstateを取得する
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
