package dbrecord

import (
	"context"
	"fmt"

	"github.com/teru01/simpledb-go/dbfile"
	"github.com/teru01/simpledb-go/dbtx"
)

const ()

// layoutを使ってtxを実行する
type RecordPage struct {
	layout *Layout
	tx     *dbtx.Transaction
	blk    dbfile.BlockID
}

func NewRecordPage(tx *dbtx.Transaction, blk dbfile.BlockID, layout Layout) *RecordPage {
	return nil
}

// func Block() dbfile.BlockID {
// 	return
// }

func (r *RecordPage) SetInt(ctx context.Context, slot int, fieldName string, value int) error {
	pos := r.offset(slot) + r.layout.Offset(fieldName)
	if err := r.tx.SetInt(ctx, r.blk, pos, value, true); err != nil {
		return fmt.Errorf("failed to SetInt: %w", err)
	}
	return nil
}

func (r *RecordPage) offset(slot int) int {
	return slot * r.layout.slotSize
}
