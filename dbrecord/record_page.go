package dbrecord

import (
	"context"
	"fmt"

	"github.com/teru01/simpledb-go/dbfile"
	"github.com/teru01/simpledb-go/dbtx"
)

type SlotStatus int

const (
	SlotEmpty SlotStatus = iota
	SlotUsed
)

// layoutを使ってtxを実行する
type RecordPage struct {
	tx     *dbtx.Transaction
	blk    dbfile.BlockID
	layout *Layout
}

func NewRecordPage(tx *dbtx.Transaction, blk dbfile.BlockID, layout *Layout) *RecordPage {
	return &RecordPage{
		tx:     tx,
		blk:    blk,
		layout: layout,
	}
}

func (r *RecordPage) GetInt(ctx context.Context, slot int, fieldName string) (int, error) {
	pos := r.slotOffset(slot) + r.layout.Offset(fieldName)
	value, err := r.tx.GetInt(ctx, r.blk, pos)
	if err != nil {
		return 0, fmt.Errorf("GetInt failed: %w", err)
	}
	return value, nil
}

func (r *RecordPage) SetInt(ctx context.Context, slot int, fieldName string, value int) error {
	pos := r.slotOffset(slot) + r.layout.Offset(fieldName)
	if err := r.tx.SetInt(ctx, r.blk, pos, value, true); err != nil {
		return fmt.Errorf("SetInt failed: %w", err)
	}
	return nil
}

func (r *RecordPage) GetString(ctx context.Context, slot int, fieldName string) (string, error) {
	pos := r.slotOffset(slot) + r.layout.Offset(fieldName)
	value, err := r.tx.GetString(ctx, r.blk, pos)
	if err != nil {
		return "", fmt.Errorf("GetString failed: %w", err)
	}
	return value, nil
}

func (r *RecordPage) SetString(ctx context.Context, slot int, fieldName string, value string) error {
	pos := r.slotOffset(slot) + r.layout.Offset(fieldName)
	if err := r.tx.SetString(ctx, r.blk, pos, value, true); err != nil {
		return fmt.Errorf("SetString failed: %w", err)
	}
	return nil
}

func (r *RecordPage) Delete(ctx context.Context, slot int) error {
	if err := r.SetFlag(ctx, slot, SlotEmpty); err != nil {
		return fmt.Errorf("failed to set flag: %w", err)
	}
	return nil
}

func (r *RecordPage) SetFlag(ctx context.Context, slot int, status SlotStatus) error {
	pos := r.slotOffset(slot)
	if err := r.tx.SetInt(ctx, r.blk, pos, int(status), true); err != nil {
		return fmt.Errorf("SetFlag failed: %w", err)
	}
	return nil
}

func (r *RecordPage) NextAfter(ctx context.Context, slot int) (int, error) {
	slot, err := r.searchAfter(ctx, slot, SlotUsed)
	if err != nil {
		return 0, fmt.Errorf("NextAfter failed: %w", err)
	}
	return slot, nil
}

func (r *RecordPage) InsertAfter(ctx context.Context, slot int) (int, error) {
	slot, err := r.searchAfter(ctx, slot, SlotEmpty)
	if err != nil {
		return 0, fmt.Errorf("InsertAfter failed: %w", err)
	}
	if err := r.SetFlag(ctx, slot, SlotUsed); err != nil {
		return 0, fmt.Errorf("failed to set used flag: %w", err)
	}
	return slot, nil
}

// slotより後ろ(slotを含まない)でstatusを持つslot offsetを返す
func (r *RecordPage) searchAfter(ctx context.Context, slot int, status SlotStatus) (int, error) {
	limit := r.SlotLengthInBlock()
	for i := slot + 1; i < limit; i++ {
		value, err := r.tx.GetInt(ctx, r.blk, r.slotOffset(i))
		if err != nil {
			return 0, fmt.Errorf("GetInt failed: %w", err)
		}
		if value == int(status) {
			return i, nil
		}
	}
	// not found
	return -1, nil
}

func (r *RecordPage) SlotLengthInBlock() int {
	return r.tx.BlockSize() / r.layout.slotSize
}

func (r *RecordPage) Format(ctx context.Context) error {
	for i := 0; i < r.SlotLengthInBlock(); i++ {
		if err := r.tx.SetInt(ctx, r.blk, r.slotOffset(i), int(SlotEmpty), false); err != nil {
			return fmt.Errorf("failed to set flag: %w", err)
		}
		for _, field := range r.layout.Schema().Fields() {
			pos := r.slotOffset(i) + r.layout.Offset(field)
			switch r.layout.Schema().FieldType(field) {
			case FieldTypeInt:
				if err := r.tx.SetInt(ctx, r.blk, pos, 0, false); err != nil {
					return fmt.Errorf("faield to set int to 0: %w", err)
				}
			case FieldTypeString:
				if err := r.tx.SetString(ctx, r.blk, pos, "", false); err != nil {
					return fmt.Errorf("failed to set string: %w", err)
				}
			}
		}
	}
	return nil
}

func (r *RecordPage) slotOffset(slot int) int {
	return slot * r.layout.slotSize
}

func (r *RecordPage) Block() dbfile.BlockID {
	return r.blk
}
