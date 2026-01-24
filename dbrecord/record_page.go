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

func NewRecordPage(ctx context.Context, tx *dbtx.Transaction, blk dbfile.BlockID, layout *Layout) (*RecordPage, error) {
	if err := tx.Pin(ctx, blk); err != nil {
		return nil, fmt.Errorf("pin block %s: %w", blk, err)
	}
	return &RecordPage{
		tx:     tx,
		blk:    blk,
		layout: layout,
	}, nil
}

func (r *RecordPage) GetInt(ctx context.Context, slot int, fieldName string) (int, error) {
	pos := r.slotOffset(slot) + r.layout.Offset(fieldName)
	value, err := r.tx.GetInt(ctx, r.blk, pos)
	if err != nil {
		return 0, fmt.Errorf("get int value from field %q at slot %d in block %s: %w", fieldName, slot, r.blk, err)
	}
	return value, nil
}

func (r *RecordPage) SetInt(ctx context.Context, slot int, fieldName string, value int) error {
	pos := r.slotOffset(slot) + r.layout.Offset(fieldName)
	if err := r.tx.SetInt(ctx, r.blk, pos, value, true); err != nil {
		return fmt.Errorf("set int value %d to field %q at slot %d in block %s: %w", value, fieldName, slot, r.blk, err)
	}
	return nil
}

func (r *RecordPage) GetString(ctx context.Context, slot int, fieldName string) (string, error) {
	pos := r.slotOffset(slot) + r.layout.Offset(fieldName)
	value, err := r.tx.GetString(ctx, r.blk, pos)
	if err != nil {
		return "", fmt.Errorf("get string value from field %q at slot %d in block %s: %w", fieldName, slot, r.blk, err)
	}
	return value, nil
}

func (r *RecordPage) SetString(ctx context.Context, slot int, fieldName string, value string) error {
	pos := r.slotOffset(slot) + r.layout.Offset(fieldName)
	if err := r.tx.SetString(ctx, r.blk, pos, value, true); err != nil {
		return fmt.Errorf("set string value %q to field %q at slot %d in block %s: %w", value, fieldName, slot, r.blk, err)
	}
	return nil
}

func (r *RecordPage) Delete(ctx context.Context, slot int) error {
	if err := r.SetFlag(ctx, slot, SlotEmpty); err != nil {
		return fmt.Errorf("set empty flag for slot %d in block %s: %w", slot, r.blk, err)
	}
	return nil
}

func (r *RecordPage) SetFlag(ctx context.Context, slot int, status SlotStatus) error {
	pos := r.slotOffset(slot)
	if err := r.tx.SetInt(ctx, r.blk, pos, int(status), true); err != nil {
		return fmt.Errorf("set flag %d to slot %d in block %s: %w", status, slot, r.blk, err)
	}
	return nil
}

// slotより後の使用中Slot numberを返す
func (r *RecordPage) NextInUseSlotAfter(ctx context.Context, slot int) (int, error) {
	slot, err := r.searchAfter(ctx, slot, SlotUsed)
	if err != nil {
		return 0, fmt.Errorf("search next in-use slot after slot %d in block %s: %w", slot, r.blk, err)
	}
	return slot, nil
}

// slotより後の空きslot numberを返し、使用中のマークする
func (r *RecordPage) InsertNextAvabilableSlotAfter(ctx context.Context, slot int) (int, error) {
	slot, err := r.searchAfter(ctx, slot, SlotEmpty)
	if err != nil {
		return 0, fmt.Errorf("search next available slot after slot %d in block %s: %w", slot, r.blk, err)
	}
	if slot < 0 {
		// No available slot found
		return slot, nil
	}
	if err := r.SetFlag(ctx, slot, SlotUsed); err != nil {
		return 0, fmt.Errorf("set used flag for slot %d in block %s: %w", slot, r.blk, err)
	}
	return slot, nil
}

// slotより後ろ(slotを含まない)でstatusを持つslot offsetを返す
func (r *RecordPage) searchAfter(ctx context.Context, slot int, status SlotStatus) (int, error) {
	limit := r.SlotLengthInBlock()
	if limit == 0 {
		return 0, fmt.Errorf("slot length is too large. increase the block size.")
	}
	for i := slot + 1; i < limit; i++ {
		value, err := r.tx.GetInt(ctx, r.blk, r.slotOffset(i))
		if err != nil {
			return 0, fmt.Errorf("get slot status at slot %d in block %s: %w", i, r.blk, err)
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

// ブロック内でslotを確保して中身をクリアする
func (r *RecordPage) Format(ctx context.Context) error {
	for i := 0; i < r.SlotLengthInBlock(); i++ {
		if err := r.tx.SetInt(ctx, r.blk, r.slotOffset(i), int(SlotEmpty), false); err != nil {
			return fmt.Errorf("set empty flag for slot %d in block %s: %w", i, r.blk, err)
		}
		for _, field := range r.layout.Schema().Fields() {
			pos := r.slotOffset(i) + r.layout.Offset(field)
			switch r.layout.Schema().FieldType(field) {
			case FieldTypeInt:
				if err := r.tx.SetInt(ctx, r.blk, pos, 0, false); err != nil {
					return fmt.Errorf("set int value 0 to field %q at slot %d in block %s: %w", field, i, r.blk, err)
				}
			case FieldTypeString:
				if err := r.tx.SetString(ctx, r.blk, pos, "", false); err != nil {
					return fmt.Errorf("set string value to field %q at slot %d in block %s: %w", field, i, r.blk, err)
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
