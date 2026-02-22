package dbquery

import (
	"context"
	"fmt"

	"github.com/teru01/simpledb-go/dbconstant"
	"github.com/teru01/simpledb-go/dbindex"
	"github.com/teru01/simpledb-go/dbrecord"
)

type IndexSelectScan struct {
	ts    *dbrecord.TableScan
	index dbindex.Index
	value dbconstant.Constant
}

func NewIndexSelectScan(ctx context.Context, ts *dbrecord.TableScan, index dbindex.Index, value dbconstant.Constant) (*IndexSelectScan, error) {
	iss := &IndexSelectScan{
		ts:    ts,
		index: index,
		value: value,
	}
	if err := iss.SetStateToBeforeFirst(ctx); err != nil {
		return nil, fmt.Errorf("before first: %w", err)
	}
	return iss, nil
}

func (i *IndexSelectScan) SetStateToBeforeFirst(ctx context.Context) error {
	return i.index.BeforeFirst(ctx, i.value)
}

func (i *IndexSelectScan) Next(ctx context.Context) (bool, error) {
	ok, err := i.index.Next(ctx)
	if err != nil {
		return false, fmt.Errorf("next: %w", err)
	}
	if ok {
		rid, err := i.index.GetDataRID(ctx)
		if err != nil {
			return false, fmt.Errorf("get data rid: %w", err)
		}
		if err := i.ts.MoveToRID(ctx, *rid); err != nil {
			return false, fmt.Errorf("move to %q: %w", rid, err)
		}
	}
	return ok, nil
}

func (i *IndexSelectScan) GetInt(ctx context.Context, fieldName string) (int, error) {
	return i.ts.GetInt(ctx, fieldName)
}

func (i *IndexSelectScan) GetString(ctx context.Context, fieldName string) (string, error) {
	return i.ts.GetString(ctx, fieldName)
}

func (i *IndexSelectScan) GetValue(ctx context.Context, fieldName string) (dbconstant.Constant, error) {
	return i.ts.GetValue(ctx, fieldName)
}

func (i *IndexSelectScan) HasField(fieldName string) bool {
	return i.ts.HasField(fieldName)
}

func (i *IndexSelectScan) Close(ctx context.Context) error {
	if err := i.index.Close(ctx); err != nil {
		return fmt.Errorf("close index: %w", err)
	}
	if err := i.ts.Close(ctx); err != nil {
		return fmt.Errorf("close table scan: %w", err)
	}
	return nil
}
