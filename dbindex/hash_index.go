package dbindex

import (
	"context"
	"fmt"

	"github.com/teru01/simpledb-go/dbconstant"
	"github.com/teru01/simpledb-go/dbname"
	"github.com/teru01/simpledb-go/dbrecord"
	"github.com/teru01/simpledb-go/dbtx"
)

const HashIndexNumBuckets = 100

type HashIndex struct {
	tx        *dbtx.Transaction
	indexName string
	layout    *dbrecord.Layout
	state     hashIndexState
}

type hashIndexState struct {
	searchKey dbconstant.Constant
	ts        *dbrecord.TableScan
}

func NewHashIndex(tx *dbtx.Transaction, indexName string, layout *dbrecord.Layout) *HashIndex {
	return &HashIndex{
		tx:        tx,
		indexName: indexName,
		layout:    layout,
	}
}

func (h *HashIndex) BeforeFirst(ctx context.Context, searchKey dbconstant.Constant) error {
	if err := h.Close(ctx); err != nil {
		return err
	}
	h.state.searchKey = searchKey
	bucket := searchKey.HashCode() % HashIndexNumBuckets
	ts, err := dbrecord.NewTableScan(ctx, h.tx, fmt.Sprintf("%s%d", h.indexName, bucket), h.layout)
	if err != nil {
		return fmt.Errorf("new table scan for index: %w", err)
	}
	h.state.ts = ts
	return nil
}

func (h *HashIndex) Next(ctx context.Context) (bool, error) {
	for {
		ok, err := h.state.ts.Next(ctx)
		if err != nil {
			return false, err
		}
		if !ok {
			break
		}
		v, err := h.state.ts.GetValue(ctx, dbname.IndexFieldDataValue)
		if err != nil {
			return false, err
		}
		if v.Equals(h.state.searchKey) {
			return true, nil
		}
	}
	return false, nil
}

func (h *HashIndex) Close(ctx context.Context) error {
	return h.state.ts.Close(ctx)
}

func (h *HashIndex) GetDataRID(ctx context.Context) (dbrecord.RID, error) {
	blk, err := h.state.ts.GetInt(ctx, dbname.IndexFieldBlock)
	if err != nil {
		return dbrecord.RID{}, fmt.Errorf("get block: %w", err)
	}
	id, err := h.state.ts.GetInt(ctx, dbname.IndexFieldID)
	if err != nil {
		return dbrecord.RID{}, fmt.Errorf("get id: %w", err)
	}
	return *dbrecord.NewRID(blk, id), nil
}

// dataRIDをインデックスに記録する
func (h *HashIndex) Insert(ctx context.Context, val dbconstant.Constant, dataRID dbrecord.RID) error {
	if err := h.BeforeFirst(ctx, val); err != nil {
		return fmt.Errorf("before first while inserting to %q: %w", &dataRID, err)
	}
	if err := h.state.ts.Insert(ctx); err != nil {
		return fmt.Errorf("insert to index: %w", err)
	}
	if err := h.state.ts.SetInt(ctx, dbname.IndexFieldBlock, dataRID.BlockNum()); err != nil {
		return fmt.Errorf("set block: %w", err)
	}
	if err := h.state.ts.SetInt(ctx, dbname.IndexFieldID, dataRID.Slot()); err != nil {
		return fmt.Errorf("set id: %w", err)
	}
	if err := h.state.ts.SetValue(ctx, dbname.IndexFieldDataValue, val); err != nil {
		return fmt.Errorf("set data value: %w", err)
	}
	return nil
}

func (h *HashIndex) Delete(ctx context.Context, val dbconstant.Constant, dataRID dbrecord.RID) error {
	if err := h.BeforeFirst(ctx, val); err != nil {
		return fmt.Errorf("before first while inserting to %q: %w", &dataRID, err)
	}

	for {
		ok, err := h.Next(ctx)
		if err != nil {
			return fmt.Errorf("next while deleting %q: %w", val, err)
		}
		if !ok {
			break
		}
		rid, err := h.GetDataRID(ctx)
		if err != nil {
			return fmt.Errorf("get data rid: %w", err)
		}
		if dataRID == rid {
			if err := h.state.ts.Delete(ctx); err != nil {
				return fmt.Errorf("delete %q: %w", &dataRID, err)
			}
		}
	}
	return nil
}

// バケットにほぼ等分されて、バケット内部はリニアサーチ
func HashIndexSearchCost(numBlocks, rbp int) int {
	return numBlocks / HashIndexNumBuckets
}
