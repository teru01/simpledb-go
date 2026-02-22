package dbindex

import (
	"context"
	"fmt"
	"math"

	"github.com/teru01/simpledb-go/dbconstant"
	"github.com/teru01/simpledb-go/dbfile"
	"github.com/teru01/simpledb-go/dbmetadata"
	"github.com/teru01/simpledb-go/dbrecord"
	"github.com/teru01/simpledb-go/dbtx"
)

type BTreeIndex struct {
	tx                    *dbtx.Transaction
	dirLayout, leafLayout *dbrecord.Layout
	leafTable             string
	leaf                  *BTreeLeaf
	rootBlock             dbfile.BlockID
}

func NewBTreeIndex(ctx context.Context, tx *dbtx.Transaction, idxName string, leafLayout *dbrecord.Layout) (*BTreeIndex, error) {
	leafTable := idxName + "leaf"
	leafTableSize, err := tx.Size(ctx, leafTable)
	if err != nil {
		return nil, fmt.Errorf("get size of leaf table %q: %w", leafTable, err)
	}
	if leafTableSize == 0 {
		rootBlk, err := tx.Append(ctx, leafTable)
		if err != nil {
			return nil, fmt.Errorf("append block to leaf table %q: %w", leafTable, err)
		}
		node, err := NewBTreePage(ctx, tx, &rootBlk, leafLayout)
		if err != nil {
			return nil, fmt.Errorf("new btree page: %w", err)
		}
		if err := node.Format(ctx, rootBlk, -1); err != nil {
			return nil, fmt.Errorf("format: %w", err)
		}
	}
	dirSchema := dbrecord.NewSchema()
	dirSchema.Add(dbmetadata.IndexFieldBlock, leafLayout.Schema())
	dirSchema.Add(dbmetadata.IndexFieldDataValue, leafLayout.Schema())

	dirTable := idxName + "dir"
	dirLayout := dbrecord.NewLayout(dirSchema)
	rootBlock := dbfile.NewBlockID(dirTable, 0)

	dirTableSize, err := tx.Size(ctx, dirTable)
	if err != nil {
		return nil, fmt.Errorf("get size of dir table %q: %w", dirTable, err)
	}
	if dirTableSize == 0 {
		_, err := tx.Append(ctx, dirTable)
		if err != nil {
			return nil, fmt.Errorf("append block to dir table %q: %w", dirTable, err)
		}
		node, err := NewBTreePage(ctx, tx, &rootBlock, dirLayout)
		if err != nil {
			return nil, fmt.Errorf("new btree page: %w", err)
		}
		if err := node.Format(ctx, rootBlock, 0); err != nil {
			return nil, fmt.Errorf("format: %w", err)
		}
		fieldType := dirLayout.Schema().FieldType(dbmetadata.IndexFieldDataValue)
		var minValue dbconstant.Constant
		if fieldType == dbrecord.FieldTypeInt {
			minValue = dbconstant.NewIntConstant(math.MinInt)
		} else {
			minValue = dbconstant.NewStringConstant("")
		}
		if err := node.InsertDir(ctx, 0, minValue, rootBlock.BlockNum()); err != nil {
			return nil, fmt.Errorf("insert dir: %w", err)
		}
		if err := node.Close(); err != nil {
			return nil, fmt.Errorf("close: %w", err)
		}
	}
	return &BTreeIndex{
		tx:         tx,
		dirLayout:  dirLayout,
		leafLayout: leafLayout,
		leafTable:  leafTable,
		leaf:       nil,
		rootBlock:  rootBlock,
	}, nil
}

func (b *BTreeIndex) BeforeFirst(ctx context.Context, searchKey dbconstant.Constant) error {
	if err := b.Close(ctx); err != nil {
		return fmt.Errorf("close: %w", err)
	}
	root, err := NewBTreeDir(ctx, b.tx, b.rootBlock, b.dirLayout)
	if err != nil {
		return fmt.Errorf("new btree dir: %w", err)
	}
	blockNum, err := root.Search(ctx, searchKey)
	if err != nil {
		return fmt.Errorf("search: %w", err)
	}
	if err := root.Close(ctx); err != nil {
		return fmt.Errorf("close: %w", err)
	}
	leafBlock := dbfile.NewBlockID(b.leafTable, blockNum)
	l, err := NewBTreeLeaf(ctx, b.tx, leafBlock, b.leafLayout, searchKey)
	if err != nil {
		return fmt.Errorf("new btree leaf: %w", err)
	}
	b.leaf = l
	return nil
}

func (b *BTreeIndex) Next(ctx context.Context) (bool, error) {
	return b.leaf.Next(ctx)
}

func (b *BTreeIndex) GetDataRID(ctx context.Context) (*dbrecord.RID, error) {
	return b.leaf.GetDataRID(ctx)
}

func (b *BTreeIndex) Close(ctx context.Context) error {
	if b.leaf != nil {
		if err := b.leaf.Close(ctx); err != nil {
			return fmt.Errorf("close leaf: %w", err)
		}
	}
	return nil
}

// インデックスに挿入する
func (b *BTreeIndex) Insert(ctx context.Context, dataValue dbconstant.Constant, dataRID dbrecord.RID) error {
	if err := b.BeforeFirst(ctx, dataValue); err != nil {
		return fmt.Errorf("before first: %w", err)
	}
	entry, err := b.leaf.Insert(ctx, &dataRID)
	if err != nil {
		return fmt.Errorf("insert leaf: %w", err)
	}
	if err := b.leaf.Close(ctx); err != nil {
		return fmt.Errorf("close leaf: %w", err)
	}
	if entry == nil {
		return nil
	}
	rootDir, err := NewBTreeDir(ctx, b.tx, b.rootBlock, b.dirLayout)
	if err != nil {
		return fmt.Errorf("init root dir: %w", err)
	}
	splittedRoot, err := rootDir.Insert(ctx, entry)
	if err != nil {
		return fmt.Errorf("insert dir: %w", err)
	}
	if splittedRoot != nil {
		if err := rootDir.MakeNewRoot(ctx, splittedRoot); err != nil {
			return fmt.Errorf("make new root: %w", err)
		}
	}
	if err := rootDir.Close(ctx); err != nil {
		return fmt.Errorf("close root: %w", err)
	}
	return nil
}

func (b *BTreeIndex) Delete(ctx context.Context, dataValue dbconstant.Constant, dataRID dbrecord.RID) error {
	if err := b.BeforeFirst(ctx, dataValue); err != nil {
		return fmt.Errorf("before first: %w", err)
	}
	if err := b.leaf.Delete(ctx, dataRID); err != nil {
		return fmt.Errorf("delete leaf: %w", err)
	}
	if err := b.leaf.Close(ctx); err != nil {
		return fmt.Errorf("close leaf: %w", err)
	}
	return nil
}

