package dbindex

import (
	"context"
	"fmt"

	"github.com/teru01/simpledb-go/dbconstant"
	"github.com/teru01/simpledb-go/dbfile"
	"github.com/teru01/simpledb-go/dbrecord"
	"github.com/teru01/simpledb-go/dbtx"
)

type BTreeDir struct {
	tx       *dbtx.Transaction
	layout   *dbrecord.Layout
	contents *BTreePage
	fileName string
}

func NewBTreeDir(ctx context.Context, tx *dbtx.Transaction, blk dbfile.BlockID, layout *dbrecord.Layout) (*BTreeDir, error) {
	contents, err := NewBTreePage(ctx, tx, &blk, layout)
	if err != nil {
		return nil, fmt.Errorf("new btree page: %w", err)
	}
	return &BTreeDir{
		tx:       tx,
		layout:   layout,
		contents: contents,
		fileName: blk.FileName(),
	}, nil
}

func (b *BTreeDir) Close(ctx context.Context) error {
	return b.contents.Close(ctx)
}

// 再起的に検索してleafのblock numberを返す
func (b *BTreeDir) Search(ctx context.Context, searchKey dbconstant.Constant) (int, error) {
	blk, err := b.findChildBlock(ctx, searchKey)
	if err != nil {
		return 0, fmt.Errorf("find child block for %q: %w", searchKey, err)
	}
	flag, err := b.contents.GetFlag(ctx)
	if err != nil {
		return 0, fmt.Errorf("get flag: %w", err)
	}
	for flag > 0 {
		if err := b.Close(ctx); err != nil {
			return 0, fmt.Errorf("close: %w", err)
		}
		b.contents, err = NewBTreePage(ctx, b.tx, &blk, b.layout)
		if err != nil {
			return 0, fmt.Errorf("initialize btree: %w", err)
		}
		flag, err = b.contents.GetFlag(ctx)
		if err != nil {
			return 0, fmt.Errorf("get flag: %w", err)
		}
		blk, err = b.findChildBlock(ctx, searchKey)
		if err != nil {
			return 0, fmt.Errorf("find child block for %q: %w", searchKey, err)
		}
	}
	return blk.BlockNum(), nil
}

// 新しいRootを作り木の深さを増やす
// rootは常にold rootと分割先のdirを持つ
func (b *BTreeDir) MakeNewRoot(ctx context.Context, entry *DirEntry) error {
	firstVal, err := b.contents.GetDataValue(ctx, 0)
	if err != nil {
		return fmt.Errorf("get first value: %w", err)
	}
	level, err := b.contents.GetFlag(ctx)
	if err != nil {
		return fmt.Errorf("get flag: %w", err)
	}
	newBlk, err := b.contents.Split(ctx, 0, level)
	if err != nil {
		return fmt.Errorf("split: %w", err)
	}
	oldRoot := NewDirEntry(firstVal, newBlk.BlockNum())
	_, err = b.insertEntry(ctx, oldRoot)
	if err != nil {
		return fmt.Errorf("insert entry: %w", err)
	}
	_, err = b.insertEntry(ctx, entry)
	if err != nil {
		return fmt.Errorf("insert entry: %w", err)
	}
	if err := b.contents.SetFlag(ctx, level+1); err != nil {
		return fmt.Errorf("set flag: %w", err)
	}
	return nil
}

// Dirを再起的に挿入する
// leafが分割された時に呼ばれる
func (b *BTreeDir) Insert(ctx context.Context, entry *DirEntry) (*DirEntry, error) {
	flag, err := b.contents.GetFlag(ctx)
	if err != nil {
		return nil, fmt.Errorf("get flag: %w", err)
	}
	if flag == 0 {
		return b.insertEntry(ctx, entry)
	}
	childBlock, err := b.findChildBlock(ctx, entry.Value())
	if err != nil {
		return nil, fmt.Errorf("find child block: %w", err)
	}
	bd, err := NewBTreeDir(ctx, b.tx, childBlock, b.layout)
	if err != nil {
		return nil, fmt.Errorf("new btree dir: %w", err)
	}
	childSplittedEntry, err := bd.Insert(ctx, entry)
	if err != nil {
		return nil, fmt.Errorf("insert: %w", err)
	}
	if childSplittedEntry != nil {
		return b.insertEntry(ctx, childSplittedEntry)
	}
	return nil, nil
}

// dirにentryを挿入する
// slotが満杯なら分割が発生し、分割した場合は
func (b *BTreeDir) insertEntry(ctx context.Context, entry *DirEntry) (*DirEntry, error) {
	slot, err := b.contents.FindSlotBefore(ctx, entry.Value())
	if err != nil {
		return nil, fmt.Errorf("find slot before %q: %w", entry.Value(), err)
	}
	newSlot := slot + 1
	if err := b.contents.InsertDir(ctx, newSlot, entry.Value(), entry.BlkNumber()); err != nil {
		return nil, fmt.Errorf("insert dir: %w", err)
	}
	isFull, err := b.contents.IsFull(ctx)
	if err != nil {
		return nil, fmt.Errorf("is full: %w", err)
	}
	if !isFull {
		return nil, nil
	}
	flag, err := b.contents.GetFlag(ctx)
	if err != nil {
		return nil, fmt.Errorf("get flag: %w", err)
	}
	n, err := b.contents.GetNumRecords(ctx)
	if err != nil {
		return nil, fmt.Errorf("get num records: %w", err)
	}
	splitPos := n / 2
	splitVal, err := b.contents.GetDataValue(ctx, splitPos)
	if err != nil {
		return nil, fmt.Errorf("get data value: %w", err)
	}
	newBlk, err := b.contents.Split(ctx, splitPos, flag)
	if err != nil {
		return nil, fmt.Errorf("split: %w", err)
	}
	newEntry := NewDirEntry(splitVal, newBlk.BlockNum())
	return newEntry, nil
}

// 子ブロックのブロック番号を返す
func (b *BTreeDir) findChildBlock(ctx context.Context, searchKey dbconstant.Constant) (dbfile.BlockID, error) {
	// インデックスは左閉,右開区間になっているため、まず直前をポイントする
	slot, err := b.contents.FindSlotBefore(ctx, searchKey)
	if err != nil {
		return dbfile.BlockID{}, fmt.Errorf("find child block: %w", err)
	}
	// 次のスロットと一致している時だけ進める（左閉区間
	val, err := b.contents.GetDataValue(ctx, slot+1)
	if err != nil {
		return dbfile.BlockID{}, fmt.Errorf("gat data value: %w", err)
	}
	if val == searchKey {
		slot++
	}

	blk, err := b.contents.GetChildNum(ctx, slot)
	if err != nil {
		return dbfile.BlockID{}, fmt.Errorf("get child num: %w", err)
	}
	return dbfile.NewBlockID(b.fileName, blk), nil
}
