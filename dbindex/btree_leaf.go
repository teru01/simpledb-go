package dbindex

import (
	"context"
	"fmt"

	"github.com/teru01/simpledb-go/dbconstant"
	"github.com/teru01/simpledb-go/dbfile"
	"github.com/teru01/simpledb-go/dbrecord"
	"github.com/teru01/simpledb-go/dbtx"
)

type BTreeLeaf struct {
	tx          *dbtx.Transaction
	layout      *dbrecord.Layout
	searchKey   dbconstant.Constant
	contents    *BTreePage
	currentSlot int
	fileName    string
}

func NewBTreeLeaf(ctx context.Context, tx *dbtx.Transaction, blk dbfile.BlockID, layout *dbrecord.Layout, searchKey dbconstant.Constant) (*BTreeLeaf, error) {
	btp, err := NewBTreePage(ctx, tx, &blk, layout)
	if err != nil {
		return nil, fmt.Errorf("new btree page: %w", err)
	}
	slot, err := btp.FindSlotBefore(ctx, searchKey)
	if err != nil {
		return nil, fmt.Errorf("find slot before: %w", err)
	}
	return &BTreeLeaf{
		tx:          tx,
		layout:      layout,
		searchKey:   searchKey,
		contents:    btp,
		currentSlot: slot,
		fileName:    blk.FileName(),
	}, nil
}

func (b *BTreeLeaf) Close(ctx context.Context) error {
	return b.contents.Close()
}

func (b *BTreeLeaf) Next(ctx context.Context) (bool, error) {
	b.currentSlot++
	n, err := b.contents.GetNumRecords(ctx)
	if err != nil {
		return false, fmt.Errorf("next: %w", err)
	}
	c, err := b.contents.GetDataValue(ctx, b.currentSlot)
	if err != nil {
		return false, fmt.Errorf("get data value in the index: %w", err)
	}
	if b.currentSlot >= n {
		isOverFlow, err := b.tryOverflow(ctx)
		if err != nil {
			return false, fmt.Errorf("try overflow in next curret slot(%q): %w", b.currentSlot, err)
		}
		return isOverFlow, nil
	} else if c.Equals(b.searchKey) {
		return true, nil
	} else {
		// ブロックの端まで行ってないが、検索キーと一致しないとき
		// AAABCFG..みたいな時にBに到達したケース. Aのoverflowが存在する可能性があるため
		return b.tryOverflow(ctx)
	}
}

func (b *BTreeLeaf) GetDataRID(ctx context.Context) (*dbrecord.RID, error) {
	return b.contents.GetDataRID(ctx, b.currentSlot)
}

func (b *BTreeLeaf) Delete(ctx context.Context, dataRID dbrecord.RID) error {
	for {
		exists, err := b.Next(ctx)
		if err != nil {
			return fmt.Errorf("next while deleting %q: %w", &dataRID, err)
		}
		if !exists {
			return nil
		}
		rid, err := b.GetDataRID(ctx)
		if err != nil {
			return fmt.Errorf("get data rid: %w", err)
		}
		if *rid == dataRID {
			if err := b.contents.Delete(ctx, b.currentSlot); err != nil {
				return fmt.Errorf("delete: %w", err)
			}
		}
	}
}

// overflowブロックが存在していればそこに移動する
// flagは<0の時オーバーフローなし、f>0の時オーバーフロー先のblock number
func (b *BTreeLeaf) tryOverflow(ctx context.Context) (bool, error) {
	firstKey, err := b.contents.GetDataValue(ctx, 0)
	if err != nil {
		return false, fmt.Errorf("get first key: %w", err)
	}
	flag, err := b.contents.GetFlag(ctx)
	if err != nil {
		return false, fmt.Errorf("get flag: %w", err)
	}
	if !b.searchKey.Equals(firstKey) || flag < 0 {
		// blockの最初がsearchKeyに一致しないあるいはオーバーフローしてない
		return false, nil
	}
	if err := b.contents.Close(); err != nil {
		return false, fmt.Errorf("close: %w", err)
	}
	nextBlk := dbfile.NewBlockID(b.fileName, flag)
	bt, err := NewBTreePage(ctx, b.tx, &nextBlk, b.layout)
	if err != nil {
		return false, fmt.Errorf("new btree page: %w", err)
	}
	b.contents = bt
	b.currentSlot = 0
	return true, nil
}

func (b *BTreeLeaf) Insert(ctx context.Context, dataRID *dbrecord.RID) (*DirEntry, error) {
	flag, err := b.contents.GetFlag(ctx)
	if err != nil {
		return nil, fmt.Errorf("get flag: %w", err)
	}
	firstKey, err := b.contents.GetDataValue(ctx, 0)
	if err != nil {
		return nil, fmt.Errorf("get data value: %w", err)
	}

	// overflowが存在して、かつsearchKeyが先頭より小さい時
	// 全て直後の新しいブロックへ移動させて、元のブロックの先頭へ挿入
	if flag >= 0 && firstKey.Compare(b.searchKey) > 0 {
		// 全て新たなブロックへ移動
		newBlk, err := b.contents.Split(ctx, 0, flag)
		if err != nil {
			return nil, fmt.Errorf("split: %w", err)
		}
		b.currentSlot = 0
		b.contents.SetFlag(ctx, -1)
		b.contents.InsertLeaf(ctx, 0, b.searchKey, dataRID)

		return NewDirEntry(firstKey, newBlk.BlockNum()), nil
	}
	b.currentSlot++
	if err := b.contents.InsertLeaf(ctx, b.currentSlot, b.searchKey, dataRID); err != nil {
		return nil, fmt.Errorf("insert leaf to %q: %w", b.currentSlot, err)
	}
	isFull, err := b.contents.IsFull(ctx)
	if err != nil {
		return nil, fmt.Errorf("is full: %w", err)
	}
	if !isFull {
		return nil, nil
	}

	// 満杯なら分割
	n, err := b.contents.GetNumRecords(ctx)
	if err != nil {
		return nil, fmt.Errorf("get num records: %w", err)
	}
	lastKey, err := b.contents.GetDataValue(ctx, n-1)
	if err != nil {
		return nil, fmt.Errorf("get last key: %w", err)
	}

	if lastKey.Equals(firstKey) {
		// 分割できないのでオーバーフローを作る

		// flagを保持することで既にオーバーフローしててもチェーンできる
		newBlk, err := b.contents.Split(ctx, 1, flag)
		if err != nil {
			return nil, fmt.Errorf("split: %w", err)
		}
		if err := b.contents.SetFlag(ctx, newBlk.BlockNum()); err != nil {
			return nil, fmt.Errorf("set flag: %w", err)
		}
		return nil, nil
	}

	// 素直に分割できるケース
	splitPos := n / 2
	splitKey, err := b.contents.GetDataValue(ctx, splitPos)
	if err != nil {
		return nil, fmt.Errorf("get data value at %q: %w", splitPos, err)
	}
	if splitKey.Equals(firstKey) {
		// 右側に検索
		var newBlk dbfile.BlockID
		for i := splitPos; i < n; i++ {
			splitKey, err = b.contents.GetDataValue(ctx, i)
			if err != nil {
				return nil, fmt.Errorf("get data value at %q: %w", i, err)
			}
			if !splitKey.Equals(firstKey) {
				newBlk, err = b.contents.Split(ctx, i, -1)
				if err != nil {
					return nil, fmt.Errorf("split at %q: %w", i, err)
				}
				break
			}
		}
		return NewDirEntry(splitKey, newBlk.BlockNum()), nil
	} else {
		// 左側に検索
		var newBlk dbfile.BlockID
		for i := splitPos; i >= 0; i-- {
			val, err := b.contents.GetDataValue(ctx, i)
			if err != nil {
				return nil, fmt.Errorf("get data value at %q: %w", i, err)
			}
			if !splitKey.Equals(val) {
				newBlk, err = b.contents.Split(ctx, i+1, -1)
				if err != nil {
					return nil, fmt.Errorf("split at %q: %w", i, err)
				}
				break
			}
		}
		return NewDirEntry(splitKey, newBlk.BlockNum()), nil
	}
}
