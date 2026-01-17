package dbbuffer

import (
	"fmt"

	"github.com/teru01/simpledb-go/dbfile"
	"github.com/teru01/simpledb-go/dblog"
)

type Buffer struct {
	ID          int
	fileManager *dbfile.FileManager
	logManager  *dblog.LogManager
	state       bufferState
}

type bufferState struct {
	contents *dbfile.Page
	blk      dbfile.BlockID
	pins     int
	txNum    uint64 // contentsをメモリ上で変更してdisk writeされてないtransaction number
	lsn      int
}

func NewBuffer(id int, fm *dbfile.FileManager, lm *dblog.LogManager) Buffer {
	return Buffer{
		ID:          id,
		fileManager: fm,
		logManager:  lm,
		state: bufferState{
			contents: dbfile.NewPage(fm.BlockSize()),
			blk:      dbfile.BlockID{},
			pins:     0,
			txNum:    0,
			lsn:      -1,
		},
	}
}

func (b *Buffer) Contents() *dbfile.Page {
	return b.state.contents
}

func (b *Buffer) BlockID() dbfile.BlockID {
	return b.state.blk
}

func (b *Buffer) SetModified(txnum uint64, lsn int) {
	b.state.txNum = txnum
	if lsn > 0 {
		b.state.lsn = lsn
	}
}

func (b *Buffer) IsPinned() bool {
	return b.state.pins > 0
}

func (b *Buffer) ModifyingTx() uint64 {
	return b.state.txNum
}

// bufferとblockとの対応関係を変更する
func (b *Buffer) assignToBlock(blockID dbfile.BlockID) error {
	if err := b.flush(); err != nil {
		return fmt.Errorf("flush buffer %d before assigning to block %s: %w", b.ID, blockID, err)
	}
	if err := b.fileManager.Read(blockID, b.state.contents); err != nil {
		return fmt.Errorf("read block %s to buffer %d: %w", blockID, b.ID, err)
	}
	b.state.blk = blockID
	b.state.pins = 0
	return nil
}

// 変更をディスクに書き出す
// WALの原則に従い、先にlogをflushする。flush()が呼ばれる前にlogにはappendされてないといけない
func (b *Buffer) flush() error {
	if b.state.txNum == 0 {
		return nil
	}
	// log managerにappendしてくれる上位コンポーネントがあるはず. それがflushしてもいい？
	if err := b.logManager.Flush(); err != nil {
		return err
	}
	if err := b.fileManager.Write(b.state.blk, b.state.contents); err != nil {
		return fmt.Errorf("write buffer %d to block %s: %w", b.ID, b.state.blk, err)
	}
	b.state.txNum = 0
	return nil
}

func (b *Buffer) pin() {
	b.state.pins++
}

func (b *Buffer) unpin() {
	b.state.pins--
}
