package dbbuffer

import (
	"fmt"

	"github.com/teru01/simpledb-go/dbfile"
	"github.com/teru01/simpledb-go/dblog"
)

type Buffer struct {
	fileManager *dbfile.FileManager
	logManager  *dblog.LogManager
	contents    dbfile.Page
	blk         dbfile.BlockID
	pins        int
	txNum       int // contentsをメモリ上で変更してdisk writeされてないtransaction number
	lsn         int
}

func NewBuffer(fm *dbfile.FileManager, lm *dblog.LogManager) Buffer {
	return Buffer{
		fileManager: fm,
		logManager:  lm,
		contents:    dbfile.NewPage(fm.BlockSize()),
		txNum:       -1,
		lsn:         -1,
	}
}

func (b Buffer) Contents() dbfile.Page {
	return b.contents
}

func (b Buffer) BlockID() dbfile.BlockID {
	return b.blk
}

func (b *Buffer) SetModified(txnum, lsn int) {
	b.txNum = txnum
	if lsn > 0 {
		b.lsn = lsn
	}
}

func (b Buffer) IsPinned() bool {
	return b.pins > 0
}

func (b Buffer) ModifyingTx() int {
	return b.txNum
}

// bufferとblockとの対応関係を変更する
func (b *Buffer) assignToBlock(blockID dbfile.BlockID) error {
	if err := b.flush(); err != nil {
		return fmt.Errorf("faield to flush: %w", err)
	}
	if err := b.fileManager.Read(blockID, b.contents); err != nil {
		return fmt.Errorf("faield to read to assign block: %w", err)
	}
	b.blk = blockID
	b.pins = 0
	return nil
}

func (b *Buffer) flush() error {
	if b.txNum == -1 {
		return nil
	}
	// log managerにappendしてくれる上位コンポーネントがあるはず. それがflushしてもいい？
	if err := b.logManager.Flush(); err != nil {
		return err
	}
	if err := b.fileManager.Write(b.blk, b.contents); err != nil {
		return fmt.Errorf("failed to write buffer: %w", err)
	}
	b.txNum = -1
	return nil
}

func (b *Buffer) pin() {
	b.pins++
}

func (b *Buffer) unpin() {
	b.pins--
}
