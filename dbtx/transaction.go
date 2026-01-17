package dbtx

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"

	"github.com/teru01/simpledb-go/dbbuffer"
	"github.com/teru01/simpledb-go/dbfile"
	"github.com/teru01/simpledb-go/dblog"
)

const EndOfFile = -1

var nextTxNum atomic.Uint64

type Transaction struct {
	recoveryManager    *RecoveryManager
	concurrencyManager *ConcurrencyManager
	bufferManager      *dbbuffer.BufferManager
	fileManager        *dbfile.FileManager
	myBufferList       *BufferList
	state              transactionState
}

type transactionState struct {
	txNum uint64
}

func NewTransaction(fm *dbfile.FileManager, lm *dblog.LogManager, bm *dbbuffer.BufferManager) (*Transaction, error) {
	tx := &Transaction{
		concurrencyManager: NewConcurrencyManager(),
		bufferManager:      bm,
		fileManager:        fm,
		myBufferList:       NewBufferList(bm),
		state: transactionState{
			txNum: NextTxNum(),
		},
	}
	var err error
	tx.recoveryManager, err = NewRecoveryManager(tx, tx.state.txNum, lm, bm)
	if err != nil {
		return nil, fmt.Errorf("initialize recovery manager for transaction %d: %w", tx.state.txNum, err)
	}
	return tx, nil
}

func (t *Transaction) TxNum() uint64 {
	return t.state.txNum
}

func (t *Transaction) Commit() error {
	defer t.concurrencyManager.Release()
	if err := t.recoveryManager.Commit(); err != nil {
		return fmt.Errorf("commit transaction %d: %w", t.state.txNum, err)
	}
	t.myBufferList.UnpinAll()
	slog.Debug("transaction committed", slog.Uint64("txnum", t.state.txNum))
	return nil
}

func (t *Transaction) Rollback(ctx context.Context) error {
	defer t.concurrencyManager.Release()
	if err := t.recoveryManager.Rollback(ctx); err != nil {
		return fmt.Errorf("rollback transaction %d: %w", t.state.txNum, err)
	}
	t.myBufferList.UnpinAll()
	slog.Debug("transaction rollback", slog.Uint64("txnum", t.state.txNum))
	return nil
}

func (t *Transaction) Recover(ctx context.Context) error {
	if err := t.bufferManager.FlushAll(t.state.txNum); err != nil {
		return fmt.Errorf("flush all buffers before recovery: %w", err)
	}
	if err := t.recoveryManager.Recover(ctx); err != nil {
		return fmt.Errorf("recover transaction %d: %w", t.state.txNum, err)
	}
	return nil
}

func (t *Transaction) Pin(ctx context.Context, blk dbfile.BlockID) error {
	return t.myBufferList.Pin(ctx, blk)
}

func (t *Transaction) UnPin(blk dbfile.BlockID) error {
	return t.myBufferList.UnPin(blk)
}

func (t *Transaction) GetInt(ctx context.Context, blk dbfile.BlockID, offset int) (int, error) {
	if err := t.concurrencyManager.SLock(ctx, blk); err != nil {
		return 0, fmt.Errorf("acquire shared lock on block %s: %w", blk, err)
	}
	buf, err := t.myBufferList.Buffer(blk)
	if err != nil {
		return 0, fmt.Errorf("get buffer for block %s: %w", blk, err)
	}
	return buf.Contents().GetInt(offset), nil
}

// valを指定のblock/offsetに書き込む
// あくまでbuffer上でメモリに乗せるだけ。disk書き込みはまだ
func (t *Transaction) SetInt(ctx context.Context, blk dbfile.BlockID, offset, val int, okToLog bool) error {
	if err := t.concurrencyManager.XLock(ctx, blk); err != nil {
		return fmt.Errorf("acquire exclusive lock on block %s: %w", blk, err)
	}
	buf, err := t.myBufferList.Buffer(blk)
	if err != nil {
		return fmt.Errorf("get buffer for block %s (buffer may not be pinned): %w", blk, err)
	}
	lsn := -1
	if okToLog {
		lsn, err = t.recoveryManager.SetInt(buf, offset, val)
	}
	page := buf.Contents()
	if err := page.SetInt(offset, val); err != nil {
		return fmt.Errorf("set int value %d at offset %d in block %s: %w", val, offset, blk, err)
	}
	buf.SetModified(t.state.txNum, lsn)
	return nil
}

func (t *Transaction) GetString(ctx context.Context, blk dbfile.BlockID, offset int) (string, error) {
	if err := t.concurrencyManager.SLock(ctx, blk); err != nil {
		return "", fmt.Errorf("acquire shared lock on block %s: %w", blk, err)
	}
	buf, err := t.myBufferList.Buffer(blk)
	if err != nil {
		return "", fmt.Errorf("get buffer for block %s: %w", blk, err)
	}
	return buf.Contents().GetString(offset), nil
}

func (t *Transaction) SetString(ctx context.Context, blk dbfile.BlockID, offset int, val string, okToLog bool) error {
	if err := t.concurrencyManager.XLock(ctx, blk); err != nil {
		return fmt.Errorf("acquire exclusive lock on block %s: %w", blk, err)
	}
	buf, err := t.myBufferList.Buffer(blk)
	if err != nil {
		return fmt.Errorf("get buffer for block %s (buffer may not be pinned): %w", blk, err)
	}
	lsn := -1
	if okToLog {
		// write ahead log
		lsn, err = t.recoveryManager.SetString(buf, offset, val)
	}
	page := buf.Contents()
	if err := page.SetString(offset, val); err != nil {
		return fmt.Errorf("set string value %q at offset %d in block %s: %w", val, offset, blk, err)
	}
	buf.SetModified(t.state.txNum, lsn)
	return nil
}

// fileNameのファイルが含むブロック数
// ファントム対策にEOFマーカーに対してSLockをとる
func (t *Transaction) Size(ctx context.Context, fileName string) (int, error) {
	blk := dbfile.NewBlockID(fileName, EndOfFile)
	if err := t.concurrencyManager.SLock(ctx, blk); err != nil {
		return 0, fmt.Errorf("acquire shared lock on EOF marker for file %q: %w", fileName, err)
	}
	length, err := t.fileManager.FileBlockLength(fileName)
	if err != nil {
		return 0, fmt.Errorf("get file block length for %q: %w", fileName, err)
	}
	return length, nil
}

// fileNameのファイルに1ブロック追加する
// ファントム対策にEOFマーカーにXLockをとる
func (t *Transaction) Append(ctx context.Context, fileName string) (dbfile.BlockID, error) {
	blk := dbfile.NewBlockID(fileName, EndOfFile)
	if err := t.concurrencyManager.XLock(ctx, blk); err != nil {
		return dbfile.BlockID{}, fmt.Errorf("acquire exclusive lock on EOF marker for file %q: %w", fileName, err)
	}
	blk, err := t.fileManager.Append(fileName)
	if err != nil {
		return dbfile.BlockID{}, fmt.Errorf("append new block to file %q: %w", fileName, err)
	}
	return blk, nil
}

func (t *Transaction) BlockSize() int {
	return t.fileManager.BlockSize()
}

func (t *Transaction) AvailableBuffs() int {
	return t.bufferManager.Available()
}

func NextTxNum() uint64 {
	nextTxNum.Add(1)
	slog.Debug("new transaction", slog.Any("nextTx", nextTxNum.Load()))
	return nextTxNum.Load()
}

func ResetTxNum() {
	nextTxNum.Store(0)
}
