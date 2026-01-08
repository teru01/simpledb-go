package dbtx

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/teru01/simpledb-go/dbbuffer"
	"github.com/teru01/simpledb-go/dbfile"
	"github.com/teru01/simpledb-go/dblog"
)

const END_OF_FILE = -1

var nextTxNum = 0

type Transaction struct {
	recoveryManager    *RecoveryManager
	concurrencyManager *ConcurrencyManager
	bufferManager      *dbbuffer.BufferManager
	fileManager        *dbfile.FileManager
	myBufferList       *BufferList
	state              transactionState
}

type transactionState struct {
	txNum int
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
		return nil, fmt.Errorf("failed to init recovery manager: %w", err)
	}
	return tx, nil
}

func (t *Transaction) Commit() error {
	defer t.concurrencyManager.Release()
	if err := t.recoveryManager.Commit(); err != nil {
		return fmt.Errorf("failed to commit: %w", err)
	}
	t.myBufferList.UnpinAll()
	slog.Debug("transaction committed", slog.Int("txnum", t.state.txNum))
	return nil
}

func (t *Transaction) Rollback(ctx context.Context) error {
	defer t.concurrencyManager.Release()
	if err := t.recoveryManager.Rollback(ctx); err != nil {
		return fmt.Errorf("failed to commit: %w", err)
	}
	t.myBufferList.UnpinAll()
	slog.Debug("transaction committed", slog.Int("txnum", t.state.txNum))
	return nil
}

func (t *Transaction) Recover(ctx context.Context) error {
	if err := t.bufferManager.FlushAll(t.state.txNum); err != nil {
		return fmt.Errorf("failed to flush before recover: %w", err)
	}
	if err := t.recoveryManager.Recover(ctx); err != nil {
		return fmt.Errorf("failed to recover: %w", err)
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
		return 0, fmt.Errorf("failed to SLock: %w", err)
	}
	buf, err := t.myBufferList.Buffer(blk)
	if err != nil {
		return 0, fmt.Errorf("failed to get buffer: %w", err)
	}
	return buf.Contents().GetInt(offset), nil
}

// valを指定のblock/offsetに書き込む
func (t *Transaction) SetInt(ctx context.Context, blk dbfile.BlockID, offset, val int, okToLog bool) error {
	if err := t.concurrencyManager.XLock(ctx, blk); err != nil {
		return fmt.Errorf("failed to XLock: %w", err)
	}
	buf, err := t.myBufferList.Buffer(blk)
	if err != nil {
		return fmt.Errorf("failed to get buffer, maybe the buffer is not pinned: %w", err)
	}
	lsn := -1
	if okToLog {
		// write ahead log
		lsn, err = t.recoveryManager.SetInt(buf, offset, val)
	}
	page := buf.Contents()
	if err := page.SetInt(offset, val); err != nil {
		return fmt.Errorf("failed to set int: %w", err)
	}
	buf.SetModified(t.state.txNum, lsn)
	return nil
}

func (t *Transaction) GetString(ctx context.Context, blk dbfile.BlockID, offset int) (string, error) {
	if err := t.concurrencyManager.SLock(ctx, blk); err != nil {
		return "", fmt.Errorf("failed to SLock: %w", err)
	}
	buf, err := t.myBufferList.Buffer(blk)
	if err != nil {
		return "", fmt.Errorf("failed to get buffer: %w", err)
	}
	return buf.Contents().GetString(offset), nil
}

func (t *Transaction) SetString(ctx context.Context, blk dbfile.BlockID, offset int, val string, okToLog bool) error {
	if err := t.concurrencyManager.XLock(ctx, blk); err != nil {
		return fmt.Errorf("failed to XLock: %w", err)
	}
	buf, err := t.myBufferList.Buffer(blk)
	if err != nil {
		return fmt.Errorf("failed to get buffer, maybe the buffer is not pinned: %w", err)
	}
	lsn := -1
	if okToLog {
		// write ahead log
		lsn, err = t.recoveryManager.SetString(buf, offset, val)
	}
	page := buf.Contents()
	if err := page.SetString(offset, val); err != nil {
		return fmt.Errorf("failed to set val: %w", err)
	}
	buf.SetModified(t.state.txNum, lsn)
	return nil
}

// ファントム対策
func (t *Transaction) Size(ctx context.Context, fileName string) (int, error) {
	blk := dbfile.NewBlockID(fileName, END_OF_FILE)
	if err := t.concurrencyManager.SLock(ctx, blk); err != nil {
		return 0, fmt.Errorf("failed to SLock: %w", err)
	}
	length, err := t.fileManager.FileBlockLength(fileName)
	if err != nil {
		return 0, fmt.Errorf("failed to get file block length: %w", err)
	}
	return length, nil
}

// ファントム対策
func (t *Transaction) Append(ctx context.Context, fileName string) (dbfile.BlockID, error) {
	blk := dbfile.NewBlockID(fileName, END_OF_FILE)
	if err := t.concurrencyManager.XLock(ctx, blk); err != nil {
		return dbfile.BlockID{}, fmt.Errorf("failed to XLock: %w", err)
	}
	blk, err := t.fileManager.Append(fileName)
	if err != nil {
		return dbfile.BlockID{}, fmt.Errorf("failed to append: %w", err)
	}
	return blk, nil
}

func (t *Transaction) BlockSize() int {
	return t.fileManager.BlockSize()
}

func (t *Transaction) AvailableBuffs() int {
	return t.bufferManager.Available()
}

func NextTxNum() int {
	nextTxNum++
	slog.Debug("new transaction", slog.Any("nextTx", nextTxNum))
	return nextTxNum
}

func ResetTxNum() {
	nextTxNum = 0
}
