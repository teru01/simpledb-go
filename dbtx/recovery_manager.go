package dbtx

import (
	"context"
	"fmt"

	"github.com/teru01/simpledb-go/dbbuffer"
	"github.com/teru01/simpledb-go/dblog"
)

// 個々のtxが独立したインスタンスを持つ
type RecoveryManager struct {
	logManager    *dblog.LogManager
	bufferManager *dbbuffer.BufferManager
	tx            *Transaction
	txNum         uint64
}

func NewRecoveryManager(tx *Transaction, txNum uint64, logManager *dblog.LogManager, bufferManager *dbbuffer.BufferManager) (*RecoveryManager, error) {
	rm := &RecoveryManager{tx: tx, txNum: txNum, logManager: logManager, bufferManager: bufferManager}
	_, err := WriteStartToLog(logManager, txNum)
	if err != nil {
		return nil, fmt.Errorf("write start record to log for transaction %d: %w", txNum, err)
	}
	return rm, nil
}

// バッファの変更が先にディスクに乗る(このwrite自体はWALが守られている）
// undo only recovery専用のcommit実装になっている: logにcommitがあるけど実際にはディスクに乗ってない、ケースは発生しない
func (rm *RecoveryManager) Commit() error {
	if err := rm.bufferManager.FlushAll(rm.txNum); err != nil {
		return fmt.Errorf("flush all buffers for transaction %d: %w", rm.txNum, err)
	}
	lsn, err := WriteCommitToLog(rm.logManager, rm.txNum)
	if err != nil {
		return fmt.Errorf("write commit record to log for transaction %d: %w", rm.txNum, err)
	}
	if err := rm.logManager.FlushWithLSN(lsn); err != nil {
		return fmt.Errorf("flush log with LSN %d: %w", lsn, err)
	}
	return nil
}

func (rm *RecoveryManager) Rollback(ctx context.Context) error {
	if err := rm.doRollback(ctx); err != nil {
		return fmt.Errorf("rollback transaction %d: %w", rm.txNum, err)
	}
	if err := rm.bufferManager.FlushAll(rm.txNum); err != nil {
		return fmt.Errorf("flush all buffers for transaction %d: %w", rm.txNum, err)
	}
	lsn, err := WriteRollbackToLog(rm.logManager, rm.txNum)
	if err != nil {
		return fmt.Errorf("write rollback record to log for transaction %d: %w", rm.txNum, err)
	}
	if err := rm.logManager.FlushWithLSN(lsn); err != nil {
		return fmt.Errorf("flush log with LSN %d: %w", lsn, err)
	}
	return nil
}

func (rm *RecoveryManager) doRollback(ctx context.Context) error {
	it, err := rm.logManager.Iterator()
	if err != nil {
		return fmt.Errorf("get log iterator for transaction %d: %w", rm.txNum, err)
	}
	for record, err := range it {
		if err != nil {
			return fmt.Errorf("get next log record: %w", err)
		}
		record := NewLogRecord(record)
		if record.txNumber() == rm.txNum {
			if record.op() == START {
				return nil
			}
			if err := record.undo(ctx, rm.tx); err != nil {
				return fmt.Errorf("undo log record for transaction %d: %w", rm.txNum, err)
			}
		}
	}
	return nil
}

func (rm *RecoveryManager) Recover(ctx context.Context) error {
	if err := rm.doRecover(ctx); err != nil {
		return fmt.Errorf("recover transaction %d: %w", rm.txNum, err)
	}
	if err := rm.bufferManager.FlushAll(rm.txNum); err != nil {
		return fmt.Errorf("flush all buffers for transaction %d: %w", rm.txNum, err)
	}
	lsn, err := WriteCheckpointToLog(rm.logManager)
	if err != nil {
		return fmt.Errorf("write checkpoint record to log: %w", err)
	}
	if err := rm.logManager.FlushWithLSN(lsn); err != nil {
		return fmt.Errorf("flush log with LSN %d: %w", lsn, err)
	}
	return nil
}

func (rm *RecoveryManager) doRecover(ctx context.Context) error {
	finishedTxNum := make(map[uint64]struct{}, 0)
	it, err := rm.logManager.Iterator()
	if err != nil {
		return fmt.Errorf("get log iterator: %w", err)
	}
	for record, err := range it {
		if err != nil {
			return fmt.Errorf("get next log record: %w", err)
		}
		record := NewLogRecord(record)
		if record.op() == CHECKPOINT {
			return nil
		} else if record.op() == COMMIT || record.op() == ROLLBACK {
			finishedTxNum[record.txNumber()] = struct{}{}
		} else if _, ok := finishedTxNum[record.txNumber()]; !ok {
			if err := record.undo(ctx, rm.tx); err != nil {
				return fmt.Errorf("undo log record for transaction %d: %w", record.txNumber(), err)
			}
		}
	}
	return nil
}

func (rm *RecoveryManager) SetInt(buf *dbbuffer.Buffer, offset, val int) (int, error) {
	oldVal := buf.Contents().GetInt(offset)
	blk := buf.BlockID()
	return WriteIntToLog(rm.logManager, rm.txNum, blk, offset, oldVal)
}

func (rm *RecoveryManager) SetString(buf *dbbuffer.Buffer, offset int, val string) (int, error) {
	oldVal := buf.Contents().GetString(offset)
	blk := buf.BlockID()
	return WriteStringToLog(rm.logManager, rm.txNum, blk, offset, oldVal)
}
