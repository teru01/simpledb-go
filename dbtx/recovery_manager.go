package dbtx

import (
	"fmt"

	"github.com/teru01/simpledb-go/dbbuffer"
	"github.com/teru01/simpledb-go/dblog"
)

type RecoveryManager struct {
	logManager    *dblog.LogManager
	bufferManager *dbbuffer.BufferManager
	tx            *Transaction
	txNum         int
}

func NewRecoveryManager(tx *Transaction, txNum int, logManager *dblog.LogManager, bufferManager *dbbuffer.BufferManager) (*RecoveryManager, error) {
	rm := &RecoveryManager{tx: tx, txNum: txNum, logManager: logManager, bufferManager: bufferManager}
	_, err := WriteStartToLog(logManager, txNum)
	if err != nil {
		return nil, fmt.Errorf("failed to write start to log: %w", err)
	}
	return rm, nil
}

// バッファの変更が先にディスクに乗る(このwrite自体はWALが守られている）
// undo only recovery専用のcommit実装になっている: logにcommitがあるけど実際にはディスクに乗ってない、ケースは発生しない
func (rm *RecoveryManager) Commit() error {
	if err := rm.bufferManager.FlushAll(rm.txNum); err != nil {
		return fmt.Errorf("failed to flush all buffers: %w", err)
	}
	lsn, err := WriteCommitToLog(rm.logManager, rm.txNum)
	if err != nil {
		return fmt.Errorf("failed to commit: %w", err)
	}
	if err := rm.logManager.FlushWithLSN(lsn); err != nil {
		return fmt.Errorf("failed to flush with lsn: %w", err)
	}
	return nil
}

func (rm *RecoveryManager) Rollback() error {
	if err := rm.doRollback(); err != nil {
		return fmt.Errorf("failed to rollback: %w", err)
	}
	if err := rm.bufferManager.FlushAll(rm.txNum); err != nil {
		return fmt.Errorf("failed to flush all buffers: %w", err)
	}
	lsn, err := WriteRollbackToLog(rm.logManager, rm.txNum)
	if err != nil {
		return fmt.Errorf("failed to write rollback to log: %w", err)
	}
	if err := rm.logManager.FlushWithLSN(lsn); err != nil {
		return fmt.Errorf("failed to flush with lsn: %w", err)
	}
	return nil
}

func (rm *RecoveryManager) doRollback() error {
	it, err := rm.logManager.Iterator()
	if err != nil {
		return fmt.Errorf("failed to get iterator: %w", err)
	}
	for record, err := range it {
		if err != nil {
			return fmt.Errorf("failed to get next record: %w", err)
		}
		record := NewLogRecord(record)
		if record.txNumber() == rm.txNum {
			if record.op() == START {
				return nil
			}
			record.undo(rm.txNum)
		}
	}
	return nil
}

func (rm *RecoveryManager) Recover() error {
	if err := rm.doRecover(); err != nil {
		return fmt.Errorf("failed to recover: %w", err)
	}
	if err := rm.bufferManager.FlushAll(rm.txNum); err != nil {
		return fmt.Errorf("failed to flush all buffers: %w", err)
	}
	lsn, err := WriteCheckpointToLog(rm.logManager)
	if err != nil {
		return fmt.Errorf("failed to write checkpoint to log: %w", err)
	}
	if err := rm.logManager.FlushWithLSN(lsn); err != nil {
		return fmt.Errorf("failed to flush with lsn: %w", err)
	}
	return nil
}

func (rm *RecoveryManager) doRecover() error {
	finishedTxNum := make(map[int]struct{}, 0)
	it, err := rm.logManager.Iterator()
	if err != nil {
		return fmt.Errorf("failed to get iterator: %w", err)
	}
	for record, err := range it {
		if err != nil {
			return fmt.Errorf("failed to get next record: %w", err)
		}
		record := NewLogRecord(record)
		if record.op() == CHECKPOINT {
			return nil
		} else if record.op() == COMMIT || record.op() == ROLLBACK {
			finishedTxNum[record.txNumber()] = struct{}{}
		} else if _, ok := finishedTxNum[record.txNumber()]; !ok {
			record.undo(rm.txNum)
		}
	}
	return nil
}
