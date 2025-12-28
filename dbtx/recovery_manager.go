package dbtx

import (
	"fmt"
	"slices"

	"github.com/teru01/simpledb-go/dbbuffer"
	"github.com/teru01/simpledb-go/dblog"
)

type RecoveryManager struct {
	logManager    *dblog.LogManager
	bufferManager *dbbuffer.BufferManager
	tx            Transaction
	txNum         int
}

func NewRecoveryManager(tx Transaction, txNum int, logManager *dblog.LogManager, bufferManager *dbbuffer.BufferManager) (*RecoveryManager, error) {
	rm := &RecoveryManager{tx: tx, txNum: txNum, logManager: logManager, bufferManager: bufferManager}
	_, err := WriteStartToLog(logManager, txNum)
	if err != nil {
		return nil, fmt.Errorf("failed to write start to log: %w", err)
	}
	return rm, nil
}

// 変更とCOMMIT logがディスクに乗ることを保証
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
	finishedTxNum := make([]int, 0)
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
			finishedTxNum = append(finishedTxNum, record.txNumber())
		} else if !slices.Contains(finishedTxNum, record.txNumber()) {
			record.undo(record.txNumber())
		}
	}
	return nil
}
