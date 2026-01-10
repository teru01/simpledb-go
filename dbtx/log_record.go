package dbtx

import (
	"context"
	"fmt"

	"github.com/teru01/simpledb-go/dbfile"
	"github.com/teru01/simpledb-go/dblog"
	"github.com/teru01/simpledb-go/size"
)

const (
	CHECKPOINT = 0
	START      = 1
	COMMIT     = 2
	ROLLBACK   = 3
	SETINT     = 4
	SETSTRING  = 5
)

type LogRecord interface {
	op() int
	txNumber() uint64
	undo(ctx context.Context, tx *Transaction) error
}

func NewLogRecord(contents []byte) LogRecord {
	page := dbfile.NewPageFromBytes(contents)
	switch page.GetInt(0) {
	case CHECKPOINT:
		return NewCheckpointLogRecord()
	case START:
		return NewStartLogRecord(page)
	case COMMIT:
		return NewCommitLogRecord(page)
	case ROLLBACK:
		return NewRollbackLogRecord(page)
	case SETINT:
		return NewSetIntLogRecord(page)
	case SETSTRING:
		return NewSetStringLogRecord(page)
	}
	return nil
}

type checkpointLogRecord struct{}

func NewCheckpointLogRecord() LogRecord {
	return checkpointLogRecord{}
}

func (l checkpointLogRecord) op() int {
	return CHECKPOINT
}

func (l checkpointLogRecord) txNumber() uint64 {
	return 0
}

func (l checkpointLogRecord) undo(ctx context.Context, tx *Transaction) error {
	return nil
}

func (l checkpointLogRecord) String() string {
	return fmt.Sprintf("{\"kind\": \"checkpoint\", \"txNum\": %d}", l.txNumber())
}

func WriteCheckpointToLog(lm *dblog.LogManager) (int, error) {
	b := make([]byte, size.IntSize)
	page := dbfile.NewPageFromBytes(b)
	if err := page.SetInt(0, CHECKPOINT); err != nil {
		return 0, fmt.Errorf("failed to set op: %w", err)
	}
	lsn, err := lm.Append(b)
	if err != nil {
		return 0, fmt.Errorf("failed to write checkpoint to log: %w", err)
	}
	return lsn, nil
}

type startLogRecord struct {
	txNum uint64
}

func NewStartLogRecord(page *dbfile.Page) LogRecord {
	txPos := size.IntSize
	txNum := page.GetUint64(txPos)
	return &startLogRecord{txNum: txNum}
}

func (l startLogRecord) op() int {
	return START
}

func (l startLogRecord) txNumber() uint64 {
	return l.txNum
}

func (l startLogRecord) String() string {
	return fmt.Sprintf("{\"kind\": \"start\", \"txNum\": %d}", l.txNumber())
}

func (l startLogRecord) undo(ctx context.Context, tx *Transaction) error {
	return nil
}

func WriteStartToLog(lm *dblog.LogManager, txNum uint64) (int, error) {
	b := make([]byte, size.IntSize+size.Uint64Size)
	txPos := size.IntSize
	page := dbfile.NewPageFromBytes(b)
	if err := page.SetInt(0, START); err != nil {
		return 0, fmt.Errorf("failed to set op: %w", err)
	}
	if err := page.SetUint64(txPos, txNum); err != nil {
		return 0, fmt.Errorf("failed to set txNum: %w", err)
	}
	lsn, err := lm.Append(b)
	if err != nil {
		return 0, fmt.Errorf("failed to write start to log: %w", err)
	}
	return lsn, nil
}

type commitLogRecord struct {
	txNum uint64
}

func NewCommitLogRecord(page *dbfile.Page) LogRecord {
	txPos := size.IntSize
	txNum := page.GetUint64(txPos)
	return &commitLogRecord{txNum: txNum}
}

func (l commitLogRecord) op() int {
	return COMMIT
}

func (l commitLogRecord) txNumber() uint64 {
	return l.txNum
}

func (l commitLogRecord) undo(ctx context.Context, tx *Transaction) error {
	return nil
}

func (l commitLogRecord) String() string {
	return fmt.Sprintf("{\"kind\": \"commit\", \"txNum\": %d}", l.txNumber())
}

func WriteCommitToLog(lm *dblog.LogManager, txNum uint64) (int, error) {
	b := make([]byte, size.IntSize+size.Uint64Size)
	txPos := size.IntSize
	page := dbfile.NewPageFromBytes(b)
	if err := page.SetInt(0, COMMIT); err != nil {
		return 0, fmt.Errorf("failed to set op: %w", err)
	}
	if err := page.SetUint64(txPos, txNum); err != nil {
		return 0, fmt.Errorf("failed to set txNum: %w", err)
	}
	lsn, err := lm.Append(b)
	if err != nil {
		return 0, fmt.Errorf("failed to write commit to log: %w", err)
	}
	return lsn, nil
}

type rollbackLogRecord struct {
	txNum uint64
}

func NewRollbackLogRecord(page *dbfile.Page) LogRecord {
	txPos := size.IntSize
	txNum := page.GetUint64(txPos)
	return &rollbackLogRecord{txNum: txNum}
}

func (l rollbackLogRecord) op() int {
	return ROLLBACK
}

func (l rollbackLogRecord) txNumber() uint64 {
	return l.txNum
}

func (l rollbackLogRecord) String() string {
	return fmt.Sprintf("{\"kind\": \"rollback\", \"txNum\": %d}", l.txNumber())
}

func (l rollbackLogRecord) undo(ctx context.Context, tx *Transaction) error {
	return nil
}

func WriteRollbackToLog(lm *dblog.LogManager, txNum uint64) (int, error) {
	b := make([]byte, size.IntSize+size.Uint64Size)
	txPos := size.IntSize
	page := dbfile.NewPageFromBytes(b)
	if err := page.SetInt(0, ROLLBACK); err != nil {
		return 0, fmt.Errorf("failed to set op: %w", err)
	}
	if err := page.SetUint64(txPos, txNum); err != nil {
		return 0, fmt.Errorf("failed to set txNum: %w", err)
	}
	lsn, err := lm.Append(b)
	if err != nil {
		return 0, fmt.Errorf("failed to write rollback to log: %w", err)
	}
	return lsn, nil
}

type setIntLogRecord struct {
	txNum   uint64
	blockID dbfile.BlockID
	offset  int
	value   int
}

func NewSetIntLogRecord(page *dbfile.Page) LogRecord {
	txPos := size.IntSize
	txNum := page.GetUint64(txPos)
	fileNamePos := txPos + size.Uint64Size
	fileName := page.GetString(fileNamePos)
	blockNumPos := fileNamePos + dbfile.MaxStringLengthOnPage(len(fileName))
	blockNum := page.GetInt(blockNumPos)
	blockID := dbfile.NewBlockID(fileName, blockNum)
	offsetPos := blockNumPos + size.IntSize
	offset := page.GetInt(offsetPos)
	valuePos := offsetPos + size.IntSize
	value := page.GetInt(valuePos)
	return &setIntLogRecord{txNum: txNum, blockID: blockID, offset: offset, value: value}
}

func (l setIntLogRecord) op() int {
	return SETINT
}

func (l setIntLogRecord) txNumber() uint64 {
	return l.txNum
}

func (l setIntLogRecord) undo(ctx context.Context, tx *Transaction) error {
	if err := tx.Pin(ctx, l.blockID); err != nil {
		return fmt.Errorf("failed to pin block: %w", err)
	}
	if err := tx.SetInt(ctx, l.blockID, l.offset, l.value, false); err != nil {
		return fmt.Errorf("failed to set int: %w", err)
	}
	if err := tx.UnPin(l.blockID); err != nil {
		return fmt.Errorf("failed to unpin block: %w", err)
	}
	return nil
}

func (l setIntLogRecord) String() string {
	return fmt.Sprintf("{\"kind\": \"setInt\", \"txNum\": %d, \"blockID\": %s, \"offset\": %d, \"value\": %d}", l.txNumber(), l.blockID.String(), l.offset, l.value)
}

// SETINT,TXNUM,FILENAME,BLOCKNUM,OFFSET,VALUE
func WriteIntToLog(lm *dblog.LogManager, txNum uint64, blockID dbfile.BlockID, offset int, value int) (int, error) {
	txPos := size.IntSize
	fileNamePos := txPos + size.Uint64Size
	blockPos := fileNamePos + dbfile.MaxStringLengthOnPage(len(blockID.FileName()))
	offsetPos := blockPos + size.IntSize
	valuePos := offsetPos + size.IntSize
	recordLen := valuePos + size.IntSize
	b := make([]byte, recordLen)
	page := dbfile.NewPageFromBytes(b)
	if err := page.SetInt(0, SETINT); err != nil {
		return 0, fmt.Errorf("failed to set op: %w", err)
	}
	if err := page.SetUint64(txPos, txNum); err != nil {
		return 0, fmt.Errorf("failed to set txNum: %w", err)
	}
	if err := page.SetString(fileNamePos, blockID.FileName()); err != nil {
		return 0, fmt.Errorf("failed to set blockID: %w", err)
	}
	if err := page.SetInt(blockPos, blockID.BlockNum()); err != nil {
		return 0, fmt.Errorf("failed to set blockNum: %w", err)
	}
	if err := page.SetInt(offsetPos, offset); err != nil {
		return 0, fmt.Errorf("failed to set offset: %w", err)
	}
	if err := page.SetInt(valuePos, value); err != nil {
		return 0, fmt.Errorf("failed to set value: %w", err)
	}
	lsn, err := lm.Append(b)
	if err != nil {
		return 0, fmt.Errorf("failed to append log record: %w", err)
	}
	return lsn, nil
}

type setStringLogRecord struct {
	txNum   uint64
	blockID dbfile.BlockID
	offset  int
	value   string
}

func NewSetStringLogRecord(page *dbfile.Page) LogRecord {
	txPos := size.IntSize
	txNum := page.GetUint64(txPos)
	fileNamePos := txPos + size.Uint64Size
	fileName := page.GetString(fileNamePos)
	blockNumPos := fileNamePos + dbfile.MaxStringLengthOnPage(len(fileName))
	blockNum := page.GetInt(blockNumPos)
	blockID := dbfile.NewBlockID(fileName, blockNum)
	offsetPos := blockNumPos + size.IntSize
	offset := page.GetInt(offsetPos)
	valuePos := offsetPos + size.IntSize
	value := page.GetString(valuePos)
	return &setStringLogRecord{txNum: txNum, blockID: blockID, offset: offset, value: value}
}

func (l setStringLogRecord) op() int {
	return SETSTRING
}

func (l setStringLogRecord) txNumber() uint64 {
	return l.txNum
}

func (l setStringLogRecord) undo(ctx context.Context, tx *Transaction) error {
	if err := tx.Pin(ctx, l.blockID); err != nil {
		return fmt.Errorf("failed to pin block: %w", err)
	}
	if err := tx.SetString(ctx, l.blockID, l.offset, l.value, false); err != nil {
		return fmt.Errorf("failed to set string: %w", err)
	}
	if err := tx.UnPin(l.blockID); err != nil {
		return fmt.Errorf("failed to unpin block: %w", err)
	}
	return nil
}

func (l setStringLogRecord) String() string {
	return fmt.Sprintf("{\"kind\": \"setString\", \"txNum\": %d, \"blockID\": %s, \"offset\": %d, \"value\": %s}", l.txNumber(), l.blockID.String(), l.offset, l.value)
}

// SETSTRING,TXNUM,FILENAME,BLOCKNUM,OFFSET,VALUE
func WriteStringToLog(lm *dblog.LogManager, txNum uint64, blockID dbfile.BlockID, offset int, value string) (int, error) {
	txPos := size.IntSize
	fileNamePos := txPos + size.Uint64Size
	blockPos := fileNamePos + dbfile.MaxStringLengthOnPage(len(blockID.FileName()))
	offsetPos := blockPos + size.IntSize
	valuePos := offsetPos + size.IntSize
	recordLen := valuePos + dbfile.MaxStringLengthOnPage(len(value))
	b := make([]byte, recordLen)
	page := dbfile.NewPageFromBytes(b)
	if err := page.SetInt(0, SETSTRING); err != nil {
		return 0, fmt.Errorf("failed to set op: %w", err)
	}
	if err := page.SetUint64(txPos, txNum); err != nil {
		return 0, fmt.Errorf("failed to set txNum: %w", err)
	}
	if err := page.SetString(fileNamePos, blockID.FileName()); err != nil {
		return 0, fmt.Errorf("failed to set blockID: %w", err)
	}
	if err := page.SetInt(blockPos, blockID.BlockNum()); err != nil {
		return 0, fmt.Errorf("failed to set blockNum: %w", err)
	}
	if err := page.SetInt(offsetPos, offset); err != nil {
		return 0, fmt.Errorf("failed to set offset: %w", err)
	}
	if err := page.SetString(valuePos, value); err != nil {
		return 0, fmt.Errorf("failed to set value: %w", err)
	}
	lsn, err := lm.Append(b)
	if err != nil {
		return 0, fmt.Errorf("failed to append log record: %w", err)
	}
	return lsn, nil
}
