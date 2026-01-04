package dbtx

import (
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
	txNumber() int
	undo(txNumber int)
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

func (l checkpointLogRecord) txNumber() int {
	return -1
}

func (l checkpointLogRecord) undo(txNumber int) {
	// no-op
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
	txNum int
}

func NewStartLogRecord(page dbfile.Page) LogRecord {
	txPos := size.IntSize
	txNum := page.GetInt(txPos)
	return &startLogRecord{txNum: txNum}
}

func (l startLogRecord) op() int {
	return START
}

func (l startLogRecord) txNumber() int {
	return l.txNum
}

func (l startLogRecord) String() string {
	return fmt.Sprintf("{\"kind\": \"start\", \"txNum\": %d}", l.txNumber())
}

func (l startLogRecord) undo(txNumber int) {
	// no-op
}

func WriteStartToLog(lm *dblog.LogManager, txNum int) (int, error) {
	b := make([]byte, size.IntSize)
	txPos := size.IntSize
	page := dbfile.NewPageFromBytes(b)
	if err := page.SetInt(0, START); err != nil {
		return 0, fmt.Errorf("failed to set op: %w", err)
	}
	if err := page.SetInt(txPos, txNum); err != nil {
		return 0, fmt.Errorf("failed to set txNum: %w", err)
	}
	lsn, err := lm.Append(b)
	if err != nil {
		return 0, fmt.Errorf("failed to write start to log: %w", err)
	}
	return lsn, nil
}

type commitLogRecord struct {
	txNum int
}

func NewCommitLogRecord(page dbfile.Page) LogRecord {
	txPos := size.IntSize
	txNum := page.GetInt(txPos)
	return &commitLogRecord{txNum: txNum}
}

func (l commitLogRecord) op() int {
	return COMMIT
}

func (l commitLogRecord) txNumber() int {
	return l.txNum
}

func (l commitLogRecord) undo(txNumber int) {
	// no-op
}

func (l commitLogRecord) String() string {
	return fmt.Sprintf("{\"kind\": \"commit\", \"txNum\": %d}", l.txNumber())
}

func WriteCommitToLog(lm *dblog.LogManager, txNum int) (int, error) {
	b := make([]byte, size.IntSize)
	txPos := size.IntSize
	page := dbfile.NewPageFromBytes(b)
	if err := page.SetInt(0, COMMIT); err != nil {
		return 0, fmt.Errorf("failed to set op: %w", err)
	}
	if err := page.SetInt(txPos, txNum); err != nil {
		return 0, fmt.Errorf("failed to set txNum: %w", err)
	}
	lsn, err := lm.Append(b)
	if err != nil {
		return 0, fmt.Errorf("failed to write commit to log: %w", err)
	}
	return lsn, nil
}

type rollbackLogRecord struct {
	txNum int
}

func NewRollbackLogRecord(page dbfile.Page) LogRecord {
	txPos := size.IntSize
	txNum := page.GetInt(txPos)
	return &rollbackLogRecord{txNum: txNum}
}

func (l rollbackLogRecord) op() int {
	return ROLLBACK
}

func (l rollbackLogRecord) txNumber() int {
	return l.txNum
}

func (l rollbackLogRecord) String() string {
	return fmt.Sprintf("{\"kind\": \"rollback\", \"txNum\": %d}", l.txNumber())
}

func (l rollbackLogRecord) undo(txNumber int) {
	// no-op
}

func WriteRollbackToLog(lm *dblog.LogManager, txNum int) (int, error) {
	b := make([]byte, size.IntSize)
	txPos := size.IntSize
	page := dbfile.NewPageFromBytes(b)
	if err := page.SetInt(0, ROLLBACK); err != nil {
		return 0, fmt.Errorf("failed to set op: %w", err)
	}
	if err := page.SetInt(txPos, txNum); err != nil {
		return 0, fmt.Errorf("failed to set txNum: %w", err)
	}
	lsn, err := lm.Append(b)
	if err != nil {
		return 0, fmt.Errorf("failed to write rollback to log: %w", err)
	}
	return lsn, nil
}

type setIntLogRecord struct {
	txNum   int
	blockID dbfile.BlockID
	offset  int
	value   int
}

func NewSetIntLogRecord(page dbfile.Page) LogRecord {
	txPos := size.IntSize
	txNum := page.GetInt(txPos)
	blockIDPos := txPos + size.IntSize
	blockID := dbfile.NewBlockID(page.GetString(blockIDPos), page.GetInt(blockIDPos+size.IntSize))
	offsetPos := blockIDPos + page.MaxLength(len(blockID.FileName()))
	offset := page.GetInt(offsetPos)
	valuePos := offsetPos + size.IntSize
	value := page.GetInt(valuePos)
	return &setIntLogRecord{txNum: txNum, blockID: blockID, offset: offset, value: value}
}

func (l setIntLogRecord) op() int {
	return SETINT
}

func (l setIntLogRecord) txNumber() int {
	return l.txNum
}

func (l setIntLogRecord) undo(txNumber int) {
	
}

func (l setIntLogRecord) String() string {
	return fmt.Sprintf("{\"kind\": \"setInt\", \"txNum\": %d, \"blockID\": %s, \"offset\": %d, \"value\": %d}", l.txNumber(), l.blockID.String(), l.offset, l.value)
}

func WriteIntToLog(lm *dblog.LogManager, txNum int, blockID dbfile.BlockID, offset int, value int) (int, error) {
	txPos := size.IntSize
	fileNamePos := txPos + size.IntSize
	blockPos := fileNamePos + dbfile.MaxStringLengthOnPage(len(blockID.FileName()))
	offsetPos := blockPos + size.IntSize
	valuePos := offsetPos + size.IntSize
	recordLen := valuePos + size.IntSize
	b := make([]byte, recordLen)
	page := dbfile.NewPageFromBytes(b)
	if err := page.SetInt(0, SETINT); err != nil {
		return 0, fmt.Errorf("failed to set op: %w", err)
	}
	if err := page.SetInt(txPos, txNum); err != nil {
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
	txNum   int
	blockID dbfile.BlockID
	offset  int
	value   string
}

func NewSetStringLogRecord(page dbfile.Page) LogRecord {
	txPos := size.IntSize
	txNum := page.GetInt(txPos)
	blockIDPos := txPos + size.IntSize
	blockID := dbfile.NewBlockID(page.GetString(blockIDPos), page.GetInt(blockIDPos+size.IntSize))
	offsetPos := blockIDPos + page.MaxLength(len(blockID.FileName()))
	offset := page.GetInt(offsetPos)
	valuePos := offsetPos + size.IntSize
	value := page.GetString(valuePos)
	return &setStringLogRecord{txNum: txNum, blockID: blockID, offset: offset, value: value}
}

func (l setStringLogRecord) op() int {
	return SETSTRING
}

func (l setStringLogRecord) txNumber() int {
	return l.txNum
}

func (l setStringLogRecord) undo(txNumber int) {
	// no-op
}

func (l setStringLogRecord) String() string {
	return fmt.Sprintf("{\"kind\": \"setString\", \"txNum\": %d, \"blockID\": %s, \"offset\": %d, \"value\": %s}", l.txNumber(), l.blockID.String(), l.offset, l.value)
}

func WriteStringToLog(lm *dblog.LogManager, txNum int, blockID dbfile.BlockID, offset int, value string) (int, error) {
	txPos := size.IntSize
	fileNamePos := txPos + size.IntSize
	blockPos := fileNamePos + dbfile.MaxStringLengthOnPage(len(blockID.FileName()))
	offsetPos := blockPos + size.IntSize
	valuePos := offsetPos + size.IntSize
	recordLen := valuePos + dbfile.MaxStringLengthOnPage(len(value))
	b := make([]byte, recordLen)
	page := dbfile.NewPageFromBytes(b)
	if err := page.SetInt(0, SETSTRING); err != nil {
		return 0, fmt.Errorf("failed to set op: %w", err)
	}
	if err := page.SetInt(txPos, txNum); err != nil {
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
