package dbtx

import (
	"github.com/teru01/simpledb-go/dbfile"
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

func (l startLogRecord) undo(txNumber int) {
	// no-op
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

func (l rollbackLogRecord) undo(txNumber int) {
	// no-op
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
	// no-op
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
