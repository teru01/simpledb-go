package dbtx

import (
	"context"
	"fmt"

	"github.com/teru01/simpledb-go/dbfile"
	"github.com/teru01/simpledb-go/dblog"
	"github.com/teru01/simpledb-go/dbsize"
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
	redo(ctx context.Context, tx *Transaction) error
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
	return &checkpointLogRecord{}
}

func (l *checkpointLogRecord) op() int {
	return CHECKPOINT
}

func (l *checkpointLogRecord) txNumber() uint64 {
	return 0
}

func (l *checkpointLogRecord) undo(ctx context.Context, tx *Transaction) error {
	return nil
}

func (l *checkpointLogRecord) redo(ctx context.Context, tx *Transaction) error {
	return nil
}

func (l *checkpointLogRecord) String() string {
	return fmt.Sprintf("{\"kind\": \"checkpoint\", \"txNum\": %d}", l.txNumber())
}

func WriteCheckpointToLog(lm *dblog.LogManager) (int, error) {
	b := make([]byte, dbsize.IntSize)
	page := dbfile.NewPageFromBytes(b)
	if err := page.SetInt(0, CHECKPOINT); err != nil {
		return 0, fmt.Errorf("set checkpoint operation code at offset 0: %w", err)
	}
	lsn, err := lm.Append(b)
	if err != nil {
		return 0, fmt.Errorf("append checkpoint record to log: %w", err)
	}
	return lsn, nil
}

type startLogRecord struct {
	txNum uint64
}

func NewStartLogRecord(page *dbfile.Page) LogRecord {
	txPos := dbsize.IntSize
	txNum := page.GetUint64(txPos)
	return &startLogRecord{txNum: txNum}
}

func (l *startLogRecord) op() int {
	return START
}

func (l *startLogRecord) txNumber() uint64 {
	return l.txNum
}

func (l *startLogRecord) String() string {
	return fmt.Sprintf("{\"kind\": \"start\", \"txNum\": %d}", l.txNumber())
}

func (l *startLogRecord) undo(ctx context.Context, tx *Transaction) error {
	return nil
}

func (l *startLogRecord) redo(ctx context.Context, tx *Transaction) error {
	return nil
}

func WriteStartToLog(lm *dblog.LogManager, txNum uint64) (int, error) {
	b := make([]byte, dbsize.IntSize+dbsize.Uint64Size)
	txPos := dbsize.IntSize
	page := dbfile.NewPageFromBytes(b)
	if err := page.SetInt(0, START); err != nil {
		return 0, fmt.Errorf("set start operation code at offset 0: %w", err)
	}
	if err := page.SetUint64(txPos, txNum); err != nil {
		return 0, fmt.Errorf("set transaction number %d at offset %d: %w", txNum, txPos, err)
	}
	lsn, err := lm.Append(b)
	if err != nil {
		return 0, fmt.Errorf("append start record to log for transaction %d: %w", txNum, err)
	}
	return lsn, nil
}

type commitLogRecord struct {
	txNum uint64
}

func NewCommitLogRecord(page *dbfile.Page) LogRecord {
	txPos := dbsize.IntSize
	txNum := page.GetUint64(txPos)
	return &commitLogRecord{txNum: txNum}
}

func (l *commitLogRecord) op() int {
	return COMMIT
}

func (l *commitLogRecord) txNumber() uint64 {
	return l.txNum
}

func (l *commitLogRecord) undo(ctx context.Context, tx *Transaction) error {
	return nil
}

func (l *commitLogRecord) redo(ctx context.Context, tx *Transaction) error {
	return nil
}

func (l *commitLogRecord) String() string {
	return fmt.Sprintf("{\"kind\": \"commit\", \"txNum\": %d}", l.txNumber())
}

func WriteCommitToLog(lm *dblog.LogManager, txNum uint64) (int, error) {
	b := make([]byte, dbsize.IntSize+dbsize.Uint64Size)
	txPos := dbsize.IntSize
	page := dbfile.NewPageFromBytes(b)
	if err := page.SetInt(0, COMMIT); err != nil {
		return 0, fmt.Errorf("set commit operation code at offset 0: %w", err)
	}
	if err := page.SetUint64(txPos, txNum); err != nil {
		return 0, fmt.Errorf("set transaction number %d at offset %d: %w", txNum, txPos, err)
	}
	lsn, err := lm.Append(b)
	if err != nil {
		return 0, fmt.Errorf("append commit record to log for transaction %d: %w", txNum, err)
	}
	return lsn, nil
}

type rollbackLogRecord struct {
	txNum uint64
}

func NewRollbackLogRecord(page *dbfile.Page) LogRecord {
	txPos := dbsize.IntSize
	txNum := page.GetUint64(txPos)
	return &rollbackLogRecord{txNum: txNum}
}

func (l *rollbackLogRecord) op() int {
	return ROLLBACK
}

func (l *rollbackLogRecord) txNumber() uint64 {
	return l.txNum
}

func (l *rollbackLogRecord) String() string {
	return fmt.Sprintf("{\"kind\": \"rollback\", \"txNum\": %d}", l.txNumber())
}

func (l *rollbackLogRecord) undo(ctx context.Context, tx *Transaction) error {
	return nil
}

func (l *rollbackLogRecord) redo(ctx context.Context, tx *Transaction) error {
	return nil
}

func WriteRollbackToLog(lm *dblog.LogManager, txNum uint64) (int, error) {
	b := make([]byte, dbsize.IntSize+dbsize.Uint64Size)
	txPos := dbsize.IntSize
	page := dbfile.NewPageFromBytes(b)
	if err := page.SetInt(0, ROLLBACK); err != nil {
		return 0, fmt.Errorf("set rollback operation code at offset 0: %w", err)
	}
	if err := page.SetUint64(txPos, txNum); err != nil {
		return 0, fmt.Errorf("set transaction number %d at offset %d: %w", txNum, txPos, err)
	}
	lsn, err := lm.Append(b)
	if err != nil {
		return 0, fmt.Errorf("append rollback record to log for transaction %d: %w", txNum, err)
	}
	return lsn, nil
}

type setIntLogRecord struct {
	txNum    uint64
	blockID  dbfile.BlockID
	offset   int
	value    int
	newValue int
}

func NewSetIntLogRecord(page *dbfile.Page) LogRecord {
	txPos := dbsize.IntSize
	txNum := page.GetUint64(txPos)
	fileNamePos := txPos + dbsize.Uint64Size
	fileName := page.GetString(fileNamePos)
	blockNumPos := fileNamePos + dbfile.MaxStringLengthOnPage(len(fileName))
	blockNum := page.GetInt(blockNumPos)
	blockID := dbfile.NewBlockID(fileName, blockNum)
	offsetPos := blockNumPos + dbsize.IntSize
	offset := page.GetInt(offsetPos)
	valuePos := offsetPos + dbsize.IntSize
	value := page.GetInt(valuePos)
	newValuePos := valuePos + dbsize.IntSize
	newValue := page.GetInt(newValuePos)
	return &setIntLogRecord{txNum: txNum, blockID: blockID, offset: offset, value: value, newValue: newValue}
}

func (l *setIntLogRecord) op() int {
	return SETINT
}

func (l *setIntLogRecord) txNumber() uint64 {
	return l.txNum
}

func (l *setIntLogRecord) undo(ctx context.Context, tx *Transaction) error {
	if err := tx.Pin(ctx, l.blockID); err != nil {
		return fmt.Errorf("pin block %s for undo: %w", l.blockID, err)
	}
	if err := tx.SetInt(ctx, l.blockID, l.offset, l.value, false); err != nil {
		return fmt.Errorf("set int value %d at offset %d in block %s for undo: %w", l.value, l.offset, l.blockID, err)
	}
	if err := tx.UnPin(l.blockID); err != nil {
		return fmt.Errorf("unpin block %s after undo: %w", l.blockID, err)
	}
	return nil
}

func (l *setIntLogRecord) redo(ctx context.Context, tx *Transaction) error {
	if err := tx.Pin(ctx, l.blockID); err != nil {
		return fmt.Errorf("pin block %s for redo: %w", l.blockID, err)
	}
	if err := tx.SetInt(ctx, l.blockID, l.offset, l.newValue, false); err != nil {
		return fmt.Errorf("set int value %d at offset %d in block %s for redo: %w", l.newValue, l.offset, l.blockID, err)
	}
	if err := tx.UnPin(l.blockID); err != nil {
		return fmt.Errorf("unpin block %s after redo: %w", l.blockID, err)
	}
	return nil
}

func (l *setIntLogRecord) String() string {
	return fmt.Sprintf("{\"kind\": \"setInt\", \"txNum\": %d, \"blockID\": %s, \"offset\": %d, \"value\": %d, \"newValue\": %d}", l.txNumber(), l.blockID.String(), l.offset, l.value, l.newValue)
}

// SETINT,TXNUM,FILENAME,BLOCKNUM,OFFSET,OLDVALUE,NEWVALUE
func WriteIntToLog(lm *dblog.LogManager, txNum uint64, blockID dbfile.BlockID, offset int, oldValue int, newValue int) (int, error) {
	txPos := dbsize.IntSize
	fileNamePos := txPos + dbsize.Uint64Size
	blockPos := fileNamePos + dbfile.MaxStringLengthOnPage(len(blockID.FileName()))
	offsetPos := blockPos + dbsize.IntSize
	oldValuePos := offsetPos + dbsize.IntSize
	newValuePos := oldValuePos + dbsize.IntSize
	recordLen := newValuePos + dbsize.IntSize
	b := make([]byte, recordLen)
	page := dbfile.NewPageFromBytes(b)
	if err := page.SetInt(0, SETINT); err != nil {
		return 0, fmt.Errorf("set SETINT operation code at offset 0: %w", err)
	}
	if err := page.SetUint64(txPos, txNum); err != nil {
		return 0, fmt.Errorf("set transaction number %d at offset %d: %w", txNum, txPos, err)
	}
	if err := page.SetString(fileNamePos, blockID.FileName()); err != nil {
		return 0, fmt.Errorf("set block file name %q at offset %d: %w", blockID.FileName(), fileNamePos, err)
	}
	if err := page.SetInt(blockPos, blockID.BlockNum()); err != nil {
		return 0, fmt.Errorf("set block number %d at offset %d: %w", blockID.BlockNum(), blockPos, err)
	}
	if err := page.SetInt(offsetPos, offset); err != nil {
		return 0, fmt.Errorf("set offset %d at offset %d: %w", offset, offsetPos, err)
	}
	if err := page.SetInt(oldValuePos, oldValue); err != nil {
		return 0, fmt.Errorf("set int old value %d at offset %d: %w", oldValue, oldValuePos, err)
	}
	if err := page.SetInt(newValuePos, newValue); err != nil {
		return 0, fmt.Errorf("set int new value %d at offset %d: %w", newValue, newValuePos, err)
	}
	lsn, err := lm.Append(b)
	if err != nil {
		return 0, fmt.Errorf("append SETINT log record for transaction %d: %w", txNum, err)
	}
	return lsn, nil
}

type setStringLogRecord struct {
	txNum    uint64
	blockID  dbfile.BlockID
	offset   int
	value    string
	newValue string
}

func NewSetStringLogRecord(page *dbfile.Page) LogRecord {
	txPos := dbsize.IntSize
	txNum := page.GetUint64(txPos)
	fileNamePos := txPos + dbsize.Uint64Size
	fileName := page.GetString(fileNamePos)
	blockNumPos := fileNamePos + dbfile.MaxStringLengthOnPage(len(fileName))
	blockNum := page.GetInt(blockNumPos)
	blockID := dbfile.NewBlockID(fileName, blockNum)
	offsetPos := blockNumPos + dbsize.IntSize
	offset := page.GetInt(offsetPos)
	valuePos := offsetPos + dbsize.IntSize
	value := page.GetString(valuePos)
	newValuePos := valuePos + dbfile.MaxStringLengthOnPage(len(value))
	newValue := page.GetString(newValuePos)
	return &setStringLogRecord{txNum: txNum, blockID: blockID, offset: offset, value: value, newValue: newValue}
}

func (l *setStringLogRecord) op() int {
	return SETSTRING
}

func (l *setStringLogRecord) txNumber() uint64 {
	return l.txNum
}

func (l *setStringLogRecord) undo(ctx context.Context, tx *Transaction) error {
	if err := tx.Pin(ctx, l.blockID); err != nil {
		return fmt.Errorf("pin block %s for undo: %w", l.blockID, err)
	}
	if err := tx.SetString(ctx, l.blockID, l.offset, l.value, false); err != nil {
		return fmt.Errorf("set string value %q at offset %d in block %s for undo: %w", l.value, l.offset, l.blockID, err)
	}
	if err := tx.UnPin(l.blockID); err != nil {
		return fmt.Errorf("unpin block %s after undo: %w", l.blockID, err)
	}
	return nil
}

func (l *setStringLogRecord) redo(ctx context.Context, tx *Transaction) error {
	if err := tx.Pin(ctx, l.blockID); err != nil {
		return fmt.Errorf("pin block %s for redo: %w", l.blockID, err)
	}
	if err := tx.SetString(ctx, l.blockID, l.offset, l.newValue, false); err != nil {
		return fmt.Errorf("set string value %q at offset %d in block %s for redo: %w", l.newValue, l.offset, l.blockID, err)
	}
	if err := tx.UnPin(l.blockID); err != nil {
		return fmt.Errorf("unpin block %s after redo: %w", l.blockID, err)
	}
	return nil
}

func (l *setStringLogRecord) String() string {
	return fmt.Sprintf("{\"kind\": \"setString\", \"txNum\": %d, \"blockID\": %s, \"offset\": %d, \"value\": %s, \"newValue\": %s}", l.txNumber(), l.blockID.String(), l.offset, l.value, l.newValue)
}

// SETSTRING,TXNUM,FILENAME,BLOCKNUM,OFFSET,OLDVALUE,NEWVALUE
func WriteStringToLog(lm *dblog.LogManager, txNum uint64, blockID dbfile.BlockID, offset int, oldValue string, newValue string) (int, error) {
	txPos := dbsize.IntSize
	fileNamePos := txPos + dbsize.Uint64Size
	blockPos := fileNamePos + dbfile.MaxStringLengthOnPage(len(blockID.FileName()))
	offsetPos := blockPos + dbsize.IntSize
	oldValuePos := offsetPos + dbsize.IntSize
	newValuePos := oldValuePos + dbfile.MaxStringLengthOnPage(len(oldValue))
	recordLen := newValuePos + dbfile.MaxStringLengthOnPage(len(newValue))
	b := make([]byte, recordLen)
	page := dbfile.NewPageFromBytes(b)
	if err := page.SetInt(0, SETSTRING); err != nil {
		return 0, fmt.Errorf("set SETSTRING operation code at offset 0: %w", err)
	}
	if err := page.SetUint64(txPos, txNum); err != nil {
		return 0, fmt.Errorf("set transaction number %d at offset %d: %w", txNum, txPos, err)
	}
	if err := page.SetString(fileNamePos, blockID.FileName()); err != nil {
		return 0, fmt.Errorf("set block file name %q at offset %d: %w", blockID.FileName(), fileNamePos, err)
	}
	if err := page.SetInt(blockPos, blockID.BlockNum()); err != nil {
		return 0, fmt.Errorf("set block number %d at offset %d: %w", blockID.BlockNum(), blockPos, err)
	}
	if err := page.SetInt(offsetPos, offset); err != nil {
		return 0, fmt.Errorf("set offset %d at offset %d: %w", offset, offsetPos, err)
	}
	if err := page.SetString(oldValuePos, oldValue); err != nil {
		return 0, fmt.Errorf("set string old value %q at offset %d: %w", oldValue, oldValuePos, err)
	}
	if err := page.SetString(newValuePos, newValue); err != nil {
		return 0, fmt.Errorf("set string new value %q at offset %d: %w", newValue, newValuePos, err)
	}
	lsn, err := lm.Append(b)
	if err != nil {
		return 0, fmt.Errorf("append SETSTRING log record for transaction %d: %w", txNum, err)
	}
	return lsn, nil
}
