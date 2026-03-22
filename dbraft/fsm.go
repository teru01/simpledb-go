package dbraft

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"

	"github.com/teru01/simpledb-go/dbbuffer"
	"github.com/teru01/simpledb-go/dbfile"
)

const (
	OpSetInt    = 4
	OpSetString = 5
)

type Command struct {
	TxNum   uint64
	Records []WALRecord
}

type WALRecord struct {
	Op        int
	FileName  string
	BlockNum  int
	Offset    int
	IntOldVal int
	IntNewVal int
	StrOldVal string
	StrNewVal string
}

func MarshalCommand(cmd *Command) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(cmd); err != nil {
		return nil, fmt.Errorf("marshal command: %w", err)
	}
	return buf.Bytes(), nil
}

func UnmarshalCommand(data []byte) (*Command, error) {
	var cmd Command
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&cmd); err != nil {
		return nil, fmt.Errorf("unmarshal command: %w", err)
	}
	return &cmd, nil
}

type FSM struct {
	bufferManager *dbbuffer.BufferManager
	fileManager   *dbfile.FileManager
}

func NewFSM(bm *dbbuffer.BufferManager, fm *dbfile.FileManager) *FSM {
	return &FSM{bufferManager: bm, fileManager: fm}
}

func (f *FSM) Apply(ctx context.Context, data []byte, isLeader bool) error {
	if data == nil {
		return nil
	}
	cmd, err := UnmarshalCommand(data)
	if err != nil {
		return fmt.Errorf("unmarshal command: %w", err)
	}

	if isLeader {
		return f.bufferManager.FlushAll(cmd.TxNum)
	}

	for _, rec := range cmd.Records {
		if err := f.ensureBlockExists(rec.FileName, rec.BlockNum); err != nil {
			return fmt.Errorf("ensure block exists for %s block %d: %w", rec.FileName, rec.BlockNum, err)
		}
		blk := dbfile.NewBlockID(rec.FileName, rec.BlockNum)
		buf, err := f.bufferManager.Pin(ctx, blk)
		if err != nil {
			return fmt.Errorf("pin block %s for redo: %w", blk, err)
		}
		switch rec.Op {
		case OpSetInt:
			if err := buf.Contents().SetInt(rec.Offset, rec.IntNewVal); err != nil {
				f.bufferManager.Unpin(buf)
				return fmt.Errorf("set int at offset %d in block %s: %w", rec.Offset, blk, err)
			}
		case OpSetString:
			if err := buf.Contents().SetString(rec.Offset, rec.StrNewVal); err != nil {
				f.bufferManager.Unpin(buf)
				return fmt.Errorf("set string at offset %d in block %s: %w", rec.Offset, blk, err)
			}
		}
		buf.SetModified(cmd.TxNum, -1)
		f.bufferManager.Unpin(buf)
	}

	return f.bufferManager.FlushAll(cmd.TxNum)
}

func (f *FSM) ensureBlockExists(fileName string, blockNum int) error {
	length, err := f.fileManager.FileBlockLength(fileName)
	if err != nil {
		return fmt.Errorf("get file block length for %q: %w", fileName, err)
	}
	for length <= blockNum {
		if _, err := f.fileManager.Append(fileName); err != nil {
			return fmt.Errorf("append block to %q: %w", fileName, err)
		}
		length++
	}
	return nil
}
