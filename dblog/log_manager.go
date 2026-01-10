package dblog

import (
	"fmt"
	"iter"
	"sync"

	"github.com/teru01/simpledb-go/dbfile"
	"github.com/teru01/simpledb-go/size"
)

type LogManager struct {
	mu          sync.RWMutex
	fileManager *dbfile.FileManager
	logFileName string
	state       logManagerState
}

type logManagerState struct {
	logPage      *dbfile.Page
	currentBlock dbfile.BlockID
	latestLSN    int
	lastSavedLSN int
}

func NewLogManager(fm *dbfile.FileManager, logFileName string) (*LogManager, error) {
	var (
		currentBlock dbfile.BlockID
		err          error
	)

	lm := LogManager{
		fileManager: fm,
		logFileName: logFileName,
	}

	logSize, err := fm.FileBlockLength(logFileName)
	if err != nil {
		return nil, fmt.Errorf("failed to get file block length: %w", err)
	}

	b := make([]byte, fm.BlockSize())
	p := dbfile.NewPageFromBytes(b)

	if logSize == 0 {
		currentBlock, err = lm.appendNewBlockLocked(p)
	} else {
		currentBlock = dbfile.NewBlockID(lm.logFileName, logSize-1)
		err = lm.fileManager.Read(currentBlock, p)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to init logPage: %w", err)
	}

	lm.state = logManagerState{
		logPage:      p,
		currentBlock: currentBlock,
		latestLSN:    0,
		lastSavedLSN: 0,
	}

	return &lm, nil
}

// 指定のlog sequenceまでのflushを保証する
func (lm *LogManager) FlushWithLSN(lsn int) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	if lsn <= lm.state.lastSavedLSN {
		// already saved
		return nil
	}
	return lm.flushlocked()
}

func (lm *LogManager) Flush() error {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	return lm.flushlocked()
}

// 最新のlog sequenceまでFlushする
func (lm *LogManager) flushlocked() error {
	if err := lm.fileManager.Write(lm.state.currentBlock, lm.state.logPage); err != nil {
		return fmt.Errorf("failed to flush to block %v: %w", lm.state.currentBlock, err)
	}
	lm.state.lastSavedLSN = lm.state.latestLSN
	return nil
}

func (lm *LogManager) Append(logRecord []byte) (int, error) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	boundary := lm.state.logPage.GetInt(0)
	recordSize := len(logRecord)
	bytesNeeded := recordSize + size.IntSize
	if boundary-bytesNeeded < size.IntSize {
		// はみ出る
		if err := lm.flushlocked(); err != nil {
			return 0, fmt.Errorf("failed to Append: %w", err)
		}
		blk, err := lm.appendNewBlockLocked(lm.state.logPage)
		if err != nil {
			return 0, fmt.Errorf("failed to Append: %w", err)
		}
		lm.state.currentBlock = blk
		boundary = lm.state.logPage.GetInt(0)
	}

	recordPos := boundary - bytesNeeded
	if err := lm.state.logPage.SetBytes(recordPos, logRecord); err != nil {
		return 0, err
	}
	if err := lm.state.logPage.SetInt(0, recordPos); err != nil {
		return 0, err
	}
	lm.state.latestLSN++
	return lm.state.latestLSN, nil
}

// log fileに1ブロック追加しページを初期化する. lock前提
func (lm *LogManager) appendNewBlockLocked(p *dbfile.Page) (dbfile.BlockID, error) {
	block, err := lm.fileManager.Append(lm.logFileName)
	if err != nil {
		return dbfile.BlockID{}, fmt.Errorf("faile to append block to %s: %w", lm.logFileName, err)
	}
	if err := p.SetInt(0, lm.fileManager.BlockSize()); err != nil {
		return dbfile.BlockID{}, err
	}
	if err := lm.fileManager.Write(block, p); err != nil {
		return dbfile.BlockID{}, err
	}
	return block, nil
}

func (lm *LogManager) Iterator() (iter.Seq2[[]byte, error], error) {
	if err := lm.Flush(); err != nil {
		return nil, fmt.Errorf("failed to create iter")
	}

	// appendしかされず過去のブロックは変更されないのでcurrent blockだけread lockする
	lm.mu.RLock()
	currentBlk := lm.state.currentBlock
	lm.mu.RUnlock()
	return func(yield func([]byte, error) bool) {
		var iterError error
		for i := currentBlk.BlockNum(); i >= 0; i-- {
			currentBlock := dbfile.NewBlockID(lm.logFileName, i)
			p := dbfile.NewPage(lm.fileManager.BlockSize())
			if err := lm.fileManager.Read(currentBlock, p); err != nil {
				iterError = fmt.Errorf("failed to read current block: %w", err)
				if !yield(nil, iterError) {
					return
				}
			}
			boundary := p.GetInt(0)
			for j := boundary; j < p.Length(); j += p.GetInt(j) + size.IntSize {
				if !yield(p.GetBytes(j), nil) {
					return
				}
			}
		}
	}, nil
}
