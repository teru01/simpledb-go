package dblog

import (
	"fmt"
	"iter"
	"sync"

	"github.com/teru01/simpledb-go/dbfile"
	"github.com/teru01/simpledb-go/size"
)

type LogManager struct {
	mu           sync.Mutex
	fileManager  *dbfile.FileManager
	logFileName  string
	logPage      dbfile.Page
	currentBlock dbfile.BlockID
	latestLSN    int
	lastSavedLSN int
}

func NewLogManager(fm *dbfile.FileManager, logFileName string) (*LogManager, error) {
	var (
		currentBlock dbfile.BlockID
		err          error
	)

	b := make([]byte, fm.BlockSize())
	p := dbfile.NewPageFromBytes(b)

	lm := LogManager{
		fileManager: fm,
		logFileName: logFileName,
		logPage:     p,
	}

	logSize, err := fm.FileBlockLength(logFileName)
	if err != nil {
		return nil, fmt.Errorf("failed to get file block length: %w", err)
	}

	if logSize == 0 {
		currentBlock, err = lm.appendNewBlock()
	} else {
		currentBlock = dbfile.NewBlockID(lm.logFileName, logSize-1)
		err = lm.fileManager.Read(currentBlock, lm.logPage)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to init logPage: %w", err)
	}

	lm.currentBlock = currentBlock

	return &lm, nil
}

// 指定のlog sequenceまでをflushする
func (lm *LogManager) FlushWithLSN(lsn int) error {
	if lsn <= lm.lastSavedLSN {
		// already saved
		return nil
	}
	return lm.Flush()
}

// 最新のlog sequenceまでFlushする
func (lm *LogManager) Flush() error {
	if err := lm.fileManager.Write(lm.currentBlock, lm.logPage); err != nil {
		return fmt.Errorf("failed to flush to block %v: %w", lm.currentBlock, err)
	}
	lm.lastSavedLSN = lm.latestLSN
	return nil
}

func (lm *LogManager) Append(logRecord []byte) (int, error) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	boundary := lm.logPage.GetInt(0)
	recordSize := len(logRecord)
	bytesNeeded := recordSize + size.IntSize
	if boundary-bytesNeeded < size.IntSize {
		// はみ出る
		if err := lm.Flush(); err != nil {
			return 0, fmt.Errorf("failed to Append: %w", err)
		}
		blk, err := lm.appendNewBlock()
		if err != nil {
			return 0, fmt.Errorf("failed to Append: %w", err)
		}
		lm.currentBlock = blk
		boundary = lm.logPage.GetInt(0)
	}

	recordPos := boundary - bytesNeeded
	if err := lm.logPage.SetBytes(recordPos, logRecord); err != nil {
		return 0, err
	}
	if err := lm.logPage.SetInt(0, recordPos); err != nil {
		return 0, err
	}
	lm.latestLSN++
	return lm.latestLSN, nil
}

// log fileに1ブロック追加しページを初期化する
func (lm *LogManager) appendNewBlock() (dbfile.BlockID, error) {
	block, err := lm.fileManager.Append(lm.logFileName)
	if err != nil {
		return dbfile.BlockID{}, fmt.Errorf("faile to append block to %s: %w", lm.logFileName, err)
	}
	if err := lm.logPage.SetInt(0, lm.fileManager.BlockSize()); err != nil {
		return dbfile.BlockID{}, err
	}
	if err := lm.fileManager.Write(block, lm.logPage); err != nil {
		return dbfile.BlockID{}, err
	}
	return block, nil
}

func (lm *LogManager) Iterator() (iter.Seq2[[]byte, error], error) {
	if err := lm.Flush(); err != nil {
		return nil, fmt.Errorf("failed to create iter")
	}
	return func(yield func([]byte, error) bool) {
		var iterError error
		for i := lm.currentBlock.BlockNum(); i >= 0; i-- {
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
