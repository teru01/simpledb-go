package dblog

import (
	"fmt"

	"github.com/teru01/simpledb-go/dbfile"
)

type LogManager struct {
	fileManager  *dbfile.FileManager
	logFileName  string
	logPage      dbfile.Page
	currentBlock dbfile.BlockID
	latestLSN    int
	lastSavedLSN int
}

func NewLogManager(fm *dbfile.FileManager, logFileName string) (*LogManager, error) {
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

	var cb int
	// if logSize == 0 {
	// 	cb =
	// }

}

func (lm *LogManager) AppendNewBlock() (dbfile.BlockID, error) {
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
