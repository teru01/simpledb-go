package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/teru01/simpledb-go/dbbuffer"
	"github.com/teru01/simpledb-go/dbfile"
	"github.com/teru01/simpledb-go/dblog"
	"github.com/teru01/simpledb-go/dbmetadata"
	"github.com/teru01/simpledb-go/dbtx"
)

func main() {
	fmt.Println("Hello, World!")
}

type SimpleDB struct {
	fileManager     *dbfile.FileManager
	logManager      *dblog.LogManager
	bufferManager   *dbbuffer.BufferManager
	metadataManager *dbmetadata.MetadataManager
}

func NewSimpleDB(dirName string, blockSize, bufferSize int) (*SimpleDB, func(), error) {
	f, err := os.Open(dirName)
	if err != nil {
		return nil, nil, fmt.Errorf("open %q: %w", dirName, err)
	}
	fm, err := dbfile.NewFileManager(f, blockSize)
	if err != nil {
		return nil, nil, fmt.Errorf("create file manager: %w", err)
	}
	lm, err := dblog.NewLogManager(fm, "log.log")
	if err != nil {
		return nil, nil, fmt.Errorf("create log manager: %w", err)
	}
	bm := dbbuffer.NewBufferManager(fm, lm, bufferSize)
	return &SimpleDB{fileManager: fm, logManager: lm, bufferManager: bm}, func() {
		f.Close()
	}, nil
}

func (s *SimpleDB) Init(ctx context.Context) error {
	tx, err := dbtx.NewTransaction(s.fileManager, s.logManager, s.bufferManager)
	if err != nil {
		return fmt.Errorf("create transaction: %w", err)
	}
	isNew := s.fileManager.IsNew()
	if isNew {
		slog.Info("initializing new database")
	} else {
		slog.Info("recovering database")
		if err := tx.Recover(ctx); err != nil {
			return fmt.Errorf("recover database: %w", err)
		}
	}

	m, err := dbmetadata.NewMetadataManager(ctx, isNew, tx)
	if err != nil {
		return fmt.Errorf("create metadata manager: %w", err)
	}
	s.metadataManager = m

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	return nil
}
