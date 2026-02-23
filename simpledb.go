package main

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	"github.com/teru01/simpledb-go/dbbuffer"
	"github.com/teru01/simpledb-go/dbfile"
	"github.com/teru01/simpledb-go/dblog"
	"github.com/teru01/simpledb-go/dbmetadata"
	"github.com/teru01/simpledb-go/dbplan"
	"github.com/teru01/simpledb-go/dbrecord"
	"github.com/teru01/simpledb-go/dbtx"
	"os"
)

type SimpleDB struct {
	fileManager     *dbfile.FileManager
	logManager      *dblog.LogManager
	bufferManager   *dbbuffer.BufferManager
	metadataManager *dbmetadata.MetadataManager
	planner         *dbplan.Planner
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

	qp := dbplan.NewQueryPlanner(s.metadataManager)
	up := dbplan.NewUpdatePlanner(s.metadataManager)
	s.planner = dbplan.NewPlanner(qp, up)

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	return nil
}

func (s *SimpleDB) Execute(ctx context.Context, sql string) error {
	tx, err := dbtx.NewTransaction(s.fileManager, s.logManager, s.bufferManager)
	if err != nil {
		return fmt.Errorf("create transaction: %w", err)
	}

	if strings.HasPrefix(strings.ToLower(sql), "select") {
		if err := s.execQuery(ctx, tx, sql); err != nil {
			tx.Rollback(ctx)
			return err
		}
	} else {
		n, err := s.planner.ExecuteUpdate(ctx, sql, tx)
		if err != nil {
			tx.Rollback(ctx)
			return err
		}
		fmt.Printf("%d row(s) affected\n", n)
	}

	return tx.Commit()
}

func (s *SimpleDB) execQuery(ctx context.Context, tx *dbtx.Transaction, sql string) error {
	plan, err := s.planner.CreateQueryPlan(ctx, sql, tx)
	if err != nil {
		return err
	}
	scan, err := plan.Open(ctx)
	if err != nil {
		return err
	}
	defer scan.Close(ctx)

	schema := plan.Schema()
	fields := schema.Fields()

	var rows [][]string
	for {
		ok, err := scan.Next(ctx)
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		row := make([]string, 0, len(fields))
		for _, f := range fields {
			switch schema.FieldType(f) {
			case dbrecord.FieldTypeInt:
				v, err := scan.GetInt(ctx, f)
				if err != nil {
					return err
				}
				row = append(row, strconv.Itoa(v))
			case dbrecord.FieldTypeString:
				v, err := scan.GetString(ctx, f)
				if err != nil {
					return err
				}
				row = append(row, v)
			}
		}
		rows = append(rows, row)
	}

	// calculate column widths
	widths := make([]int, len(fields))
	for i, f := range fields {
		widths[i] = len(f)
	}
	for _, row := range rows {
		for i, v := range row {
			if len(v) > widths[i] {
				widths[i] = len(v)
			}
		}
	}

	// print header
	header := make([]string, len(fields))
	for i, f := range fields {
		header[i] = padRight(f, widths[i])
	}
	fmt.Println(strings.Join(header, " | "))

	// print separator
	sep := make([]string, len(fields))
	for i, w := range widths {
		sep[i] = strings.Repeat("-", w)
	}
	fmt.Println(strings.Join(sep, "-+-"))

	// print rows
	for _, row := range rows {
		cols := make([]string, len(fields))
		for i, v := range row {
			cols[i] = padRight(v, widths[i])
		}
		fmt.Println(strings.Join(cols, " | "))
	}

	fmt.Printf("%d row(s)\n", len(rows))
	return nil
}

func padRight(s string, width int) string {
	if len(s) >= width {
		return s
	}
	return s + strings.Repeat(" ", width-len(s))
}
