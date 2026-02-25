package dbexecutor

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	"os"

	"github.com/teru01/simpledb-go/dbbuffer"
	"github.com/teru01/simpledb-go/dbfile"
	"github.com/teru01/simpledb-go/dblog"
	"github.com/teru01/simpledb-go/dbmetadata"
	"github.com/teru01/simpledb-go/dbplan"
	"github.com/teru01/simpledb-go/dbrecord"
	"github.com/teru01/simpledb-go/dbtx"
)

// ExecuteResult represents the result of a SQL execution.
type ExecuteResult struct {
	// Tag is the command tag (e.g. "SELECT 3", "INSERT 0 1", "CREATE TABLE", "BEGIN", "COMMIT").
	Tag string
	// Fields holds column names for SELECT results.
	Fields []string
	// FieldTypes holds column types (dbrecord.FieldTypeInt or dbrecord.FieldTypeString) for SELECT results.
	FieldTypes []int
	// Rows holds the result rows as string values for SELECT results.
	Rows [][]string
}

type SimpleDB struct {
	fileManager     *dbfile.FileManager
	logManager      *dblog.LogManager
	bufferManager   *dbbuffer.BufferManager
	metadataManager *dbmetadata.MetadataManager
	planner         *dbplan.Planner
	explicitTx      *dbtx.Transaction
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
	up := dbplan.NewIndexUpdatePlanner(s.metadataManager)
	s.planner = dbplan.NewPlanner(qp, up)

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	return nil
}

func (s *SimpleDB) Execute(ctx context.Context, sql string) (*ExecuteResult, error) {
	if matchStartTx(sql) {
		tx, err := dbtx.NewTransaction(s.fileManager, s.logManager, s.bufferManager)
		if err != nil {
			return nil, fmt.Errorf("create transaction: %w", err)
		}
		s.explicitTx = tx
		return &ExecuteResult{Tag: "BEGIN"}, nil
	} else if matchCommit(sql) {
		if s.explicitTx == nil {
			return nil, fmt.Errorf("no transactions yet.")
		}
		if err := s.explicitTx.Commit(); err != nil {
			return nil, fmt.Errorf("commit: %w", err)
		}
		s.explicitTx = nil
		return &ExecuteResult{Tag: "COMMIT"}, nil
	} else if matchRollback(sql) {
		if s.explicitTx == nil {
			return nil, fmt.Errorf("no transactions yet.")
		}
		if err := s.explicitTx.Rollback(ctx); err != nil {
			return nil, fmt.Errorf("rollback: %w", err)
		}
		s.explicitTx = nil
		return &ExecuteResult{Tag: "ROLLBACK"}, nil
	}

	var (
		tx  *dbtx.Transaction
		err error
	)
	if s.explicitTx != nil {
		tx = s.explicitTx
	} else {
		tx, err = dbtx.NewTransaction(s.fileManager, s.logManager, s.bufferManager)
		if err != nil {
			return nil, fmt.Errorf("create transaction: %w", err)
		}
	}

	var result *ExecuteResult
	if matchSelect(sql) {
		result, err = s.execQuery(ctx, tx, sql)
		if err != nil {
			s.explicitTx = nil
			if rbErr := tx.Rollback(ctx); rbErr != nil {
				return nil, rbErr
			}
			return nil, err
		}
	} else {
		n, err := s.planner.ExecuteUpdate(ctx, sql, tx)
		if err != nil {
			s.explicitTx = nil
			if rbErr := tx.Rollback(ctx); rbErr != nil {
				return nil, rbErr
			}
			return nil, err
		}
		result = &ExecuteResult{Tag: updateTag(sql, n)}
	}

	if s.explicitTx != nil {
		return result, nil
	}
	return result, tx.Commit()
}

func (s *SimpleDB) execQuery(ctx context.Context, tx *dbtx.Transaction, sql string) (*ExecuteResult, error) {
	plan, err := s.planner.CreateQueryPlan(ctx, sql, tx)
	if err != nil {
		return nil, err
	}
	scan, err := plan.Open(ctx)
	if err != nil {
		return nil, err
	}
	defer scan.Close(ctx)

	schema := plan.Schema()
	fields := schema.Fields()

	fieldTypes := make([]int, len(fields))
	for i, f := range fields {
		fieldTypes[i] = schema.FieldType(f)
	}

	var rows [][]string
	for {
		ok, err := scan.Next(ctx)
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		row := make([]string, 0, len(fields))
		for i, f := range fields {
			switch fieldTypes[i] {
			case dbrecord.FieldTypeInt:
				v, err := scan.GetInt(ctx, f)
				if err != nil {
					return nil, err
				}
				row = append(row, strconv.Itoa(v))
			case dbrecord.FieldTypeString:
				v, err := scan.GetString(ctx, f)
				if err != nil {
					return nil, err
				}
				row = append(row, v)
			}
		}
		rows = append(rows, row)
	}

	return &ExecuteResult{
		Tag:        fmt.Sprintf("SELECT %d", len(rows)),
		Fields:     fields,
		FieldTypes: fieldTypes,
		Rows:       rows,
	}, nil
}

func updateTag(sql string, n int) string {
	lower := strings.ToLower(sql)
	switch {
	case strings.HasPrefix(lower, "insert"):
		return fmt.Sprintf("INSERT 0 %d", n)
	case strings.HasPrefix(lower, "update"):
		return fmt.Sprintf("UPDATE %d", n)
	case strings.HasPrefix(lower, "delete"):
		return fmt.Sprintf("DELETE %d", n)
	case strings.HasPrefix(lower, "create table"):
		return "CREATE TABLE"
	case strings.HasPrefix(lower, "create view"):
		return "CREATE VIEW"
	case strings.HasPrefix(lower, "create index"):
		return "CREATE INDEX"
	default:
		return fmt.Sprintf("UPDATE %d", n)
	}
}

func matchSelect(sql string) bool {
	return strings.HasPrefix(strings.ToLower(sql), "select")
}

func matchStartTx(sql string) bool {
	return strings.HasPrefix(strings.ToLower(sql), "start transaction")
}

func matchCommit(sql string) bool {
	return strings.HasPrefix(strings.ToLower(sql), "commit")
}

func matchRollback(sql string) bool {
	return strings.HasPrefix(strings.ToLower(sql), "rollback")
}
