package dbplan

import (
	"context"
	"fmt"

	"github.com/teru01/simpledb-go/dbmetadata"
	"github.com/teru01/simpledb-go/dbquery"
	"github.com/teru01/simpledb-go/dbrecord"
	"github.com/teru01/simpledb-go/dbtx"
)

type TablePlan struct {
	tx        *dbtx.Transaction
	tableName string
	layout    *dbrecord.Layout
	statInfo  *dbmetadata.StatInfo
}

func NewTablePlan(ctx context.Context, tx *dbtx.Transaction, tableName string, metadataManager *dbmetadata.MetadataManager) (*TablePlan, error) {
	layout, err := metadataManager.GetLayout(ctx, tableName, tx)
	if err != nil {
		return nil, fmt.Errorf("get layout for %q: %w", tableName, err)
	}
	statInfo, err := metadataManager.GetStatInfo(ctx, tableName, layout, tx)
	if err != nil {
		return nil, fmt.Errorf("get stat info for %q: %w", tableName, err)
	}
	return &TablePlan{tx: tx, tableName: tableName, layout: layout, statInfo: statInfo}, nil
}

func (t *TablePlan) Open(ctx context.Context) (dbquery.Scan, error) {
	return dbrecord.NewTableScan(ctx, t.tx, t.tableName, t.layout)
}

func (t *TablePlan) BlockAccessed() int {
	return t.statInfo.BlockAccessed()
}

func (t *TablePlan) RecordsOutput() int {
	return t.statInfo.RecordsOutput()
}

func (t *TablePlan) DistinctValues(fieldName string) int {
	return t.statInfo.DistinctValues(fieldName)
}

func (t *TablePlan) Schema() *dbrecord.Schema {
	return t.layout.Schema()
}
