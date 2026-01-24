package dbmetadata

import (
	"context"
	"errors"
	"fmt"

	"github.com/teru01/simpledb-go/dbrecord"
	"github.com/teru01/simpledb-go/dbtx"
)

const (
	IndexCatalogTableName = "index_catalog"
)

type IndexManger struct {
	layout       *dbrecord.Layout
	tableManager *TableManager
	statManager  *StatManager
}

type IndexInfo struct {
	indexName   string
	tableName   string
	fieldName   string
	tx          *dbtx.Transaction
	tableSchema *dbrecord.Schema
	indexLayout *dbrecord.Layout
	tableLayout *dbrecord.Layout
	statInfo    *StatInfo
}

func NewIndexManager(ctx context.Context, isNew bool, tableManager *TableManager, statManager *StatManager, tx *dbtx.Transaction) (*IndexManger, error) {
	if isNew {
		schema := dbrecord.NewSchema()
		schema.AddStringField("indexname", MaxNameLength)
		schema.AddStringField("tablename", MaxNameLength)
		schema.AddStringField("fieldname", MaxNameLength)
		if err := tableManager.CreateTable(ctx, IndexCatalogTableName, schema, tx); err != nil {
			return nil, fmt.Errorf("create %q table: %w", IndexCatalogTableName, err)
		}
	}
	l, err := tableManager.GetLayout(ctx, IndexCatalogTableName, tx)
	if err != nil {
		return nil, fmt.Errorf("get layout for %q: %w", IndexCatalogTableName, err)
	}
	return &IndexManger{
		layout:       l,
		tableManager: tableManager,
		statManager:  statManager,
	}, nil
}

func (i *IndexManger) CreateIndex(ctx context.Context, indexName string, tableName string, fieldName string, tx *dbtx.Transaction) error {
	ts, err := dbrecord.NewTableScan(ctx, tx, IndexCatalogTableName, i.layout)
	if err != nil {
		return fmt.Errorf("new table scan for %q: %w", IndexCatalogTableName, err)
	}
	if err := ts.Insert(ctx); err != nil {
		return fmt.Errorf("insert to %q: %w", IndexCatalogTableName, err)
	}
	if err := ts.SetString(ctx, "indexname", indexName); err != nil {
		return fmt.Errorf("set indexname for %q: %w", IndexCatalogTableName, err)
	}
	if err := ts.SetString(ctx, "tablename", tableName); err != nil {
		return fmt.Errorf("set tablename for %q: %w", IndexCatalogTableName, err)
	}
	if err := ts.SetString(ctx, "fieldname", fieldName); err != nil {
		return fmt.Errorf("set fieldname for %q: %w", IndexCatalogTableName, err)
	}
	if err := ts.Close(); err != nil {
		return fmt.Errorf("close table scan for %q: %w", IndexCatalogTableName, err)
	}
	return nil
}

func (i *IndexManger) GetIndexInfo(ctx context.Context, tableName string, tx *dbtx.Transaction) (indexInfos map[string]*IndexInfo, err error) {
	indexInfos = make(map[string]*IndexInfo)
	ts, err := dbrecord.NewTableScan(ctx, tx, IndexCatalogTableName, i.layout)
	if err != nil {
		return nil, fmt.Errorf("create table scan for %q: %w", IndexCatalogTableName, err)
	}
	defer func() {
		if closeErr := ts.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close table scan for %q: %w", IndexCatalogTableName, closeErr))
		}
	}()
	for {
		next, err := ts.Next(ctx)
		if err != nil {
			return nil, fmt.Errorf("go next for %q: %w", IndexCatalogTableName, err)
		}
		if !next {
			break
		}
		tableNameValue, err := ts.GetString(ctx, "tablename")
		if err != nil {
			return nil, fmt.Errorf("get tablename for %q: %w", IndexCatalogTableName, err)
		}
		if tableNameValue == tableName {
			indexName, err := ts.GetString(ctx, "indexname")
			if err != nil {
				return nil, fmt.Errorf("get indexname for %q: %w", IndexCatalogTableName, err)
			}
			fieldName, err := ts.GetString(ctx, "fieldname")
			if err != nil {
				return nil, fmt.Errorf("get fieldname for %q: %w", IndexCatalogTableName, err)
			}
			tableLayout, err := i.tableManager.GetLayout(ctx, tableName, tx)
			if err != nil {
				return nil, fmt.Errorf("get layout for %q: %w", tableName, err)
			}
			statInfo, err := i.statManager.GetStatInfo(ctx, tableName, tableLayout, tx)
			if err != nil {
				return nil, fmt.Errorf("get stat info for %q: %w", tableName, err)
			}

			targetTableLayout, err := i.tableManager.GetLayout(ctx, tableName, tx)
			if err != nil {
				return nil, fmt.Errorf("get layout for %q: %w", tableName, err)
			}
			indexInfo, err := NewIndexInfo(ctx, indexName, fieldName, tableLayout.Schema(), tx, statInfo, targetTableLayout)
			if err != nil {
				return nil, fmt.Errorf("new index info for %q: %w", indexName, err)
			}
			indexInfos[fieldName] = indexInfo
		}
	}
	return indexInfos, nil
}

func NewIndexInfo(ctx context.Context, indexName string, fieldName string, schema *dbrecord.Schema, tx *dbtx.Transaction, statInfo *StatInfo, tableLayout *dbrecord.Layout) (*IndexInfo, error) {
	ii := &IndexInfo{
		indexName:   indexName,
		fieldName:   fieldName,
		tableSchema: schema,
		tx:          tx,
		statInfo:    statInfo,
		tableLayout: tableLayout,
	}
	ii.indexLayout = ii.createIndexLayout()
	return ii, nil
}

func (i *IndexInfo) blockAccessed() (int, error) {
	recordsPerBlock := i.tx.BlockSize() / i.tableLayout.SlotSize()
	numBlocks := i.RecordsOutput() / recordsPerBlock
	return numBlocks, nil // needs to modify
}

func (i *IndexInfo) RecordsOutput() int {
	return i.statInfo.RecordsOutput() / i.statInfo.DistinctValues(i.fieldName)
}

func (i *IndexInfo) DistinctValues(fieldName string) int {
	if fieldName == i.fieldName {
		return 1
	}
	return i.statInfo.DistinctValues(fieldName)
}

func (i *IndexInfo) createIndexLayout() *dbrecord.Layout {
	schema := dbrecord.NewSchema()
	schema.AddIntField("block")
	schema.AddIntField("id")
	if i.tableLayout.Schema().FieldType(i.fieldName) == dbrecord.FieldTypeInt {
		schema.AddIntField("data_value")
	} else {
		schema.AddStringField("data_value", i.tableLayout.Schema().Length(i.fieldName))
	}
	return dbrecord.NewLayout(schema)
}
