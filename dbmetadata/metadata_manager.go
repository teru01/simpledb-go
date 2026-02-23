package dbmetadata

import (
	"context"
	"fmt"

	"github.com/teru01/simpledb-go/dbrecord"
	"github.com/teru01/simpledb-go/dbtx"
)

type MetadataManager struct {
	tableManager *TableManager
	statManager  *StatManager
	indexManager *IndexManager
	viewManager  *ViewManager
}

func NewMetadataManager(ctx context.Context, isNew bool, tx *dbtx.Transaction) (*MetadataManager, error) {
	tableManager, err := NewTableManager(ctx, isNew, tx)
	if err != nil {
		return nil, fmt.Errorf("new table manager: %w", err)
	}
	statManager, err := NewStatManager(ctx, tableManager, tx)
	if err != nil {
		return nil, fmt.Errorf("new stat manager: %w", err)
	}
	indexManager, err := NewIndexManager(ctx, tableManager, statManager, tx)
	if err != nil {
		return nil, fmt.Errorf("new index manager: %w", err)
	}
	viewManager, err := NewViewManager(ctx, tableManager, tx)
	if err != nil {
		return nil, fmt.Errorf("new view manager: %w", err)
	}
	return &MetadataManager{
		tableManager: tableManager,
		statManager:  statManager,
		indexManager: indexManager,
		viewManager:  viewManager,
	}, nil
}

func (m *MetadataManager) CreateTable(ctx context.Context, tableName string, schema *dbrecord.Schema, tx *dbtx.Transaction) error {
	return m.tableManager.CreateTable(ctx, tableName, schema, tx)
}

func (m *MetadataManager) GetLayout(ctx context.Context, tableName string, tx *dbtx.Transaction) (*dbrecord.Layout, error) {
	return m.tableManager.GetLayout(ctx, tableName, tx)
}

func (m *MetadataManager) GetStatInfo(ctx context.Context, tableName string, layout *dbrecord.Layout, tx *dbtx.Transaction) (*StatInfo, error) {
	return m.statManager.GetStatInfo(ctx, tableName, layout, tx)
}

func (m *MetadataManager) CreateIndex(ctx context.Context, indexName string, tableName string, fieldName string, tx *dbtx.Transaction) error {
	return m.indexManager.CreateIndex(ctx, indexName, tableName, fieldName, tx)
}

func (m *MetadataManager) GetIndexInfo(ctx context.Context, tableName string, tx *dbtx.Transaction) (indexInfos map[string]*IndexInfo, err error) {
	return m.indexManager.GetIndexInfo(ctx, tableName, tx)
}

func (m *MetadataManager) CreateView(ctx context.Context, viewName string, viewDef string, tx *dbtx.Transaction) error {
	return m.viewManager.CreateView(ctx, viewName, viewDef, tx)
}

func (m *MetadataManager) GetViewDef(ctx context.Context, viewName string, tx *dbtx.Transaction) (string, error) {
	return m.viewManager.GetViewDef(ctx, viewName, tx)
}
