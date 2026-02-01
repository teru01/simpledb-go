package dbmetadata

import (
	"context"
	"fmt"

	"github.com/teru01/simpledb-go/dbrecord"
	"github.com/teru01/simpledb-go/dbtx"
)

const (
	MaxViewDefinitionLength = 100
	ViewCatalogTableName    = "view_catalog"
)

type ViewManager struct {
	tableManager *TableManager
}

func NewViewManager(ctx context.Context, isNew bool, tableManager *TableManager, tx *dbtx.Transaction) (*ViewManager, error) {
	if isNew {
		schema := dbrecord.NewSchema()
		schema.AddStringField("viewname", MaxNameLength)
		schema.AddStringField("viewdef", MaxViewDefinitionLength)
		if err := tableManager.CreateTable(ctx, ViewCatalogTableName, schema, tx); err != nil {
			return nil, fmt.Errorf("init view manager: %w", err)
		}
	}
	return &ViewManager{
		tableManager: tableManager,
	}, nil
}

func (v *ViewManager) CreateView(ctx context.Context, viewName string, viewDef string, tx *dbtx.Transaction) error {
	layout, err := v.tableManager.GetLayout(ctx, ViewCatalogTableName, tx)
	if err != nil {
		return fmt.Errorf("get layout before creating view %q: %w", viewName, err)
	}
	ts, err := dbrecord.NewTableScan(ctx, tx, ViewCatalogTableName, layout)
	if err != nil {
		return fmt.Errorf("create table scan for view_catalog when creating view %q: %w", viewName, err)
	}
	if err := ts.Insert(ctx); err != nil {
		return fmt.Errorf("insert to view_catalog for %q: %w", viewName, err)
	}
	if err := ts.SetString(ctx, "viewname", viewName); err != nil {
		return fmt.Errorf("set viewname for %q: %w", viewName, err)
	}
	if err := ts.SetString(ctx, "viewdef", viewDef); err != nil {
		return fmt.Errorf("set viewdef for %q: %w", viewName, err)
	}
	if err := ts.Close(); err != nil {
		return fmt.Errorf("close table scan for view_catalog when creating view %q: %w", viewName, err)
	}
	return nil
}

// viewが見つからない時は空文字を返す
func (v *ViewManager) GetViewDef(ctx context.Context, viewName string, tx *dbtx.Transaction) (string, error) {
	layout, err := v.tableManager.GetLayout(ctx, ViewCatalogTableName, tx)
	if err != nil {
		return "", fmt.Errorf("get layout before get view %q: %w", viewName, err)
	}
	ts, err := dbrecord.NewTableScan(ctx, tx, ViewCatalogTableName, layout)
	if err != nil {
		return "", fmt.Errorf("create table scan for view_catalog when get view %q: %w", viewName, err)
	}
	viewDef := ""
	for {
		next, err := ts.Next(ctx)
		if err != nil {
			return "", fmt.Errorf("go next for %q: %w", "view_catalog", err)
		}
		if !next {
			break
		}
		viewNameValue, err := ts.GetString(ctx, "viewname")
		if err != nil {
			return "", fmt.Errorf("get viewname value: %w", err)
		}
		if viewNameValue == viewName {
			viewDefValue, err := ts.GetString(ctx, "viewdef")
			if err != nil {
				return "", fmt.Errorf("get viewdef value: %w", err)
			}
			viewDef = viewDefValue
			break
		}
	}
	if err := ts.Close(); err != nil {
		return "", fmt.Errorf("close view_catalog: %w", err)
	}
	return viewDef, nil
}
