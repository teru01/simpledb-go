package dbplan

import (
	"context"
	"errors"
	"fmt"

	"github.com/teru01/simpledb-go/dbmetadata"
	"github.com/teru01/simpledb-go/dbparse"
	"github.com/teru01/simpledb-go/dbquery"
	"github.com/teru01/simpledb-go/dbtx"
)

type BasicUpdatePlanner struct {
	metadataManager *dbmetadata.MetadataManager
}

func NewUpdatePlanner(metadataManager *dbmetadata.MetadataManager) *BasicUpdatePlanner {
	return &BasicUpdatePlanner{metadataManager: metadataManager}
}

func (u *BasicUpdatePlanner) ExecuteDelete(ctx context.Context, deleteData *dbparse.DeleteData, tx *dbtx.Transaction) (affectedRows int, err error) {
	p, err := NewTablePlan(ctx, tx, deleteData.TableName(), u.metadataManager)
	if err != nil {
		return 0, fmt.Errorf("create table plan for %q: %w", deleteData.TableName(), err)
	}
	scan, err := NewSelectPlan(p, deleteData.Predicate()).Open(ctx)
	if err != nil {
		return 0, fmt.Errorf("open table plan for %q: %w", deleteData.TableName(), err)
	}
	defer func() {
		if closeErr := scan.Close(); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}()
	affectedRows = 0
	for {
		next, err := scan.Next(ctx)
		if err != nil {
			return 0, fmt.Errorf("go next for %q: %w", deleteData.TableName(), err)
		}
		if !next {
			break
		}
		if err := scan.(dbquery.UpdateScan).Delete(ctx); err != nil {
			return 0, fmt.Errorf("delete for %q: %w", deleteData.TableName(), err)
		}
		affectedRows++
	}
	return affectedRows, nil
}

func (u *BasicUpdatePlanner) ExecuteModify(ctx context.Context, modifyData *dbparse.ModifyData, tx *dbtx.Transaction) (affectedRows int, err error) {
	p, err := NewTablePlan(ctx, tx, modifyData.TableName(), u.metadataManager)
	if err != nil {
		return 0, fmt.Errorf("create table plan for %q: %w", modifyData.TableName(), err)
	}
	scan, err := NewSelectPlan(p, modifyData.Predicate()).Open(ctx)
	if err != nil {
		return 0, fmt.Errorf("open table plan for %q: %w", modifyData.TableName(), err)
	}
	defer func() {
		if closeErr := scan.Close(); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}()
	affectedRows = 0

	for {
		next, err := scan.Next(ctx)
		if err != nil {
			return 0, fmt.Errorf("go next for %q: %w", modifyData.TableName(), err)
		}
		if !next {
			break
		}
		newVal, err := modifyData.NewVal().Evaluate(ctx, scan)
		if err != nil {
			return 0, fmt.Errorf("evaluate new value for %q: %w", modifyData.TableName(), err)
		}
		if err := scan.(dbquery.UpdateScan).SetValue(ctx, modifyData.FieldName(), newVal); err != nil {
			return 0, fmt.Errorf("delete for %q: %w", modifyData.TableName(), err)
		}
		affectedRows++
	}
	return affectedRows, nil
}

func (u *BasicUpdatePlanner) ExecuteInsert(ctx context.Context, insertData *dbparse.InsertData, tx *dbtx.Transaction) (affectedRows int, err error) {
	p, err := NewTablePlan(ctx, tx, insertData.TableName(), u.metadataManager)
	if err != nil {
		return 0, fmt.Errorf("create table plan for %q: %w", insertData.TableName(), err)
	}
	scan, err := p.Open(ctx)
	if err != nil {
		return 0, fmt.Errorf("open table plan for %q: %w", insertData.TableName(), err)
	}
	defer func() {
		if closeErr := scan.Close(); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}()

	if err = scan.(dbquery.UpdateScan).Insert(ctx); err != nil {
		return 0, fmt.Errorf("insert to %q: %w", insertData.TableName(), err)
	}

	for i, field := range insertData.Fields() {
		if err = scan.(dbquery.UpdateScan).SetValue(ctx, field, insertData.Vals()[i]); err != nil {
			return 0, fmt.Errorf("set value to %q: %w", field, err)
		}
	}
	return 1, nil
}

func (u *BasicUpdatePlanner) ExecuteCreateTable(ctx context.Context, createTableData *dbparse.CreateTableData, tx *dbtx.Transaction) (int, error) {
	if err := u.metadataManager.CreateTable(ctx, createTableData.TableName(), createTableData.Schema(), tx); err != nil {
		return 0, fmt.Errorf("create table for %q: %w", createTableData.TableName(), err)
	}
	return 0, nil
}

func (u *BasicUpdatePlanner) ExecuteCreateIndex(ctx context.Context, createIndexData *dbparse.CreateIndexData, tx *dbtx.Transaction) (int, error) {
	if err := u.metadataManager.CreateIndex(ctx, createIndexData.IndexName(), createIndexData.TableName(), createIndexData.FieldName(), tx); err != nil {
		return 0, fmt.Errorf("create index for %q: %w", createIndexData.IndexName(), err)
	}
	return 0, nil
}

func (u *BasicUpdatePlanner) ExecuteCreateView(ctx context.Context, createViewData *dbparse.CreateViewData, tx *dbtx.Transaction) (int, error) {
	if err := u.metadataManager.CreateView(ctx, createViewData.ViewName(), createViewData.Query().String(), tx); err != nil {
		return 0, fmt.Errorf("create view for %q: %w", createViewData.ViewName(), err)
	}
	return 0, nil
}
