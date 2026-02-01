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

type UpdatePlanner struct {
	metadataManager *dbmetadata.MetadataManager
}

func NewUpdatePlanner(metadataManager *dbmetadata.MetadataManager) *UpdatePlanner {
	return &UpdatePlanner{metadataManager: metadataManager}
}

func (u *UpdatePlanner) ExecuteDelete(ctx context.Context, deleteData *dbparse.DeleteData, tx *dbtx.Transaction) (affectedRows int, err error) {
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

func (u *UpdatePlanner) ExecuteModify(ctx context.Context, modifyData *dbparse.ModifyData, tx *dbtx.Transaction) (affectedRows int, err error) {
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

func (u *UpdatePlanner) ExecuteInsert(ctx context.Context, insertData *dbparse.InsertData, tx *dbtx.Transaction) (affectedRows int, err error) {
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
