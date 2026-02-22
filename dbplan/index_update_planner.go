package dbplan

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/teru01/simpledb-go/dbmetadata"
	"github.com/teru01/simpledb-go/dbparse"
	"github.com/teru01/simpledb-go/dbquery"
	"github.com/teru01/simpledb-go/dbtx"
)

type IndexUpdatePlanner struct {
	metadataManager *dbmetadata.MetadataManager
}

func NewIndexUpdatePlanner(metadataManager *dbmetadata.MetadataManager) *IndexUpdatePlanner {
	return &IndexUpdatePlanner{metadataManager: metadataManager}
}

func (p *IndexUpdatePlanner) ExecuteInsert(ctx context.Context, data *dbparse.InsertData, tx *dbtx.Transaction) (n int, err error) {
	tableName := data.TableName()
	plan, err := NewTablePlan(ctx, tx, tableName, p.metadataManager)
	if err != nil {
		return 0, fmt.Errorf("init table plan: %w", err)
	}
	s, err := plan.Open(ctx)
	if err != nil {
		return 0, fmt.Errorf("open plan: %w", err)
	}
	scan := s.(dbquery.UpdateScan)
	if err := scan.Insert(ctx); err != nil {
		return 0, fmt.Errorf("update scan: %w", err)
	}
	defer func() {
		if closeErr := scan.Close(ctx); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}()

	indexes, err := p.metadataManager.GetIndexInfo(ctx, tableName, tx)
	if err != nil {
		return 0, fmt.Errorf("get index info: %w", err)
	}
	for i, fieldName := range data.Fields() {
		if err = scan.SetValue(ctx, fieldName, data.Vals()[i]); err != nil {
			return 0, fmt.Errorf("set value to %q: %w", fieldName, err)
		}
		ii, ok := indexes[fieldName]
		if !ok {
			continue
		}
		slog.Debug("insert %q = %q", fieldName, data.Vals()[i])
		index, err := ii.Open(ctx)
		if err != nil {
			return 0, fmt.Errorf("open index: %w", err)
		}
		if err := index.Insert(ctx, data.Vals()[i], *scan.RID()); err != nil {
			return 0, fmt.Errorf("insert index: %w", err)
		}
		if err := index.Close(ctx); err != nil {
			return 0, fmt.Errorf("close index: %w", err)
		}
	}
	return 1, nil
}

func (p *IndexUpdatePlanner) ExecuteDelete(ctx context.Context, deleteData *dbparse.DeleteData, tx *dbtx.Transaction) (affectedRows int, err error) {
	plan, err := NewTablePlan(ctx, tx, deleteData.TableName(), p.metadataManager)
	if err != nil {
		return 0, fmt.Errorf("create table plan for %q: %w", deleteData.TableName(), err)
	}
	s, err := NewSelectPlan(plan, deleteData.Predicate()).Open(ctx)
	if err != nil {
		return 0, fmt.Errorf("open table plan for %q: %w", deleteData.TableName(), err)
	}

	scan := s.(dbquery.UpdateScan)
	defer func() {
		if closeErr := scan.Close(ctx); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}()
	indexes, err := p.metadataManager.GetIndexInfo(ctx, deleteData.TableName(), tx)
	if err != nil {
		return 0, fmt.Errorf("get index info: %w", err)
	}
	affectedRows = 0
	for {
		next, err := scan.Next(ctx)
		if err != nil {
			return 0, fmt.Errorf("go next for %q: %w", deleteData.TableName(), err)
		}
		if !next {
			break
		}
		if err := scan.Delete(ctx); err != nil {
			return 0, fmt.Errorf("delete for %q: %w", deleteData.TableName(), err)
		}
		affectedRows++

		for field, ii := range indexes {
			index, err := ii.Open(ctx)
			if err != nil {
				return affectedRows, fmt.Errorf("open: %w", err)
			}
			val, err := scan.GetValue(ctx, field)
			if err != nil {
				return affectedRows, fmt.Errorf("get value for %q: %w", field, err)
			}
			if err := index.Delete(ctx, val, *scan.RID()); err != nil {
				return affectedRows, fmt.Errorf("delete index: %w", err)
			}
			if err := index.Close(ctx); err != nil {
				return affectedRows, fmt.Errorf("close index: %w", err)
			}
		}
	}
	return affectedRows, nil
}

func (p *IndexUpdatePlanner) ExecuteModify(ctx context.Context, modifyData *dbparse.ModifyData, tx *dbtx.Transaction) (affectedRows int, err error) {
	plan, err := NewTablePlan(ctx, tx, modifyData.TableName(), p.metadataManager)
	if err != nil {
		return 0, fmt.Errorf("create table plan for %q: %w", modifyData.TableName(), err)
	}
	s, err := NewSelectPlan(plan, modifyData.Predicate()).Open(ctx)
	if err != nil {
		return 0, fmt.Errorf("open table plan for %q: %w", modifyData.TableName(), err)
	}
	scan := s.(dbquery.UpdateScan)
	defer func() {
		if closeErr := scan.Close(ctx); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}()

	indexes, err := p.metadataManager.GetIndexInfo(ctx, modifyData.TableName(), tx)
	if err != nil {
		return 0, fmt.Errorf("get index info: %w", err)
	}
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
		if err := scan.SetValue(ctx, modifyData.FieldName(), newVal); err != nil {
			return 0, fmt.Errorf("delete for %q: %w", modifyData.TableName(), err)
		}

		for field, ii := range indexes {
			index, err := ii.Open(ctx)
			if err != nil {
				return affectedRows, fmt.Errorf("open: %w", err)
			}
			oldVal, err := scan.GetValue(ctx, field)
			if err != nil {
				return affectedRows, fmt.Errorf("get value for %q: %w", field, err)
			}

			if err := index.Delete(ctx, oldVal, *scan.RID()); err != nil {
				return affectedRows, fmt.Errorf("delete index: %w", err)
			}
			if err := index.Insert(ctx, newVal, *scan.RID()); err != nil {
				return affectedRows, fmt.Errorf("insert index: %w", err)
			}
			if err := index.Close(ctx); err != nil {
				return affectedRows, fmt.Errorf("close index: %w", err)
			}
		}
		affectedRows++
	}
	return affectedRows, nil
}

func (p *IndexUpdatePlanner) CreatePlan(ctx context.Context, queryData *dbparse.QueryData, tx *dbtx.Transaction) (dbquery.Plan, error) {
	return nil, nil
}
