package dbplan

import (
	"context"
	"fmt"

	"github.com/teru01/simpledb-go/dbparse"
	"github.com/teru01/simpledb-go/dbquery"
	"github.com/teru01/simpledb-go/dbtx"
)

type QueryPlanner interface {
	CreatePlan(ctx context.Context, data *dbparse.QueryData, tx *dbtx.Transaction) (dbquery.Plan, error)
}

type UpdatePlanner interface {
	ExecuteDelete(ctx context.Context, data *dbparse.DeleteData, tx *dbtx.Transaction) (int, error)
	ExecuteModify(ctx context.Context, data *dbparse.ModifyData, tx *dbtx.Transaction) (int, error)
	ExecuteInsert(ctx context.Context, data *dbparse.InsertData, tx *dbtx.Transaction) (int, error)
	ExecuteCreateTable(ctx context.Context, data *dbparse.CreateTableData, tx *dbtx.Transaction) (int, error)
	ExecuteCreateIndex(ctx context.Context, data *dbparse.CreateIndexData, tx *dbtx.Transaction) (int, error)
	ExecuteCreateView(ctx context.Context, data *dbparse.CreateViewData, tx *dbtx.Transaction) (int, error)
}

type Planner struct {
	queryPlanner  QueryPlanner
	updatePlanner UpdatePlanner
}

func NewPlanner(queryPlanner QueryPlanner, updatePlanner UpdatePlanner) *Planner {
	return &Planner{queryPlanner: queryPlanner, updatePlanner: updatePlanner}
}

func (p *Planner) CreateQueryPlan(ctx context.Context, sql string, tx *dbtx.Transaction) (dbquery.Plan, error) {
	parser := dbparse.NewParser(sql)
	queryData, err := parser.Query()
	if err != nil {
		return nil, fmt.Errorf("parse query: %w", err)
	}
	return p.queryPlanner.CreatePlan(ctx, queryData, tx)
}

func (p *Planner) ExecuteUpdate(ctx context.Context, sql string, tx *dbtx.Transaction) (int, error) {
	parser := dbparse.NewParser(sql)
	updateData, err := parser.UpdateCmd()
	if err != nil {
		return 0, fmt.Errorf("parse update: %w", err)
	}
	switch updateData := updateData.(type) {
	case *dbparse.DeleteData:
		return p.updatePlanner.ExecuteDelete(ctx, updateData, tx)
	case *dbparse.ModifyData:
		return p.updatePlanner.ExecuteModify(ctx, updateData, tx)
	case *dbparse.InsertData:
		return p.updatePlanner.ExecuteInsert(ctx, updateData, tx)
	case *dbparse.CreateTableData:
		return p.updatePlanner.ExecuteCreateTable(ctx, updateData, tx)
	case *dbparse.CreateIndexData:
		return p.updatePlanner.ExecuteCreateIndex(ctx, updateData, tx)
	case *dbparse.CreateViewData:
		return p.updatePlanner.ExecuteCreateView(ctx, updateData, tx)
	default:
		return 0, fmt.Errorf("unexpected update data: %T", updateData)
	}
}
