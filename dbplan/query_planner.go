package dbplan

import (
	"context"
	"fmt"

	"github.com/teru01/simpledb-go/dbmetadata"
	"github.com/teru01/simpledb-go/dbparse"
	"github.com/teru01/simpledb-go/dbquery"
	"github.com/teru01/simpledb-go/dbtx"
)

type BasicQueryPlanner struct {
	metadataManager *dbmetadata.MetadataManager
}

func NewQueryPlanner(metadataManager *dbmetadata.MetadataManager) *BasicQueryPlanner {
	return &BasicQueryPlanner{metadataManager: metadataManager}
}

// create plan from query data
// step1: create plan for each table or view
// step2: apply index select if possible (WHERE field = constant on indexed field)
// step3: create product plan for each pair of plans
// step4: create select plan
// step5: create project plan for the final plan
func (q *BasicQueryPlanner) CreatePlan(ctx context.Context, queryData *dbparse.QueryData, tx *dbtx.Transaction) (dbquery.Plan, error) {
	var plans []dbquery.Plan
	for _, tableName := range queryData.Tables() {
		viewDef, err := q.metadataManager.GetViewDef(ctx, tableName, tx)
		if err != nil {
			return nil, fmt.Errorf("get view def for plan: %w", err)
		}
		if viewDef != "" {
			// view exists. recursively executes plan
			p := dbparse.NewParser(viewDef)
			queryData, err := p.Query()
			if err != nil {
				return nil, fmt.Errorf("build query from view %q: %w", tableName, err)
			}
			vplan, err := q.CreatePlan(ctx, queryData, tx)
			if err != nil {
				return nil, fmt.Errorf("plan query: %w", err)
			}
			plans = append(plans, vplan)
		} else {
			tablePlan, err := NewTablePlan(ctx, tx, tableName, q.metadataManager)
			if err != nil {
				return nil, fmt.Errorf("create table plan for %q: %w", tableName, err)
			}
			// try to use index select (WHERE indexed_field = constant)
			var plan dbquery.Plan = tablePlan
			indexes, err := q.metadataManager.GetIndexInfo(ctx, tableName, tx)
			if err != nil {
				return nil, fmt.Errorf("get index info for %q: %w", tableName, err)
			}
			for fieldName, ii := range indexes {
				val := queryData.Predicate().EquatesWithConstant(fieldName)
				if val != nil {
					plan = NewIndexSelectPlan(tablePlan, *ii, val)
					break
				}
			}
			plans = append(plans, plan)
		}
	}

	plan := plans[0]
	if len(plans) > 1 {
		others := plans[1:]
		for _, pp := range others {
			if joined := q.tryIndexJoin(ctx, plan, pp, queryData, tx); joined != nil {
				plan = joined
			} else if joined := q.tryIndexJoin(ctx, pp, plan, queryData, tx); joined != nil {
				plan = joined
			} else {
				p1 := NewProductPlan(plan, pp)
				p2 := NewProductPlan(pp, plan)
				if p1.BlockAccessed() < p2.BlockAccessed() {
					plan = p1
				} else {
					plan = p2
				}
			}
		}
	}

	plan = NewSelectPlan(plan, queryData.Predicate())
	return NewProjectPlan(plan, queryData.Fields()), nil
}

// p1のカラムが、p2のテーブルにあるインデックス付きカラムとのjoinならIndexJoinを試みる
func (q *BasicQueryPlanner) tryIndexJoin(ctx context.Context, p1, p2 dbquery.Plan, queryData *dbparse.QueryData, tx *dbtx.Transaction) dbquery.Plan {
	tp2, ok := p2.(*TablePlan)
	if !ok {
		return nil
	}
	indexes, err := q.metadataManager.GetIndexInfo(ctx, tp2.tableName, tx)
	if err != nil {
		return nil
	}
	for fieldName, ii := range indexes {
		joinField := queryData.Predicate().EquatesWithFieldName(fieldName)
		if joinField != "" && p1.Schema().HasField(joinField) {
			return NewIndexJoinPlan(p1, p2, ii, joinField)
		}
	}
	return nil
}
