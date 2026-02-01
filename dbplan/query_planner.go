package dbplan

import (
	"context"
	"fmt"

	"github.com/teru01/simpledb-go/dbmetadata"
	"github.com/teru01/simpledb-go/dbparse"
	"github.com/teru01/simpledb-go/dbquery"
	"github.com/teru01/simpledb-go/dbtx"
)

type QueryPlanner struct {
	metadataManager *dbmetadata.MetadataManager
}

func NewQueryPlanner(metadataManager *dbmetadata.MetadataManager) *QueryPlanner {
	return &QueryPlanner{metadataManager: metadataManager}
}

// create plan from query data
// step1: create plan for each table or view
// step2: create product plan for each pair of plans
// step3: create select plan
// step4: create project plan for the final plan
func (q *QueryPlanner) CreatePlan(ctx context.Context, queryData *dbparse.QueryData, tx *dbtx.Transaction) (dbquery.Plan, error) {
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
			// no views
			tablePlan, err := NewTablePlan(ctx, tx, tableName, q.metadataManager)
			if err != nil {
				return nil, fmt.Errorf("create table plan for %q: %w", tableName, err)
			}
			plans = append(plans, tablePlan)
		}
	}

	plan := plans[0]
	if len(plans) > 1 {
		others := plans[1:]
		for _, pp := range others {
			p1 := NewProductPlan(plan, pp)
			p2 := NewProductPlan(pp, plan)
			if p1.BlockAccessed() < p2.BlockAccessed() {
				plan = p1
			} else {
				plan = p2
			}
		}
	}

	plan = NewSelectPlan(plan, queryData.Predicate())
	return NewProjectPlan(plan, queryData.Fields()), nil
}
