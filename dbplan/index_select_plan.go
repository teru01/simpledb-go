package dbplan

import (
	"context"
	"fmt"

	"github.com/teru01/simpledb-go/dbconstant"
	"github.com/teru01/simpledb-go/dbmetadata"
	"github.com/teru01/simpledb-go/dbquery"
	"github.com/teru01/simpledb-go/dbrecord"
)

type IndexSelectPlan struct {
	plan      dbquery.Plan
	indexInfo dbmetadata.IndexInfo
	value     dbconstant.Constant
}

func NewIndexSelectPlan(plan dbquery.Plan, indexInfo dbmetadata.IndexInfo, value dbconstant.Constant) *IndexSelectPlan {
	return &IndexSelectPlan{
		plan:      plan,
		indexInfo: indexInfo,
		value:     value,
	}
}

func (p *IndexSelectPlan) Open(ctx context.Context) (dbquery.Scan, error) {
	s, err := p.plan.Open(ctx)
	if err != nil {
		return nil, fmt.Errorf("open plan: %w", err)
	}
	ts, ok := s.(*dbrecord.TableScan)
	if !ok {
		return nil, fmt.Errorf("can't open index other than table scan")
	}
	idx, err := p.indexInfo.Open(ctx)
	if err != nil {
		return nil, fmt.Errorf("open index: %w", err)
	}
	return dbquery.NewIndexSelectScan(ctx, ts, idx, p.value)
}

func (p *IndexSelectPlan) BlockAccessed() int {
	return p.indexInfo.BlockAccessed() + p.RecordsOutput()
}

func (p *IndexSelectPlan) RecordsOutput() int {
	return p.indexInfo.RecordsOutput()
}

func (p *IndexSelectPlan) DistinctValues(fieldName string) int {
	return p.indexInfo.DistinctValues(fieldName)
}

func (p *IndexSelectPlan) Schema() *dbrecord.Schema {
	return p.plan.Schema()
}
