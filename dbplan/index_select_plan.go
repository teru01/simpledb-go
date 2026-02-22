package dbplan

import (
	"context"
	"fmt"

	"github.com/teru01/simpledb-go/dbconstant"
	"github.com/teru01/simpledb-go/dbmetadata"
	"github.com/teru01/simpledb-go/dbquery"
)

type IndexSelectPlan struct {
	plan      dbquery.Plan
	indexInfo dbmetadata.IndexInfo
	value     dbconstant.Constant
}

func NewIndexSelectPlan(indexInfo dbmetadata.IndexInfo, value dbconstant.Constant) *IndexSelectPlan {
	return &IndexSelectPlan{
		indexInfo: indexInfo,
		value:     value,
	}
}

func (p *IndexSelectPlan) Open(ctx context.Context) (dbquery.Scan, error) {
	ts, err := p.plan.Open(ctx)
	if err != nil {
		return nil, fmt.Errorf("open plan: %w", err)
	}
	idx := p.indexInfo.Open()

	return nil, nil
}
