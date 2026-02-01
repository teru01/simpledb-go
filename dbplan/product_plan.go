package dbplan

import (
	"context"

	"github.com/teru01/simpledb-go/dbquery"
	"github.com/teru01/simpledb-go/dbrecord"
)

type ProductPlan struct {
	plan1  dbquery.Plan
	plan2  dbquery.Plan
	schema *dbrecord.Schema
}

func NewProductPlan(plan1 dbquery.Plan, plan2 dbquery.Plan) *ProductPlan {
	s := dbrecord.NewSchema()
	s.AddAll(plan1.Schema())
	s.AddAll(plan2.Schema())
	return &ProductPlan{plan1: plan1, plan2: plan2, schema: s}
}

func (p *ProductPlan) Open(ctx context.Context) dbquery.Scan {
	return dbquery.NewProductScan(p.plan1.Open(ctx), p.plan2.Open(ctx))
}

func (p *ProductPlan) BlockAccessed() int {
	return p.plan1.BlockAccessed() + p.plan1.RecordsOutput()*p.plan2.BlockAccessed()
}

func (p *ProductPlan) RecordsOutput() int {
	return p.plan1.RecordsOutput() * p.plan2.RecordsOutput()
}

func (p *ProductPlan) DistinctValues(fieldName string) int {
	if p.plan1.Schema().HasField(fieldName) {
		return p.plan1.DistinctValues(fieldName)
	}
	return p.plan2.DistinctValues(fieldName)
}

func (p *ProductPlan) Schema() *dbrecord.Schema {
	return p.schema
}
