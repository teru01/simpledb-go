package dbplan

import (
	"context"
	"fmt"

	"github.com/teru01/simpledb-go/dbmetadata"
	"github.com/teru01/simpledb-go/dbquery"
	"github.com/teru01/simpledb-go/dbrecord"
)

type IndexJoinPlan struct {
	p1, p2    dbquery.Plan
	indexInfo *dbmetadata.IndexInfo
	joinField string
	schema    *dbrecord.Schema
}

// p2に対してindexが効いている必要がある
func NewIndexJoinPlan(p1 dbquery.Plan, p2 dbquery.Plan, indexInfo *dbmetadata.IndexInfo, joinField string) *IndexJoinPlan {
	schema := dbrecord.NewSchema()
	schema.AddAll(p1.Schema())
	schema.AddAll(p2.Schema())
	return &IndexJoinPlan{p1: p1, p2: p2, indexInfo: indexInfo, joinField: joinField, schema: schema}
}

func (p *IndexJoinPlan) Open(ctx context.Context) (dbquery.Scan, error) {
	s1, err := p.p1.Open(ctx)
	if err != nil {
		return nil, fmt.Errorf("open plan1: %w", err)
	}
	s2Opened, err := p.p2.Open(ctx)
	if err != nil {
		return nil, fmt.Errorf("open plan2: %w", err)
	}
	s2, ok := s2Opened.(*dbrecord.TableScan)
	if !ok {
		return nil, fmt.Errorf("s2 must be table scan: %w", err)
	}
	idx, err := p.indexInfo.Open(ctx)
	if err != nil {
		return nil, fmt.Errorf("open index: %w", err)
	}
	return dbquery.NewIndexJoinScan(ctx, s1, idx, p.joinField, s2)
}

func (p *IndexJoinPlan) BlockAccessed() int {
	return p.p1.BlockAccessed() + p.p1.RecordsOutput()*p.indexInfo.BlockAccessed() + p.RecordsOutput()
}

func (p *IndexJoinPlan) RecordsOutput() int {
	return p.p1.RecordsOutput() * p.indexInfo.RecordsOutput()
}

func (p *IndexJoinPlan) DistinctValues(fieldName string) int {
	if p.p1.Schema().HasField(fieldName) {
		return p.p1.DistinctValues(fieldName)
	}
	return p.p2.DistinctValues(fieldName)
}

func (p *IndexJoinPlan) Schema() *dbrecord.Schema {
	return p.schema
}
