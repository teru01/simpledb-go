package dbquery

import (
	"context"

	"github.com/teru01/simpledb-go/dbrecord"
)

type Plan interface {
	Open(ctx context.Context) Scan
	BlockAccessed() int
	RecordsOutput() int
	DistinctValues(fieldName string) int
	Schema() *dbrecord.Schema
}
