package dbindex

import (
	"context"

	"github.com/teru01/simpledb-go/dbconstant"
	"github.com/teru01/simpledb-go/dbrecord"
)

type Index interface {
	BeforeFirst(ctx context.Context, searchKey dbconstant.Constant) error
	Next(ctx context.Context) (bool, error)
	GetDataRID(ctx context.Context) (dbrecord.RID, error)
	Insert(ctx context.Context, dataRID dbrecord.RID) error
	Delete(ctx context.Context, dataRID dbrecord.RID) error
	Close() error
}
