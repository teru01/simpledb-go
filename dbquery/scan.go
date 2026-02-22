package dbquery

import (
	"context"

	"github.com/teru01/simpledb-go/dbconstant"
	"github.com/teru01/simpledb-go/dbrecord"
)

type Scan interface {
	SetStateToBeforeFirst(ctx context.Context) error
	Next(ctx context.Context) (bool, error)
	GetInt(ctx context.Context, fieldName string) (int, error)
	GetString(ctx context.Context, fieldName string) (string, error)
	GetValue(ctx context.Context, fieldName string) (dbconstant.Constant, error)
	HasField(fieldName string) bool
	Close(ctx context.Context) error
}

type UpdateScan interface {
	Scan
	SetInt(ctx context.Context, fieldName string, value int) error
	SetString(ctx context.Context, fieldName string, value string) error
	SetValue(ctx context.Context, fieldName string, value dbconstant.Constant) error
	Insert(ctx context.Context) error
	Delete(ctx context.Context) error
	MoveToRID(ctx context.Context, rID dbrecord.RID) error
	RID() *dbrecord.RID
}
