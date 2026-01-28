package dbquery

import (
	"context"
	"fmt"

	"github.com/teru01/simpledb-go/dbconstant"
	"github.com/teru01/simpledb-go/dbrecord"
)

type SelectScan struct {
	scan      Scan
	predicate Predicate
}

func NewSelectScan(scan Scan, predicate Predicate) *SelectScan {
	return &SelectScan{
		scan:      scan,
		predicate: predicate,
	}
}

func (s *SelectScan) SetStateToBeforeFirst(ctx context.Context) error {
	return s.SetStateToBeforeFirst(ctx)
}

func (s *SelectScan) Next(ctx context.Context) (bool, error) {
	for {
		ok, err := s.scan.Next(ctx)
		if err != nil {
			return false, fmt.Errorf("next: %w", err)
		}
		if !ok {
			break
		}
		if s.predicate.IsSatisfied(s.scan) {
			return true, nil
		}
	}
	return false, nil
}

func (s *SelectScan) GetInt(ctx context.Context, fieldName string) (int, error) {
	return s.scan.GetInt(ctx, fieldName)
}

func (s *SelectScan) GetString(ctx context.Context, fieldName string) (string, error) {
	return s.scan.GetString(ctx, fieldName)
}

func (s *SelectScan) GetVal(ctx context.Context, fieldName string) (dbconstant.Constant, error) {
	return s.scan.GetVal(ctx, fieldName)
}

func (s *SelectScan) HasField(fieldName string) bool {
	return s.scan.HasField(fieldName)
}

func (s *SelectScan) Close() error {
	return s.scan.Close()
}

func (s *SelectScan) SetInt(ctx context.Context, fieldName string, value int) error {
	return s.scan.(UpdateScan).SetInt(ctx, fieldName, value)
}

func (s *SelectScan) SetString(ctx context.Context, fieldName string, value string) error {
	return s.scan.(UpdateScan).SetString(ctx, fieldName, value)
}

func (s *SelectScan) SetValue(ctx context.Context, fieldName string, value dbconstant.Constant) error {
	return s.scan.(UpdateScan).SetValue(ctx, fieldName, value)
}

func (s *SelectScan) Insert(ctx context.Context) error {
	return s.scan.(UpdateScan).Insert(ctx)
}

func (s *SelectScan) Delete(ctx context.Context) error {
	return s.scan.(UpdateScan).Delete(ctx)
}

func (s *SelectScan) MoveToRID(ctx context.Context, rID dbrecord.RID) error {
	return s.scan.(UpdateScan).MoveToRID(ctx, rID)
}

func (s *SelectScan) RID() *dbrecord.RID {
	return s.scan.(UpdateScan).RID()
}
