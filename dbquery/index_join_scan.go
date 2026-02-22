package dbquery

import (
	"context"
	"fmt"

	"github.com/teru01/simpledb-go/dbconstant"
	"github.com/teru01/simpledb-go/dbindex"
	"github.com/teru01/simpledb-go/dbrecord"
)

type IndexJoinScan struct {
	lhs       Scan
	index     dbindex.Index
	joinField string
	// インデックスによる検索対象
	rhs *dbrecord.TableScan
}

func NewIndexJoinScan(ctx context.Context, lhs Scan, index dbindex.Index, joinField string, rhs *dbrecord.TableScan) (*IndexJoinScan, error) {
	s := &IndexJoinScan{lhs: lhs, index: index, joinField: joinField, rhs: rhs}
	if err := s.SetStateToBeforeFirst(ctx); err != nil {
		return nil, fmt.Errorf("set state to before first: %w", err)
	}
	return s, nil
}

func (s *IndexJoinScan) SetStateToBeforeFirst(ctx context.Context) error {
	if err := s.lhs.SetStateToBeforeFirst(ctx); err != nil {
		return fmt.Errorf("set state to before first: %w", err)
	}
	if _, err := s.lhs.Next(ctx); err != nil {
		return fmt.Errorf("next: %w", err)
	}
	return s.ResetIndex(ctx)
}

// rhsを検索するインデックスのカーソルを先頭に戻す
func (s *IndexJoinScan) ResetIndex(ctx context.Context) error {
	searchKey, err := s.lhs.GetValue(ctx, s.joinField)
	if err != nil {
		return fmt.Errorf("get value: %w", err)
	}
	return s.index.BeforeFirst(ctx, searchKey)
}

func (s *IndexJoinScan) Next(ctx context.Context) (bool, error) {
	for {
		ok, err := s.index.Next(ctx)
		if err != nil {
			return false, fmt.Errorf("next index: %w", err)
		}
		if ok {
			rid, err := s.index.GetDataRID(ctx)
			if err != nil {
				return false, fmt.Errorf("get data rid: %w", err)
			}
			if err := s.rhs.MoveToRID(ctx, *rid); err != nil {
				return false, fmt.Errorf("move to %q: %w", rid, err)
			}
			return true, nil
		}
		exists, err := s.lhs.Next(ctx)
		if err != nil {
			return false, fmt.Errorf("next: %w", err)
		}
		if !exists {
			return false, nil
		}
		if err := s.ResetIndex(ctx); err != nil {
			return false, fmt.Errorf("reset index: %w", err)
		}
	}
}

func (s *IndexJoinScan) GetInt(ctx context.Context, fieldName string) (int, error) {
	if s.rhs.HasField(fieldName) {
		return s.rhs.GetInt(ctx, fieldName)
	}
	return s.lhs.GetInt(ctx, fieldName)
}

func (s *IndexJoinScan) GetString(ctx context.Context, fieldName string) (string, error) {
	if s.rhs.HasField(fieldName) {
		return s.rhs.GetString(ctx, fieldName)
	}
	return s.lhs.GetString(ctx, fieldName)
}

func (s *IndexJoinScan) GetValue(ctx context.Context, fieldName string) (dbconstant.Constant, error) {
	if s.rhs.HasField(fieldName) {
		return s.rhs.GetValue(ctx, fieldName)
	}
	return s.lhs.GetValue(ctx, fieldName)
}

func (s *IndexJoinScan) HasField(fieldName string) bool {
	return s.rhs.HasField(fieldName) || s.lhs.HasField(fieldName)
}

func (s *IndexJoinScan) Close(ctx context.Context) error {
	if err := s.index.Close(ctx); err != nil {
		return fmt.Errorf("close index: %w", err)
	}
	if err := s.rhs.Close(ctx); err != nil {
		return fmt.Errorf("close rhs: %w", err)
	}
	return s.lhs.Close(ctx)
}
