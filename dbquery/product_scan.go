package dbquery

import (
	"context"
	"fmt"

	"github.com/teru01/simpledb-go/dbconstant"
)

type ProductScan struct {
	scan1 Scan
	scan2 Scan
}

func NewProductScan(scan1 Scan, scan2 Scan) *ProductScan {
	return &ProductScan{scan1: scan1, scan2: scan2}
}

// scan1は1番目に, scan2はheadにセットする
// Next(ctx)参照
func (s *ProductScan) SetStateToBeforeFirst(ctx context.Context) error {
	if err := s.scan1.SetStateToBeforeFirst(ctx); err != nil {
		return fmt.Errorf("set state to before first: %w", err)
	}
	_, err := s.scan1.Next(ctx)
	if err != nil {
		return fmt.Errorf("next scan1: %w", err)
	}
	return s.scan2.SetStateToBeforeFirst(ctx)
}

// s1を固定し、s2をすべて読み込んだら、s1を1つ進める
// s1とs2のレコードをすべて読み込んだら、falseを返す
func (s *ProductScan) Next(ctx context.Context) (bool, error) {
	s2ok, err := s.scan2.Next(ctx)
	if err != nil {
		return false, fmt.Errorf("next: %w", err)
	}
	if s2ok {
		return true, nil
	}
	s.scan2.SetStateToBeforeFirst(ctx)
	s2ok, err = s.scan2.Next(ctx)
	if err != nil {
		return false, fmt.Errorf("next: %w", err)
	}
	s1ok, err := s.scan1.Next(ctx)
	if err != nil {
		return false, fmt.Errorf("next: %w", err)
	}
	return s1ok && s2ok, nil
}

func (s *ProductScan) GetInt(ctx context.Context, fieldName string) (int, error) {
	if s.scan1.HasField(fieldName) {
		return s.scan1.GetInt(ctx, fieldName)
	}
	if s.scan2.HasField(fieldName) {
		return s.scan2.GetInt(ctx, fieldName)
	}
	return 0, fmt.Errorf("field %q not found", fieldName)
}

func (s *ProductScan) GetString(ctx context.Context, fieldName string) (string, error) {
	if s.scan1.HasField(fieldName) {
		return s.scan1.GetString(ctx, fieldName)
	}
	if s.scan2.HasField(fieldName) {
		return s.scan2.GetString(ctx, fieldName)
	}
	return "", fmt.Errorf("field %q not found", fieldName)
}

func (s *ProductScan) GetValue(ctx context.Context, fieldName string) (dbconstant.Constant, error) {
	if s.scan1.HasField(fieldName) {
		return s.scan1.GetValue(ctx, fieldName)
	}
	if s.scan2.HasField(fieldName) {
		return s.scan2.GetValue(ctx, fieldName)
	}
	return nil, fmt.Errorf("field %q not found", fieldName)
}

func (s *ProductScan) HasField(fieldName string) bool {
	return s.scan1.HasField(fieldName) || s.scan2.HasField(fieldName)
}

func (s *ProductScan) Close() error {
	if err := s.scan1.Close(); err != nil {
		return fmt.Errorf("close scan1: %w", err)
	}
	if err := s.scan2.Close(); err != nil {
		return fmt.Errorf("close scan2: %w", err)
	}
	return nil
}
