package dbquery

import (
	"context"
	"fmt"

	"github.com/teru01/simpledb-go/dbconstant"
)

type ProjectScan struct {
	scan      Scan
	fieldList []string
}

func NewProjectScan(scan Scan, fieldList []string) *ProjectScan {
	return &ProjectScan{
		scan:      scan,
		fieldList: fieldList,
	}
}

func (s *ProjectScan) SetStateToBeforeFirst(ctx context.Context) error {
	return s.scan.SetStateToBeforeFirst(ctx)
}

func (s *ProjectScan) Next(ctx context.Context) (bool, error) {
	return s.scan.Next(ctx)
}

func (s *ProjectScan) GetInt(ctx context.Context, fieldName string) (int, error) {
	if !s.scan.HasField(fieldName) {
		return 0, fmt.Errorf("field %q not found", fieldName)
	}
	return s.scan.GetInt(ctx, fieldName)
}

func (s *ProjectScan) GetString(ctx context.Context, fieldName string) (string, error) {
	if !s.scan.HasField(fieldName) {
		return "", fmt.Errorf("field %q not found", fieldName)
	}
	return s.scan.GetString(ctx, fieldName)
}

func (s *ProjectScan) GetVal(ctx context.Context, fieldName string) (dbconstant.Constant, error) {
	if !s.scan.HasField(fieldName) {
		return nil, fmt.Errorf("field %q not found", fieldName)
	}
	return s.scan.GetVal(ctx, fieldName)
}

func (s *ProjectScan) HasField(fieldName string) bool {
	return s.scan.HasField(fieldName)
}

func (s *ProjectScan) Close() error {
	return s.scan.Close()
}
