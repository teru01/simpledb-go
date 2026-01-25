package dbmetadata

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"

	"github.com/teru01/simpledb-go/dbrecord"
	"github.com/teru01/simpledb-go/dbtx"
)

const (
	statRefreshInterval = 100
)

type StatInfo struct {
	numBlocks         int
	numRecords        int
	distinctValuesMap map[string]int
}

type StatManager struct {
	mu           sync.Mutex
	tableManager *TableManager
	tableStats   map[string]*StatInfo
	numCalls     int
}

func NewStatManager(ctx context.Context, tableManager *TableManager, tx *dbtx.Transaction) (*StatManager, error) {
	s := &StatManager{
		tableManager: tableManager,
		tableStats:   make(map[string]*StatInfo),
		numCalls:     0,
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.refreshStatisticsLocked(ctx, tx); err != nil {
		return nil, fmt.Errorf("refresh stats: %w", err)
	}
	return s, nil
}

func (s *StatManager) GetStatInfo(ctx context.Context, tableName string, layout *dbrecord.Layout, tx *dbtx.Transaction) (*StatInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.numCalls++
	if s.numCalls > statRefreshInterval {
		if err := s.refreshStatisticsLocked(ctx, tx); err != nil {
			return nil, fmt.Errorf("refresh stats: %w", err)
		}
	}
	si, ok := s.tableStats[tableName]
	var err error
	if !ok {
		si, err = s.calcTableStatsLocked(ctx, tableName, layout, tx)
		if err != nil {
			return nil, fmt.Errorf("calc table stats for %q: %w", tableName, err)
		}
	}
	return si, nil
}

func (s *StatManager) refreshStatisticsLocked(ctx context.Context, tx *dbtx.Transaction) (err error) {
	tableStats := make(map[string]*StatInfo)
	s.numCalls = 0

	layout, err := s.tableManager.GetLayout(ctx, TableCatalogTableName, tx)
	if err != nil {
		return fmt.Errorf("get layout while refresh stats: %w", err)
	}
	ts, err := dbrecord.NewTableScan(ctx, tx, TableCatalogTableName, layout)
	if err != nil {
		return fmt.Errorf("new table scan for table_catalog: %w", err)
	}
	defer func() {
		if closeErr := ts.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close table scan for table_catalog: %w", err))
		}
	}()
	for {
		next, err := ts.Next(ctx)
		if err != nil {
			return fmt.Errorf("go next for table_catalog: %w", err)
		}
		if !next {
			break
		}
		tableName, err := ts.GetString(ctx, "tablename")
		if err != nil {
			return fmt.Errorf("get tablename from %q: %w", TableCatalogTableName, err)
		}
		tableLayout, err := s.tableManager.GetLayout(ctx, tableName, tx)
		if err != nil {
			return fmt.Errorf("get layout for %q: %w", tableName, err)
		}
		stats, err := s.calcTableStatsLocked(ctx, tableName, tableLayout, tx)
		if err != nil {
			return fmt.Errorf("get stats for %q: %w", tableName, err)
		}
		tableStats[tableName] = stats
	}
	return nil
}

func (s *StatManager) calcTableStatsLocked(ctx context.Context, tableName string, layout *dbrecord.Layout, tx *dbtx.Transaction) (statInfo *StatInfo, err error) {
	ts, err := dbrecord.NewTableScan(ctx, tx, tableName, layout)
	if err != nil {
		return nil, fmt.Errorf("new table scan for %q: %w", tableName, err)
	}
	defer func() {
		if closeErr := ts.Close(); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}()
	var numRecord, numBlocks int
	for {
		next, err := ts.Next(ctx)
		if err != nil {
			return nil, fmt.Errorf("go next for %q: %w", tableName, err)
		}
		if !next {
			numBlocks = ts.RID().BlockNum() + 1
			break
		}
		numRecord++
	}

	d, err := s.CalcDistinctValues(ctx, numRecord, ts, layout)
	if err != nil {
		return nil, fmt.Errorf("calc distinct values for %q: %w", tableName, err)
	}

	return &StatInfo{
		numBlocks:         numBlocks,
		numRecords:        numRecord,
		distinctValuesMap: d,
	}, nil
}

func (s *StatManager) CalcDistinctValues(ctx context.Context, numRecord int, ts *dbrecord.TableScan, layout *dbrecord.Layout) (map[string]int, error) {
	// distinct valueを計算するサンプル数
	numSamples := numRecord/10 + 1
	if err := ts.SetStateToBeforeFirst(ctx); err != nil {
		return nil, fmt.Errorf("set state to before first for %q: %w", ts.TableName(), err)
	}
	distinctValuesForCalc := make(map[string]map[string]struct{})
	for i := 0; i < numSamples; i++ {
		next, err := ts.Next(ctx)
		if err != nil {
			return nil, fmt.Errorf("go next for %q: %w", ts.TableName(), err)
		}
		if !next {
			break
		}
		for _, field := range layout.Schema().Fields() {
			var fieldValString string
			switch layout.Schema().FieldType(field) {
			case dbrecord.FieldTypeInt:
				val, err := ts.GetInt(ctx, field)
				if err != nil {
					return nil, fmt.Errorf("get int value for %q: %w", field, err)
				}
				fieldValString = strconv.Itoa(val)
			case dbrecord.FieldTypeString:
				val, err := ts.GetString(ctx, field)
				if err != nil {
					return nil, fmt.Errorf("get string value for %q: %w", field, err)
				}
				fieldValString = val
			}
			if len(distinctValuesForCalc[field]) == 0 {
				distinctValuesForCalc[field] = make(map[string]struct{})
			}
			distinctValuesForCalc[field][fieldValString] = struct{}{}
		}
	}

	distinctValues := make(map[string]int)
	for field, m := range distinctValuesForCalc {
		if len(m) > int(float64(numSamples)*0.9) {
			// サンプル数とパターン数がほぼ同一: idなどの可能性が高い
			// レコードの数だけパターンがあると近似
			distinctValues[field] = numRecord
		} else if len(m) < int(float64(numSamples)*0.1) {
			// パターン数がサンプル数よりも大きく少ない: 性別やフラグなど
			// パターンが出来っていると近似
			distinctValues[field] = len(m)
		} else {
			// それ以外は同じ比率で含まれてると近似
			distinctValues[field] = len(m) * numRecord / numSamples
		}
	}
	return distinctValues, nil
}

func NewStatInfo(numBlocks, numRecords int) *StatInfo {
	return &StatInfo{
		numBlocks:  numBlocks,
		numRecords: numRecords,
	}
}

func (s *StatInfo) BlockAccessed() int {
	return s.numBlocks
}

func (s *StatInfo) RecordsOutput() int {
	return s.numRecords
}

func (s *StatInfo) DistinctValues(fieldName string) int {
	return s.distinctValuesMap[fieldName]
}

func (s *StatInfo) DistinctValuesMap() map[string]int {
	return s.distinctValuesMap
}
