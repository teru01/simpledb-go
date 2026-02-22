package dbmetadata

import (
	"context"
	"fmt"

	"github.com/teru01/simpledb-go/dbrecord"
	"github.com/teru01/simpledb-go/dbtx"
)

const (
	MaxNameLength         = 16
	TableCatalogTableName = "table_catalog"
	FieldCatalogTableName = "field_catalog"
)

type TableManager struct {
	tableCatalogLayout *dbrecord.Layout
	fieldCatalogLayout *dbrecord.Layout
}

func NewTableManager(ctx context.Context, isNew bool, tx *dbtx.Transaction) (*TableManager, error) {
	tableCatalogSchema := dbrecord.NewSchema()
	tableCatalogSchema.AddStringField("tablename", MaxNameLength)
	tableCatalogSchema.AddIntField("slotsize")
	tableCatalogLayout := dbrecord.NewLayout(tableCatalogSchema)

	fieldCatalogSchema := dbrecord.NewSchema()
	fieldCatalogSchema.AddStringField("tablename", MaxNameLength)
	fieldCatalogSchema.AddStringField("fieldname", MaxNameLength)
	fieldCatalogSchema.AddIntField("type")
	fieldCatalogSchema.AddIntField("length")
	fieldCatalogSchema.AddIntField("offset")
	fieldCatalogLayout := dbrecord.NewLayout(fieldCatalogSchema)

	t := &TableManager{
		tableCatalogLayout: tableCatalogLayout,
		fieldCatalogLayout: fieldCatalogLayout,
	}

	exists, err := t.tableFileExists(ctx, tx)
	if err != nil {
		return nil, fmt.Errorf("check if table file exists: %w", err)
	}

	if isNew || !exists {
		if err := t.CreateTable(ctx, TableCatalogTableName, tableCatalogSchema, tx); err != nil {
			return nil, fmt.Errorf("create table catalog: %w", err)
		}
		if err := t.CreateTable(ctx, FieldCatalogTableName, fieldCatalogSchema, tx); err != nil {
			return nil, fmt.Errorf("create field catalog: %w", err)
		}
	}
	return t, nil
}

func (t *TableManager) tableFileExists(ctx context.Context, tx *dbtx.Transaction) (bool, error) {
	size, err := tx.Size(ctx, dbrecord.TableFileName(TableCatalogTableName))
	if err != nil {
		return false, fmt.Errorf("get size for %q: %w", dbrecord.TableFileName(TableCatalogTableName), err)
	}
	return size > 0, nil
}

func (t *TableManager) CreateTable(ctx context.Context, tableName string, schema *dbrecord.Schema, tx *dbtx.Transaction) error {
	layout := dbrecord.NewLayout(schema)

	tableCatlog, err := dbrecord.NewTableScan(ctx, tx, TableCatalogTableName, t.tableCatalogLayout)
	if err != nil {
		return fmt.Errorf("create table scan for table_catalog when creating %q: %w", tableName, err)
	}
	if err := tableCatlog.Insert(ctx); err != nil {
		return fmt.Errorf("insert to table_catalog for %q: %w", tableName, err)
	}
	if err := tableCatlog.SetString(ctx, "tablename", tableName); err != nil {
		return fmt.Errorf("set tablename for %q: %w", tableName, err)
	}
	if err := tableCatlog.SetInt(ctx, "slotsize", layout.SlotSize()); err != nil {
		return fmt.Errorf("set slotsize for %q: %w", tableName, err)
	}
	if err := tableCatlog.Close(ctx); err != nil {
		return fmt.Errorf("close table scan for table_catalog when creating %q: %w", tableName, err)
	}
	fieldCatlog, err := dbrecord.NewTableScan(ctx, tx, FieldCatalogTableName, t.fieldCatalogLayout)
	if err != nil {
		return fmt.Errorf("create table scan for field_catalog when creating %q: %w", tableName, err)
	}
	for _, fieldName := range schema.Fields() {
		if err := fieldCatlog.Insert(ctx); err != nil {
			return fmt.Errorf("insert to %q: %w", FieldCatalogTableName, err)
		}
		if err := fieldCatlog.SetString(ctx, "tablename", tableName); err != nil {
			return fmt.Errorf("set tablename for %q: %w", tableName, err)
		}
		if err := fieldCatlog.SetString(ctx, "fieldname", fieldName); err != nil {
			return fmt.Errorf("set fieldname for %q: %w", tableName, err)
		}
		if err := fieldCatlog.SetInt(ctx, "type", schema.FieldType(fieldName)); err != nil {
			return fmt.Errorf("set type for %q: %w", tableName, err)
		}
		if err := fieldCatlog.SetInt(ctx, "length", schema.Length(fieldName)); err != nil {
			return fmt.Errorf("set length for %q: %w", tableName, err)
		}
		if err := fieldCatlog.SetInt(ctx, "offset", layout.Offset(fieldName)); err != nil {
			return fmt.Errorf("set offset for %q: %w", tableName, err)
		}
	}
	if err := fieldCatlog.Close(ctx); err != nil {
		return fmt.Errorf("close table scan for field_catalog when creating %q: %w", tableName, err)
	}
	return nil
}

func (t *TableManager) GetLayout(ctx context.Context, tableName string, tx *dbtx.Transaction) (*dbrecord.Layout, error) {
	tableCatlog, err := dbrecord.NewTableScan(ctx, tx, TableCatalogTableName, t.tableCatalogLayout)
	if err != nil {
		return nil, fmt.Errorf("create table scan: %w", err)
	}
	slotSize := -1
	for {
		next, err := tableCatlog.Next(ctx)
		if err != nil {
			return nil, fmt.Errorf("go next for %q: %w", TableCatalogTableName, err)
		}
		if !next {
			if err := tableCatlog.Close(ctx); err != nil {
				return nil, fmt.Errorf("close table_catalog: %w", err)
			}
			break
		}
		tableNameValue, err := tableCatlog.GetString(ctx, "tablename")
		if err != nil {
			return nil, fmt.Errorf("get tablename: %w", err)
		}
		if tableNameValue == tableName {
			ss, err := tableCatlog.GetInt(ctx, "slotsize")
			if err != nil {
				return nil, fmt.Errorf("get slotsize: %w", err)
			}
			slotSize = ss
		}
	}

	schema := dbrecord.NewSchema()

	fieldCatlog, err := dbrecord.NewTableScan(ctx, tx, FieldCatalogTableName, t.fieldCatalogLayout)
	if err != nil {
		return nil, fmt.Errorf("create table scan: %w", err)
	}

	offsets := make(map[string]int)
	for {
		next, err := fieldCatlog.Next(ctx)
		if err != nil {
			return nil, fmt.Errorf("go next for %q: %w", FieldCatalogTableName, err)
		}
		if !next {
			if err := fieldCatlog.Close(ctx); err != nil {
				return nil, fmt.Errorf("close field_catalog: %w", err)
			}
			break
		}
		tableNameValue, err := fieldCatlog.GetString(ctx, "tablename")
		if err != nil {
			return nil, fmt.Errorf("get tablename: %w", err)
		}
		if tableNameValue == tableName {
			fieldName, err := fieldCatlog.GetString(ctx, "fieldname")
			if err != nil {
				return nil, fmt.Errorf("get fieldname: %w", err)
			}
			fieldType, err := fieldCatlog.GetInt(ctx, "type")
			if err != nil {
				return nil, fmt.Errorf("get type: %w", err)
			}
			length, err := fieldCatlog.GetInt(ctx, "length")
			if err != nil {
				return nil, fmt.Errorf("get length: %w", err)
			}
			offset, err := fieldCatlog.GetInt(ctx, "offset")
			if err != nil {
				return nil, fmt.Errorf("get offset: %w", err)
			}
			schema.AddField(fieldName, fieldType, length)
			offsets[fieldName] = offset
		}
	}
	return dbrecord.NewLayoutFromOffsets(schema, offsets, slotSize), nil
}
