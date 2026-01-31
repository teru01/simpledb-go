package dbparse

import (
	"github.com/teru01/simpledb-go/dbconstant"
	"github.com/teru01/simpledb-go/dbquery"
	"github.com/teru01/simpledb-go/dbrecord"
)

// InsertData represents an INSERT statement
type InsertData struct {
	tableName string
	fields    []string
	vals      []dbconstant.Constant
}

func NewInsertData(tableName string, fields []string, vals []dbconstant.Constant) *InsertData {
	return &InsertData{tableName: tableName, fields: fields, vals: vals}
}

func (d *InsertData) TableName() string {
	return d.tableName
}

func (d *InsertData) Fields() []string {
	return d.fields
}

func (d *InsertData) Vals() []dbconstant.Constant {
	return d.vals
}

// DeleteData represents a DELETE statement
type DeleteData struct {
	tableName string
	predicate *dbquery.Predicate
}

func NewDeleteData(tableName string, predicate *dbquery.Predicate) *DeleteData {
	return &DeleteData{tableName: tableName, predicate: predicate}
}

func (d *DeleteData) TableName() string {
	return d.tableName
}

func (d *DeleteData) Predicate() *dbquery.Predicate {
	return d.predicate
}

// ModifyData represents an UPDATE statement
type ModifyData struct {
	tableName string
	fieldName string
	newVal    *dbquery.Expression
	predicate *dbquery.Predicate
}

func NewModifyData(tableName string, fieldName string, newVal *dbquery.Expression, predicate *dbquery.Predicate) *ModifyData {
	return &ModifyData{tableName: tableName, fieldName: fieldName, newVal: newVal, predicate: predicate}
}

func (d *ModifyData) TableName() string {
	return d.tableName
}

func (d *ModifyData) FieldName() string {
	return d.fieldName
}

func (d *ModifyData) NewVal() *dbquery.Expression {
	return d.newVal
}

func (d *ModifyData) Predicate() *dbquery.Predicate {
	return d.predicate
}

// CreateTableData represents a CREATE TABLE statement
type CreateTableData struct {
	tableName string
	schema    *dbrecord.Schema
}

func NewCreateTableData(tableName string, schema *dbrecord.Schema) *CreateTableData {
	return &CreateTableData{tableName: tableName, schema: schema}
}

func (d *CreateTableData) TableName() string {
	return d.tableName
}

func (d *CreateTableData) Schema() *dbrecord.Schema {
	return d.schema
}

// CreateViewData represents a CREATE VIEW statement
type CreateViewData struct {
	viewName string
	query    *QueryData
}

func NewCreateViewData(viewName string, query *QueryData) *CreateViewData {
	return &CreateViewData{viewName: viewName, query: query}
}

func (d *CreateViewData) ViewName() string {
	return d.viewName
}

func (d *CreateViewData) Query() *QueryData {
	return d.query
}

func (d *CreateViewData) ViewDef() string {
	return d.query.String()
}

// CreateIndexData represents a CREATE INDEX statement
type CreateIndexData struct {
	indexName string
	tableName string
	fieldName string
}

func NewCreateIndexData(indexName string, tableName string, fieldName string) *CreateIndexData {
	return &CreateIndexData{indexName: indexName, tableName: tableName, fieldName: fieldName}
}

func (d *CreateIndexData) IndexName() string {
	return d.indexName
}

func (d *CreateIndexData) TableName() string {
	return d.tableName
}

func (d *CreateIndexData) FieldName() string {
	return d.fieldName
}

type QueryData struct {
	fields    []string
	tables    []string
	predicate *dbquery.Predicate
}

func NewQueryData(fields []string, tables []string, predicate *dbquery.Predicate) *QueryData {
	return &QueryData{fields: fields, tables: tables, predicate: predicate}
}

func (q *QueryData) Fields() []string {
	return q.fields
}

func (q *QueryData) Tables() []string {
	return q.tables
}

func (q *QueryData) Predicate() *dbquery.Predicate {
	return q.predicate
}

func (q *QueryData) String() string {
	result := "SELECT "
	for _, field := range q.fields {
		result += field + ", "
	}
	result = result[:len(result)-2]
	result += " FROM "
	for _, table := range q.tables {
		result += table + ", "
	}
	result = result[:len(result)-2]

	if q.predicate != nil {
		result += " WHERE "
		result += q.predicate.String()
	}
	return result
}
