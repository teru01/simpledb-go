package dbparse

import "github.com/teru01/simpledb-go/dbquery"

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
