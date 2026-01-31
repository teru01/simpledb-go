package dbparse_test

import (
	"testing"

	"github.com/teru01/simpledb-go/dbparse"
	"github.com/teru01/simpledb-go/dbrecord"
)

func TestParseQuery(t *testing.T) {
	tests := []struct {
		name           string
		input          string
		expectedFields []string
		expectedTables []string
	}{
		{
			name:           "simple select",
			input:          "SELECT name FROM users",
			expectedFields: []string{"name"},
			expectedTables: []string{"users"},
		},
		{
			name:           "multiple fields",
			input:          "SELECT id, name, age FROM users",
			expectedFields: []string{"id", "name", "age"},
			expectedTables: []string{"users"},
		},
		{
			name:           "multiple tables",
			input:          "SELECT id, name FROM users, orders",
			expectedFields: []string{"id", "name"},
			expectedTables: []string{"users", "orders"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := dbparse.NewParser(tt.input)
			q, err := p.Query()
			if err != nil {
				t.Fatalf("failed to parse query: %v", err)
			}

			if len(q.Fields()) != len(tt.expectedFields) {
				t.Errorf("expected %d fields, got %d", len(tt.expectedFields), len(q.Fields()))
			}
			for i, f := range q.Fields() {
				if f != tt.expectedFields[i] {
					t.Errorf("expected field %q, got %q", tt.expectedFields[i], f)
				}
			}

			if len(q.Tables()) != len(tt.expectedTables) {
				t.Errorf("expected %d tables, got %d", len(tt.expectedTables), len(q.Tables()))
			}
			for i, tbl := range q.Tables() {
				if tbl != tt.expectedTables[i] {
					t.Errorf("expected table %q, got %q", tt.expectedTables[i], tbl)
				}
			}
		})
	}
}

func TestParseQueryWithWhere(t *testing.T) {
	input := "SELECT name FROM users WHERE id = 1"
	p := dbparse.NewParser(input)
	q, err := p.Query()
	if err != nil {
		t.Fatalf("failed to parse query: %v", err)
	}

	if len(q.Fields()) != 1 || q.Fields()[0] != "name" {
		t.Errorf("unexpected fields: %v", q.Fields())
	}
	if len(q.Tables()) != 1 || q.Tables()[0] != "users" {
		t.Errorf("unexpected tables: %v", q.Tables())
	}
	if q.Predicate() == nil {
		t.Errorf("expected predicate, got nil")
	}
}

func TestParseQueryWithMultiplePredicates(t *testing.T) {
	input := `SELECT name FROM users WHERE id = 1 AND age = 25`
	p := dbparse.NewParser(input)
	q, err := p.Query()
	if err != nil {
		t.Fatalf("failed to parse query: %v", err)
	}

	if q.Predicate() == nil {
		t.Errorf("expected predicate, got nil")
	}
}

func TestParseInsert(t *testing.T) {
	input := `INSERT INTO users (id, name, age) VALUES (1, "alice", 25)`
	p := dbparse.NewParser(input)
	ins, err := p.Insert()
	if err != nil {
		t.Fatalf("failed to parse insert: %v", err)
	}

	if ins.TableName() != "users" {
		t.Errorf("expected table 'users', got %q", ins.TableName())
	}

	expectedFields := []string{"id", "name", "age"}
	if len(ins.Fields()) != len(expectedFields) {
		t.Errorf("expected %d fields, got %d", len(expectedFields), len(ins.Fields()))
	}
	for i, f := range ins.Fields() {
		if f != expectedFields[i] {
			t.Errorf("expected field %q, got %q", expectedFields[i], f)
		}
	}

	if len(ins.Vals()) != 3 {
		t.Errorf("expected 3 values, got %d", len(ins.Vals()))
	}
}

func TestParseDelete(t *testing.T) {
	input := "DELETE FROM users WHERE id = 1"
	p := dbparse.NewParser(input)
	del, err := p.Delete()
	if err != nil {
		t.Fatalf("failed to parse delete: %v", err)
	}

	if del.TableName() != "users" {
		t.Errorf("expected table 'users', got %q", del.TableName())
	}
	if del.Predicate() == nil {
		t.Errorf("expected predicate, got nil")
	}
}

func TestParseDeleteWithoutWhere(t *testing.T) {
	input := "DELETE FROM users"
	p := dbparse.NewParser(input)
	del, err := p.Delete()
	if err != nil {
		t.Fatalf("failed to parse delete: %v", err)
	}

	if del.TableName() != "users" {
		t.Errorf("expected table 'users', got %q", del.TableName())
	}
}

func TestParseModify(t *testing.T) {
	input := "UPDATE users SET name = \"bob\" WHERE id = 1"
	p := dbparse.NewParser(input)
	mod, err := p.Modify()
	if err != nil {
		t.Fatalf("failed to parse modify: %v", err)
	}

	if mod.TableName() != "users" {
		t.Errorf("expected table 'users', got %q", mod.TableName())
	}
	if mod.FieldName() != "name" {
		t.Errorf("expected field 'name', got %q", mod.FieldName())
	}
	if mod.NewVal() == nil {
		t.Errorf("expected new value, got nil")
	}
	if mod.Predicate() == nil {
		t.Errorf("expected predicate, got nil")
	}
}

func TestParseModifyWithoutWhere(t *testing.T) {
	input := "UPDATE users SET age = 30"
	p := dbparse.NewParser(input)
	mod, err := p.Modify()
	if err != nil {
		t.Fatalf("failed to parse modify: %v", err)
	}

	if mod.TableName() != "users" {
		t.Errorf("expected table 'users', got %q", mod.TableName())
	}
	if mod.FieldName() != "age" {
		t.Errorf("expected field 'age', got %q", mod.FieldName())
	}
}

func TestParseCreateTable(t *testing.T) {
	input := "CREATE TABLE users (id INT, name VARCHAR(20), age INT)"
	p := dbparse.NewParser(input)
	ct, err := p.Create()
	if err != nil {
		t.Fatalf("failed to parse create table: %v", err)
	}

	createTable, ok := ct.(*dbparse.CreateTableData)
	if !ok {
		t.Fatalf("expected *CreateTableData, got %T", ct)
	}

	if createTable.TableName() != "users" {
		t.Errorf("expected table 'users', got %q", createTable.TableName())
	}

	schema := createTable.Schema()
	fields := schema.Fields()
	if len(fields) != 3 {
		t.Errorf("expected 3 fields, got %d", len(fields))
	}

	// Check field types
	if schema.FieldType("id") != dbrecord.FieldTypeInt {
		t.Errorf("expected id to be INT")
	}
	if schema.FieldType("name") != dbrecord.FieldTypeString {
		t.Errorf("expected name to be VARCHAR")
	}
	if schema.Length("name") != 20 {
		t.Errorf("expected name length 20, got %d", schema.Length("name"))
	}
	if schema.FieldType("age") != dbrecord.FieldTypeInt {
		t.Errorf("expected age to be INT")
	}
}

func TestParseCreateView(t *testing.T) {
	input := "CREATE VIEW active_users AS SELECT id, name FROM users WHERE active = 1"
	p := dbparse.NewParser(input)
	cv, err := p.Create()
	if err != nil {
		t.Fatalf("failed to parse create view: %v", err)
	}

	createView, ok := cv.(*dbparse.CreateViewData)
	if !ok {
		t.Fatalf("expected *CreateViewData, got %T", cv)
	}

	if createView.ViewName() != "active_users" {
		t.Errorf("expected view 'active_users', got %q", createView.ViewName())
	}

	query := createView.Query()
	if query == nil {
		t.Errorf("expected query, got nil")
	}

	expectedFields := []string{"id", "name"}
	if len(query.Fields()) != len(expectedFields) {
		t.Errorf("expected %d fields, got %d", len(expectedFields), len(query.Fields()))
	}
}

func TestParseCreateIndex(t *testing.T) {
	input := "CREATE INDEX idx_name ON users (name)"
	p := dbparse.NewParser(input)
	ci, err := p.Create()
	if err != nil {
		t.Fatalf("failed to parse create index: %v", err)
	}

	createIndex, ok := ci.(*dbparse.CreateIndexData)
	if !ok {
		t.Fatalf("expected *CreateIndexData, got %T", ci)
	}

	if createIndex.IndexName() != "idx_name" {
		t.Errorf("expected index 'idx_name', got %q", createIndex.IndexName())
	}
	if createIndex.TableName() != "users" {
		t.Errorf("expected table 'users', got %q", createIndex.TableName())
	}
	if createIndex.FieldName() != "name" {
		t.Errorf("expected field 'name', got %q", createIndex.FieldName())
	}
}

func TestParseUpdateCmd(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "insert",
			input:    `INSERT INTO users (id) VALUES (1)`,
			expected: "*dbparse.InsertData",
		},
		{
			name:     "delete",
			input:    "DELETE FROM users",
			expected: "*dbparse.DeleteData",
		},
		{
			name:     "update",
			input:    "UPDATE users SET id = 1",
			expected: "*dbparse.ModifyData",
		},
		{
			name:     "create table",
			input:    "CREATE TABLE users (id INT)",
			expected: "*dbparse.CreateTableData",
		},
		{
			name:     "create view",
			input:    "CREATE VIEW v AS SELECT id FROM users",
			expected: "*dbparse.CreateViewData",
		},
		{
			name:     "create index",
			input:    "CREATE INDEX idx ON users (id)",
			expected: "*dbparse.CreateIndexData",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := dbparse.NewParser(tt.input)
			cmd, err := p.UpdateCmd()
			if err != nil {
				t.Fatalf("failed to parse: %v", err)
			}

			typeName := getTypeName(cmd)
			if typeName != tt.expected {
				t.Errorf("expected %s, got %s", tt.expected, typeName)
			}
		})
	}
}

func getTypeName(v any) string {
	switch v.(type) {
	case *dbparse.InsertData:
		return "*dbparse.InsertData"
	case *dbparse.DeleteData:
		return "*dbparse.DeleteData"
	case *dbparse.ModifyData:
		return "*dbparse.ModifyData"
	case *dbparse.CreateTableData:
		return "*dbparse.CreateTableData"
	case *dbparse.CreateViewData:
		return "*dbparse.CreateViewData"
	case *dbparse.CreateIndexData:
		return "*dbparse.CreateIndexData"
	default:
		return "unknown"
	}
}

func TestParseTerm(t *testing.T) {
	input := "id = 1"
	p := dbparse.NewParser(input)
	term, err := p.Term()
	if err != nil {
		t.Fatalf("failed to parse term: %v", err)
	}
	if term == nil {
		t.Errorf("expected term, got nil")
	}
}

func TestParseExpression(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"field", "name"},
		{"int constant", "42"},
		{"string constant", `"hello"`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := dbparse.NewParser(tt.input)
			expr, err := p.Expression()
			if err != nil {
				t.Fatalf("failed to parse expression: %v", err)
			}
			if expr == nil {
				t.Errorf("expected expression, got nil")
			}
		})
	}
}

func TestParsePredicate(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"single term", "id = 1"},
		{"two terms", "id = 1 AND name = \"alice\""},
		{"three terms", "id = 1 AND name = \"alice\" AND age = 25"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := dbparse.NewParser(tt.input)
			pred, err := p.Predicate()
			if err != nil {
				t.Fatalf("failed to parse predicate: %v", err)
			}
			if pred == nil {
				t.Errorf("expected predicate, got nil")
			}
		})
	}
}
