package dbparse

import (
	"fmt"

	"github.com/teru01/simpledb-go/dbconstant"
	"github.com/teru01/simpledb-go/dbquery"
	"github.com/teru01/simpledb-go/dbrecord"
)

type Parser struct {
	lex *Lexer
}

func NewParser(s string) *Parser {
	return &Parser{
		lex: NewLexer(s),
	}
}

// <Field> := IdTok
func (p *Parser) Field() (string, error) {
	return p.lex.EatIdentifier()
}

// <Constant> := StrTok | IntTok
func (p *Parser) Constant() (dbconstant.Constant, error) {
	if p.lex.IsNextString() {
		s, err := p.lex.EatStringConstant()
		if err != nil {
			return nil, err
		}
		return dbconstant.NewStringConstant(s), nil
	}
	i, err := p.lex.EatIntConstant()
	if err != nil {
		return nil, err
	}
	return dbconstant.NewIntConstant(i), nil
}

// <Expression> := <Field> | <Constant>
func (p *Parser) Expression() (*dbquery.Expression, error) {
	if p.lex.IsNextIdentifier() {
		field, err := p.Field()
		if err != nil {
			return nil, err
		}
		return dbquery.NewExpressionFromFieldName(field), nil
	}
	constant, err := p.Constant()
	if err != nil {
		return nil, err
	}
	return dbquery.NewExpressionFromValue(constant), nil
}

// <Term> := <Expression> = <Expression>
func (p *Parser) Term() (*dbquery.Term, error) {
	lhs, err := p.Expression()
	if err != nil {
		return nil, err
	}
	if err := p.lex.EatDelimiter('='); err != nil {
		return nil, err
	}
	rhs, err := p.Expression()
	if err != nil {
		return nil, err
	}
	return dbquery.NewTerm(lhs, rhs), nil
}

// <Predicate> := <Term> [ AND <Predicate> ]
func (p *Parser) Predicate() (*dbquery.Predicate, error) {
	term, err := p.Term()
	if err != nil {
		return nil, err
	}
	pred := dbquery.NewPredicate(term)
	for p.lex.IsNextKeyword("and") {
		if err := p.lex.EatKeyword("and"); err != nil {
			return nil, err
		}
		term, err := p.Term()
		if err != nil {
			return nil, err
		}
		pred.ConjoinWith(dbquery.NewPredicate(term))
	}
	return pred, nil
}

// <Query> := SELECT <SelectList> FROM <TableList> [ WHERE <Predicate> ]
func (p *Parser) Query() (*QueryData, error) {
	if err := p.lex.EatKeyword("select"); err != nil {
		return nil, err
	}
	fields, err := p.selectList()
	if err != nil {
		return nil, err
	}
	if err := p.lex.EatKeyword("from"); err != nil {
		return nil, err
	}
	tables, err := p.tableList()
	if err != nil {
		return nil, err
	}
	pred := dbquery.NewPredicate()
	if p.lex.IsNextKeyword("where") {
		if err := p.lex.EatKeyword("where"); err != nil {
			return nil, err
		}
		pred, err = p.Predicate()
		if err != nil {
			return nil, err
		}
	}
	return NewQueryData(fields, tables, pred), nil
}

// <SelectList> := <Field> [ , <SelectList> ]
func (p *Parser) selectList() ([]string, error) {
	fields := []string{}
	field, err := p.Field()
	if err != nil {
		return nil, err
	}
	fields = append(fields, field)
	for p.lex.IsNextDelimiter(',') {
		if err := p.lex.EatDelimiter(','); err != nil {
			return nil, err
		}
		field, err := p.Field()
		if err != nil {
			return nil, err
		}
		fields = append(fields, field)
	}
	return fields, nil
}

// <TableList> := IdTok [ , <TableList> ]
func (p *Parser) tableList() ([]string, error) {
	tables := []string{}
	table, err := p.lex.EatIdentifier()
	if err != nil {
		return nil, err
	}
	tables = append(tables, table)
	for p.lex.IsNextDelimiter(',') {
		if err := p.lex.EatDelimiter(','); err != nil {
			return nil, err
		}
		table, err := p.lex.EatIdentifier()
		if err != nil {
			return nil, err
		}
		tables = append(tables, table)
	}
	return tables, nil
}

// <UpdateCmd> := <Insert> | <Delete> | <Modify> | <Create>
func (p *Parser) UpdateCmd() (any, error) {
	if p.lex.IsNextKeyword("insert") {
		return p.Insert()
	} else if p.lex.IsNextKeyword("delete") {
		return p.Delete()
	} else if p.lex.IsNextKeyword("update") {
		return p.Modify()
	} else if p.lex.IsNextKeyword("create") {
		return p.Create()
	}
	return nil, fmt.Errorf("unexpected token: expected insert, delete, update, or create")
}

// <Create> := <CreateTable> | <CreateView> | <CreateIndex>
func (p *Parser) Create() (any, error) {
	if err := p.lex.EatKeyword("create"); err != nil {
		return nil, err
	}
	if p.lex.IsNextKeyword("table") {
		return p.CreateTable()
	} else if p.lex.IsNextKeyword("view") {
		return p.CreateView()
	} else if p.lex.IsNextKeyword("index") {
		return p.CreateIndex()
	}
	return nil, fmt.Errorf("unexpected token: expected table, view, or index")
}

// <Insert> := INSERT INTO IdTok ( <FieldList> ) VALUES ( <ConstList> )
func (p *Parser) Insert() (*InsertData, error) {
	if err := p.lex.EatKeyword("insert"); err != nil {
		return nil, err
	}
	if err := p.lex.EatKeyword("into"); err != nil {
		return nil, err
	}
	tableName, err := p.lex.EatIdentifier()
	if err != nil {
		return nil, err
	}
	if err := p.lex.EatDelimiter('('); err != nil {
		return nil, err
	}
	fields, err := p.fieldList()
	if err != nil {
		return nil, err
	}
	if err := p.lex.EatDelimiter(')'); err != nil {
		return nil, err
	}
	if err := p.lex.EatKeyword("values"); err != nil {
		return nil, err
	}
	if err := p.lex.EatDelimiter('('); err != nil {
		return nil, err
	}
	vals, err := p.constList()
	if err != nil {
		return nil, err
	}
	if err := p.lex.EatDelimiter(')'); err != nil {
		return nil, err
	}
	return NewInsertData(tableName, fields, vals), nil
}

// <FieldList> := <Field> [ , <FieldList> ]
func (p *Parser) fieldList() ([]string, error) {
	fields := []string{}
	field, err := p.Field()
	if err != nil {
		return nil, err
	}
	fields = append(fields, field)
	for p.lex.IsNextDelimiter(',') {
		if err := p.lex.EatDelimiter(','); err != nil {
			return nil, err
		}
		field, err := p.Field()
		if err != nil {
			return nil, err
		}
		fields = append(fields, field)
	}
	return fields, nil
}

// <ConstList> := <Constant> [ , <ConstList> ]
func (p *Parser) constList() ([]dbconstant.Constant, error) {
	vals := []dbconstant.Constant{}
	val, err := p.Constant()
	if err != nil {
		return nil, err
	}
	vals = append(vals, val)
	for p.lex.IsNextDelimiter(',') {
		if err := p.lex.EatDelimiter(','); err != nil {
			return nil, err
		}
		val, err := p.Constant()
		if err != nil {
			return nil, err
		}
		vals = append(vals, val)
	}
	return vals, nil
}

// <Delete> := DELETE FROM IdTok [ WHERE <Predicate> ]
func (p *Parser) Delete() (*DeleteData, error) {
	if err := p.lex.EatKeyword("delete"); err != nil {
		return nil, err
	}
	if err := p.lex.EatKeyword("from"); err != nil {
		return nil, err
	}
	tableName, err := p.lex.EatIdentifier()
	if err != nil {
		return nil, err
	}
	pred := dbquery.NewPredicate()
	if p.lex.IsNextKeyword("where") {
		if err := p.lex.EatKeyword("where"); err != nil {
			return nil, err
		}
		pred, err = p.Predicate()
		if err != nil {
			return nil, err
		}
	}
	return NewDeleteData(tableName, pred), nil
}

// <Modify> := UPDATE IdTok SET <Field> = <Expression> [ WHERE <Predicate> ]
func (p *Parser) Modify() (*ModifyData, error) {
	if err := p.lex.EatKeyword("update"); err != nil {
		return nil, err
	}
	tableName, err := p.lex.EatIdentifier()
	if err != nil {
		return nil, err
	}
	if err := p.lex.EatKeyword("set"); err != nil {
		return nil, err
	}
	fieldName, err := p.Field()
	if err != nil {
		return nil, err
	}
	if err := p.lex.EatDelimiter('='); err != nil {
		return nil, err
	}
	newVal, err := p.Expression()
	if err != nil {
		return nil, err
	}
	pred := dbquery.NewPredicate()
	if p.lex.IsNextKeyword("where") {
		if err := p.lex.EatKeyword("where"); err != nil {
			return nil, err
		}
		pred, err = p.Predicate()
		if err != nil {
			return nil, err
		}
	}
	return NewModifyData(tableName, fieldName, newVal, pred), nil
}

// <CreateTable> := CREATE TABLE IdTok ( <FieldDefs> )
func (p *Parser) CreateTable() (*CreateTableData, error) {
	if err := p.lex.EatKeyword("table"); err != nil {
		return nil, err
	}
	tableName, err := p.lex.EatIdentifier()
	if err != nil {
		return nil, err
	}
	if err := p.lex.EatDelimiter('('); err != nil {
		return nil, err
	}
	schema, err := p.fieldDefs()
	if err != nil {
		return nil, err
	}
	if err := p.lex.EatDelimiter(')'); err != nil {
		return nil, err
	}
	return NewCreateTableData(tableName, schema), nil
}

// <FieldDefs> := <FieldDef> [ , <FieldDefs> ]
func (p *Parser) fieldDefs() (*dbrecord.Schema, error) {
	schema := dbrecord.NewSchema()
	fieldName, fieldType, length, err := p.fieldDef()
	if err != nil {
		return nil, err
	}
	schema.AddField(fieldName, fieldType, length)
	for p.lex.IsNextDelimiter(',') {
		if err := p.lex.EatDelimiter(','); err != nil {
			return nil, err
		}
		fieldName, fieldType, length, err := p.fieldDef()
		if err != nil {
			return nil, err
		}
		schema.AddField(fieldName, fieldType, length)
	}
	return schema, nil
}

// <FieldDef> := IdTok <TypeDef>
func (p *Parser) fieldDef() (string, int, int, error) {
	fieldName, err := p.lex.EatIdentifier()
	if err != nil {
		return "", 0, 0, err
	}
	fieldType, length, err := p.typeDef()
	if err != nil {
		return "", 0, 0, err
	}
	return fieldName, fieldType, length, nil
}

// <TypeDef> := INT | VARCHAR ( IntTok )
func (p *Parser) typeDef() (int, int, error) {
	if p.lex.IsNextKeyword("int") {
		if err := p.lex.EatKeyword("int"); err != nil {
			return 0, 0, err
		}
		return dbrecord.FieldTypeInt, 0, nil
	}
	if err := p.lex.EatKeyword("varchar"); err != nil {
		return 0, 0, err
	}
	if err := p.lex.EatDelimiter('('); err != nil {
		return 0, 0, err
	}
	length, err := p.lex.EatIntConstant()
	if err != nil {
		return 0, 0, err
	}
	if err := p.lex.EatDelimiter(')'); err != nil {
		return 0, 0, err
	}
	return dbrecord.FieldTypeString, length, nil
}

// <CreateView> := CREATE VIEW IdTok AS <Query>
func (p *Parser) CreateView() (*CreateViewData, error) {
	if err := p.lex.EatKeyword("view"); err != nil {
		return nil, err
	}
	viewName, err := p.lex.EatIdentifier()
	if err != nil {
		return nil, err
	}
	if err := p.lex.EatKeyword("as"); err != nil {
		return nil, err
	}
	query, err := p.Query()
	if err != nil {
		return nil, err
	}
	return NewCreateViewData(viewName, query), nil
}

// <CreateIndex> := CREATE INDEX IdTok ON IdTok ( <Field> )
func (p *Parser) CreateIndex() (*CreateIndexData, error) {
	if err := p.lex.EatKeyword("index"); err != nil {
		return nil, err
	}
	indexName, err := p.lex.EatIdentifier()
	if err != nil {
		return nil, err
	}
	if err := p.lex.EatKeyword("on"); err != nil {
		return nil, err
	}
	tableName, err := p.lex.EatIdentifier()
	if err != nil {
		return nil, err
	}
	if err := p.lex.EatDelimiter('('); err != nil {
		return nil, err
	}
	fieldName, err := p.Field()
	if err != nil {
		return nil, err
	}
	if err := p.lex.EatDelimiter(')'); err != nil {
		return nil, err
	}
	return NewCreateIndexData(indexName, tableName, fieldName), nil
}
