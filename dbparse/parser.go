package dbparse

import (
	"github.com/teru01/simpledb-go/dbconstant"
	"github.com/teru01/simpledb-go/dbquery"
)

type Parser struct {
	lex *Lexer
}

func NewParser(s string) *Parser {
	return &Parser{
		lex: NewLexer(s),
	}
}

func (p *Parser) Field() (string, error) {
	return p.lex.EatIdentifier()
}

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

func (p *Parser) Predicate() (*dbquery.Predicate, error) {
	term, err := p.Term()
	if err != nil {
		return nil, err
	}
	pred := dbquery.NewPredicate(term)
	for {
		if p.lex.IsNextKeyword("and") {
			if err := p.lex.EatKeyword("and"); err != nil {
				return nil, err
			}
			term, err := p.Term()
			if err != nil {
				return nil, err
			}
			pred.ConjoinWith(dbquery.NewPredicate(term))
		} else {
			break
		}
	}
	return pred, nil
}

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

func (p *Parser) selectList() ([]string, error) {
	fields := []string{}
	for {
		field, err := p.Field()
		if err != nil {
			return nil, err
		}
		fields = append(fields, field)
		if !p.lex.IsNextDelimiter(',') {
			break
		}
		if err := p.lex.EatDelimiter(','); err != nil {
			return nil, err
		}
	}
	return fields, nil
}

func (p *Parser) tableList() ([]string, error) {
	tables := []string{}
	for {
		table, err := p.lex.EatIdentifier()
		if err != nil {
			return nil, err
		}
		tables = append(tables, table)
		if !p.lex.IsNextDelimiter(',') {
			break
		}
		if err := p.lex.EatDelimiter(','); err != nil {
			return nil, err
		}
	}
	return tables, nil
}

func (p *Parser) UpdateCmd() {
	if p.lex.IsNextKeyword("insert") {

	}
}

func (p *Parser) create() {

}
