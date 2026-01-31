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

// func (p *Parser) Term() (*dbquery.Term, error) {
// 	lhs, err := p.Expression()
// 	if err != nil {
// 		return nil, err
// 	}
// 	if err := p.lex.EatDelimiter('='); err != nil {
// 		return nil, err
// 	}
// 	rhs, err := p.Expression()
// 	if err != nil {
// 		return nil, err
// 	}
// 	return dbquery.NewTerm(lhs, rhs), nil
// }
