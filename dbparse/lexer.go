package dbparse

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"text/scanner"

	"github.com/teru01/simpledb-go/dberr"
)

type Lexer struct {
	keywords []string
	scanner  scanner.Scanner
}

func NewLexer(s string) *Lexer {
	var scanner scanner.Scanner
	scanner.Init(strings.NewReader(s))
	scanner.Scan()
	return &Lexer{
		keywords: []string{"select", "from", "where", "and",
			"insert", "into", "values", "delete", "update",
			"set", "create", "table", "varchar",
			"int", "view", "as", "index", "on"},
		scanner: scanner,
	}
}

func (l *Lexer) IsNextString() bool {
	next := l.scanner.Peek()
	return next == scanner.String
}

func (l *Lexer) IsNextIdentifier() bool {
	next := l.scanner.Peek()
	return next == scanner.Ident
}

func (l *Lexer) IsNextInt() bool {
	next := l.scanner.Peek()
	return next == scanner.Int
}

func (l *Lexer) EatDelimiter(d rune) error {
	tok := l.scanner.Scan()
	if tok != d {
		return dberr.New(dberr.CodeSyntaxError, fmt.Sprintf("expected delimiter %q but got %q", d, tok), nil)
	}
	return nil
}

func (l *Lexer) EatIntConstant() (int, error) {
	tok := l.scanner.Scan()
	if tok != scanner.Int {
		return 0, dberr.New(dberr.CodeSyntaxError, fmt.Sprintf("expected int but got %q", tok), nil)
	}
	return strconv.Atoi(l.scanner.TokenText())
}

func (l *Lexer) EatStringConstant() (string, error) {
	tok := l.scanner.Scan()
	if tok != scanner.String {
		return "", dberr.New(dberr.CodeSyntaxError, fmt.Sprintf("expected string but got %q", tok), nil)
	}
	return strconv.Unquote(l.scanner.TokenText())
}

func (l *Lexer) EatKeyword(w string) error {
	tok := l.scanner.Scan()
	if tok != scanner.Ident {
		return dberr.New(dberr.CodeSyntaxError, fmt.Sprintf("expected keyword %q but got %q", w, tok), nil)
	}
	if l.scanner.TokenText() != w {
		return dberr.New(dberr.CodeSyntaxError, fmt.Sprintf("expected keyword %q but got %q", w, l.scanner.TokenText()), nil)
	}
	return nil
}

func (l *Lexer) EatIdentifier() (string, error) {
	tok := l.scanner.Scan()
	if tok != scanner.Ident {
		return "", dberr.New(dberr.CodeSyntaxError, fmt.Sprintf("expected identifier but got %q", tok), nil)
	}
	if slices.Contains(l.keywords, l.scanner.TokenText()) {
		return "", dberr.New(dberr.CodeSyntaxError, fmt.Sprintf("using reserved keyword: %q", l.scanner.TokenText()), nil)
	}
	return l.scanner.TokenText(), nil
}
