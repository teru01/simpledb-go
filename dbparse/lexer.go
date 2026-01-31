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
	keywords  []string
	scanner   scanner.Scanner
	nextToken rune
}

func NewLexer(s string) *Lexer {
	var scanner scanner.Scanner
	scanner.Init(strings.NewReader(s))
	nextToken := scanner.Scan()
	return &Lexer{
		keywords: []string{"select", "from", "where", "and",
			"insert", "into", "values", "delete", "update",
			"set", "create", "table", "varchar",
			"int", "view", "as", "index", "on"},
		scanner:   scanner,
		nextToken: nextToken,
	}
}

func (l *Lexer) IsNextString() bool {
	return l.nextToken == scanner.String
}

func (l *Lexer) IsNextIdentifier() bool {
	return l.nextToken == scanner.Ident
}

func (l *Lexer) IsNextInt() bool {
	return l.nextToken == scanner.Int
}

func (l *Lexer) IsNextKeyword(w string) bool {
	return l.nextToken == scanner.Ident && strings.ToLower(l.scanner.TokenText()) == w
}

func (l *Lexer) IsNextDelimiter(d rune) bool {
	return l.nextToken == d
}

func (l *Lexer) EatDelimiter(d rune) error {
	if l.nextToken != d {
		return dberr.New(dberr.CodeSyntaxError, fmt.Sprintf("expected delimiter %q but got %q", d, l.nextToken), nil)
	}
	l.nextToken = l.scanner.Scan()
	return nil
}

func (l *Lexer) EatIntConstant() (int, error) {
	if l.nextToken != scanner.Int {
		return 0, dberr.New(dberr.CodeSyntaxError, fmt.Sprintf("expected int but got %q", l.nextToken), nil)
	}
	val, err := strconv.Atoi(l.scanner.TokenText())
	if err != nil {
		return 0, dberr.New(dberr.CodeSyntaxError, fmt.Sprintf("invalid int constant: %q", l.scanner.TokenText()), nil)
	}
	l.nextToken = l.scanner.Scan()
	return val, nil
}

func (l *Lexer) EatStringConstant() (string, error) {
	if l.nextToken != scanner.String {
		return "", dberr.New(dberr.CodeSyntaxError, fmt.Sprintf("expected string but got %q", l.nextToken), nil)
	}
	ss := strings.ToLower(l.scanner.TokenText())
	str, err := strconv.Unquote(ss)
	if err != nil {
		return "", dberr.New(dberr.CodeSyntaxError, fmt.Sprintf("invalid string constant: %q", ss), nil)
	}
	l.nextToken = l.scanner.Scan()
	return str, nil
}

func (l *Lexer) EatIdentifier() (string, error) {
	if l.nextToken != scanner.Ident {
		return "", dberr.New(dberr.CodeSyntaxError, fmt.Sprintf("expected identifier but got %q", l.nextToken), nil)
	}
	id := strings.ToLower(l.scanner.TokenText())
	if slices.Contains(l.keywords, id) {
		return "", dberr.New(dberr.CodeSyntaxError, fmt.Sprintf("using reserved keyword: %q", id), nil)
	}
	l.nextToken = l.scanner.Scan()
	return id, nil
}

func (l *Lexer) EatKeyword(w string) error {
	text := strings.ToLower(l.scanner.TokenText())
	if l.nextToken != scanner.Ident || text != w {
		return dberr.New(dberr.CodeSyntaxError, fmt.Sprintf("expected keyword %q but got %q", w, text), nil)
	}
	l.nextToken = l.scanner.Scan()
	return nil
}
