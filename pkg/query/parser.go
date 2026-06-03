package query

import (
	"fmt"
	"strings"
	"unicode"
)

// ParseQueryString converts a Lucene-style query string into a Node tree.
// Grammar: expr := term | NOT term | expr AND expr | expr OR expr | (expr)
// term := [field:]value
// Uses operator-precedence: NOT > AND > OR.
func ParseQueryString(input string) (*Node, error) {
	p := &qsParser{input: strings.TrimSpace(input), pos: 0}
	node, err := p.parseOr()
	if err != nil {
		return nil, err
	}
	return node, nil
}

type qsParser struct {
	input string
	pos   int
}

func (p *qsParser) peek() string {
	p.skipWS()
	rest := p.input[p.pos:]
	upper := strings.ToUpper(rest)
	if strings.HasPrefix(upper, "AND") && (len(rest) == 3 || !isIdentChar(rune(rest[3]))) {
		return "AND"
	}
	if strings.HasPrefix(upper, "OR") && (len(rest) == 2 || !isIdentChar(rune(rest[2]))) {
		return "OR"
	}
	if strings.HasPrefix(upper, "NOT") && (len(rest) == 3 || !isIdentChar(rune(rest[3]))) {
		return "NOT"
	}
	return ""
}

func (p *qsParser) consume(op string) {
	p.skipWS()
	p.pos += len(op)
}

func (p *qsParser) parseOr() (*Node, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	for p.peek() == "OR" {
		p.consume("OR")
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		left = &Node{
			Type:   NodeBool,
			Should: []*Node{left, right},
		}
	}
	return left, nil
}

func (p *qsParser) parseAnd() (*Node, error) {
	left, err := p.parseNot()
	if err != nil {
		return nil, err
	}
	for {
		op := p.peek()
		if op == "AND" {
			p.consume("AND")
		} else if op == "" && p.hasMore() && p.peek() != "OR" && p.peek() != ")" {
			// Implicit AND: two adjacent terms.
			// Check that next token isn't closing paren.
			p.skipWS()
			if p.pos < len(p.input) && p.input[p.pos] == ')' {
				break
			}
			if p.peek() == "OR" || p.peek() == "NOT" {
				break
			}
		} else {
			break
		}
		right, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		if left.Type == NodeBool && len(left.Should) == 0 && len(left.MustNot) == 0 {
			left.Must = append(left.Must, right)
		} else {
			left = &Node{Type: NodeBool, Must: []*Node{left, right}}
		}
	}
	return left, nil
}

func (p *qsParser) parseNot() (*Node, error) {
	if p.peek() == "NOT" {
		p.consume("NOT")
		child, err := p.parsePrimary()
		if err != nil {
			return nil, err
		}
		return &Node{Type: NodeBool, MustNot: []*Node{child}}, nil
	}
	return p.parsePrimary()
}

func (p *qsParser) parsePrimary() (*Node, error) {
	p.skipWS()
	if p.pos >= len(p.input) {
		return nil, fmt.Errorf("unexpected end of query")
	}
	if p.input[p.pos] == '(' {
		p.pos++ // consume (
		node, err := p.parseOr()
		if err != nil {
			return nil, err
		}
		p.skipWS()
		if p.pos >= len(p.input) || p.input[p.pos] != ')' {
			return nil, fmt.Errorf("missing closing )")
		}
		p.pos++ // consume )
		return node, nil
	}
	return p.parseTerm()
}

func (p *qsParser) parseTerm() (*Node, error) {
	p.skipWS()
	tok := p.readToken()
	if tok == "" {
		return nil, fmt.Errorf("expected term at position %d", p.pos)
	}
	// Check for field:value
	if idx := strings.Index(tok, ":"); idx > 0 {
		field := tok[:idx]
		value := tok[idx+1:]
		if value == "" {
			return nil, fmt.Errorf("empty value for field %q", field)
		}
		// If value has spaces it was quoted; handle by re-reading.
		if strings.HasPrefix(value, `"`) {
			// value was part of quoted string; readToken already handled it.
		}
		return termNode(field, value), nil
	}
	// Bare term: search all text fields (represented as match with field="_all").
	return termNode("_all", tok), nil
}

func termNode(field, value string) *Node {
	if strings.Contains(value, " ") {
		// Phrase query.
		terms := strings.Fields(value)
		return &Node{Type: NodePhrase, Field: field, Terms: terms}
	}
	if strings.HasSuffix(value, "*") {
		return &Node{Type: NodePrefix, Field: field, Value: strings.TrimSuffix(value, "*")}
	}
	return &Node{Type: NodeMatch, Field: field, Value: value}
}

func (p *qsParser) readToken() string {
	p.skipWS()
	if p.pos >= len(p.input) {
		return ""
	}
	// Quoted string.
	if p.input[p.pos] == '"' {
		p.pos++
		start := p.pos
		for p.pos < len(p.input) && p.input[p.pos] != '"' {
			p.pos++
		}
		tok := p.input[start:p.pos]
		if p.pos < len(p.input) {
			p.pos++ // closing "
		}
		return tok
	}
	// Unquoted: read until whitespace or special chars.
	start := p.pos
	for p.pos < len(p.input) {
		r := rune(p.input[p.pos])
		if r == ' ' || r == '\t' || r == '(' || r == ')' {
			break
		}
		p.pos++
	}
	tok := p.input[start:p.pos]
	// Don't consume AND/OR/NOT as term tokens.
	upper := strings.ToUpper(tok)
	if upper == "AND" || upper == "OR" || upper == "NOT" {
		p.pos = start
		return ""
	}
	return tok
}

func (p *qsParser) skipWS() {
	for p.pos < len(p.input) && unicode.IsSpace(rune(p.input[p.pos])) {
		p.pos++
	}
}

func (p *qsParser) hasMore() bool {
	p.skipWS()
	return p.pos < len(p.input)
}

func isIdentChar(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_'
}
