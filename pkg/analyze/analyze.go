package analyze

import (
	"strings"
	"unicode"

	"golang.org/x/text/transform"
	"golang.org/x/text/unicode/norm"
)

// Token is one analyzed term with its source position.
type Token struct {
	Term     string
	Position uint32 // 0-based term position in the field
	StartOff uint32 // byte offset in original text
	EndOff   uint32
}

// Tokenizer splits text into tokens.
type Tokenizer interface {
	Tokenize(text string) []Token
}

// StandardTokenizer performs word boundary splitting + lowercase + ASCII fold.
// Punctuation-only and empty tokens are dropped.
type StandardTokenizer struct{}

// foldASCII lowercases and strips diacritics.
// Creates a fresh transform chain per call — transform.Chain is stateful and not goroutine-safe.
func foldASCII(s string) string {
	s = strings.ToLower(s)
	chain := transform.Chain(
		norm.NFD,
		transform.RemoveFunc(func(r rune) bool { return unicode.Is(unicode.Mn, r) }),
		norm.NFC,
	)
	result, _, err := transform.String(chain, s)
	if err != nil {
		return s
	}
	return result
}

func isWordRune(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsDigit(r)
}

func (StandardTokenizer) Tokenize(text string) []Token {
	var tokens []Token
	pos := uint32(0)
	inWord := false
	wordStart := 0

	for i, r := range text {
		if isWordRune(r) {
			if !inWord {
				wordStart = i
				inWord = true
			}
		} else {
			if inWord {
				term := foldASCII(text[wordStart:i])
				if term != "" {
					tokens = append(tokens, Token{
						Term:     term,
						Position: pos,
						StartOff: uint32(wordStart),
						EndOff:   uint32(i),
					})
					pos++
				}
				inWord = false
			}
		}
	}
	if inWord {
		term := foldASCII(text[wordStart:])
		if term != "" {
			tokens = append(tokens, Token{
				Term:     term,
				Position: pos,
				StartOff: uint32(wordStart),
				EndOff:   uint32(len(text)),
			})
		}
	}
	return tokens
}
