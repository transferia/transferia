package abstract

import (
	"strings"

	"github.com/antlr4-go/antlr/v4"
)

type Token struct {
	AntlrToken antlr.Token
	LowerText  string
}

func NewToken(token antlr.Token) *Token {
	return &Token{
		AntlrToken: token,
		LowerText:  strings.ToLower(token.GetText()),
	}
}
