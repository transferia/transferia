package postgres

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTranslateOracleDefault(t *testing.T) {
	tests := []struct {
		name  string
		input interface{}
		want  interface{}
	}{
		// Non-string values: pass-through
		{name: "int value", input: 42, want: 42},

		// Oracle function mapping
		{name: "SYSDATE", input: "SYSDATE", want: "now()"},
		{name: "SYSTIMESTAMP", input: "SYSTIMESTAMP", want: "now()"},
		{name: "CURRENT_TIMESTAMP", input: "CURRENT_TIMESTAMP", want: "CURRENT_TIMESTAMP"},
		{name: "CURRENT_DATE", input: "CURRENT_DATE", want: "CURRENT_DATE"},

		// Case-insensitive
		{name: "sysdate lowercase", input: "sysdate", want: "now()"},

		// Literals pass-through
		{name: "integer literal", input: "42", want: "42"},
		{name: "string literal", input: "'ACTIVE'", want: "'ACTIVE'"},
		{name: "expression", input: "TO_DATE('2024-01-01','YYYY-MM-DD')", want: "TO_DATE('2024-01-01','YYYY-MM-DD')"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := translateOracleDefault(tt.input)
			require.Equal(t, tt.want, got)
		})
	}
}
