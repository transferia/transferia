//nolint:descriptiveerrors
package log_miner

import (
	"strings"

	"github.com/transferia/transferia/library/go/core/xerrors"
	oracle_common "github.com/transferia/transferia/pkg/providers/oracle/common"
)

type parseResult struct {
	SchemaName string
	TableName  string
	NewValues  map[string]*string
	OldValues  map[string]*string
}

func (result *parseResult) TableID() *oracle_common.TableID {
	return oracle_common.NewTableID(result.SchemaName, result.TableName)
}

func (result *parseResult) TableFullName() string {
	return oracle_common.CreateSQLName(result.SchemaName, result.TableName)
}

// TODO: validate ParseResult?

func parseLogMinerSQL(sql string) (*parseResult, error) {
	sqlChars := []rune(sql)
	i := 0
	switch startToken := getToken(sqlChars, &i); startToken {
	case "insert":
		//nolint:descriptiveerrors
		return parseInsert(sqlChars, &i)
	case "delete":
		//nolint:descriptiveerrors
		return parseDelete(sqlChars, &i)
	case "update":
		//nolint:descriptiveerrors
		return parseUpdate(sqlChars, &i)
	default:
		return nil, xerrors.Errorf("Unsupported sql statement type: %v", startToken)
	}
}

func convertToName(token string) (string, error) {
	const nameQuote = "\""
	if !strings.HasPrefix(token, "\"") || !strings.HasSuffix(token, "\"") {
		return token, nil
	}
	nameQuoteLen := len(nameQuote)
	name := token[nameQuoteLen : len(token)-nameQuoteLen]
	return name, nil
}

func parseInsert(sqlChars []rune, sqlIndex *int) (*parseResult, error) {
	result := new(parseResult)

	if getToken(sqlChars, sqlIndex) != "into" {
		return nil, xerrors.New("Can't parse sql insert statement")
	}

	if err := parseTable(sqlChars, sqlIndex, result); err != nil {
		return nil, xerrors.Errorf("Can't parse sql insert statement: %w", err)
	}

	if getToken(sqlChars, sqlIndex) != "(" {
		return nil, xerrors.New("Can't parse sql insert statement")
	}

	columnNames := []string{}
	for {
		columnName := getToken(sqlChars, sqlIndex)
		if columnName == "" {
			return nil, xerrors.New("Can't parse sql insert statement")
		}
		if columnName == "," {
			continue
		}
		if columnName == ")" {
			break
		}

		columnName, err := convertToName(columnName)
		if err != nil {
			return nil, xerrors.Errorf("Can't parse sql insert statement: %w", err)
		}

		columnNames = append(columnNames, columnName)
	}

	if getToken(sqlChars, sqlIndex) != "values" || getToken(sqlChars, sqlIndex) != "(" {
		return nil, xerrors.New("Can't parse sql insert statement")
	}

	columnValues := []*string{}
	for {
		columnValue := getValue(sqlChars, sqlIndex)
		if columnValue == "" {
			return nil, xerrors.New("Can't parse sql insert statement")
		}
		if columnValue == "," {
			continue
		}
		if columnValue == ")" {
			break
		}

		columnValues = append(columnValues, &columnValue)
	}

	if len(columnNames) != len(columnValues) {
		return nil, xerrors.New("Can't parse sql insert statement")
	}

	result.NewValues = map[string]*string{}
	for i := 0; i < len(columnNames); i++ {
		result.NewValues[columnNames[i]] = columnValues[i]
	}

	return result, nil
}

func parseDelete(sqlChars []rune, sqlIndex *int) (*parseResult, error) {
	result := new(parseResult)

	if getToken(sqlChars, sqlIndex) != "from" {
		return nil, xerrors.New("Can't parse sql delete statement")
	}

	if err := parseTable(sqlChars, sqlIndex, result); err != nil {
		return nil, xerrors.Errorf("Can't parse sql delete statement: %w", err)
	}

	if getToken(sqlChars, sqlIndex) != "where" {
		return nil, xerrors.New("Can't parse sql where statement")
	}

	oldValues, err := parseWhere(sqlChars, sqlIndex)
	if err != nil {
		return nil, xerrors.Errorf("Can't parse sql delete statement: %w", err)
	}
	result.OldValues = oldValues

	return result, nil
}

func parseUpdate(sqlChars []rune, sqlIndex *int) (*parseResult, error) {
	result := new(parseResult)

	if err := parseTable(sqlChars, sqlIndex, result); err != nil {
		return nil, xerrors.Errorf("Can't parse sql update statement: %w", err)
	}

	if getToken(sqlChars, sqlIndex) != "set" {
		return nil, xerrors.New("Can't parse sql update statement")
	}

	result.NewValues = map[string]*string{}
	for {
		columnName := getToken(sqlChars, sqlIndex)
		if columnName == "" {
			return nil, xerrors.New("Can't parse sql update statement")
		}
		if columnName == "," {
			continue
		}
		if columnName == "where" {
			break
		}

		columnName, err := convertToName(columnName)
		if err != nil {
			return nil, xerrors.Errorf("Can't parse sql update statement: %w", err)
		}

		if getToken(sqlChars, sqlIndex) != "=" {
			return nil, xerrors.New("Can't parse sql update statement")
		}

		columnValue := getValue(sqlChars, sqlIndex)
		if columnValue == "" {
			return nil, xerrors.New("Can't parse sql where statement")
		}

		result.NewValues[columnName] = &columnValue
	}

	oldValues, err := parseWhere(sqlChars, sqlIndex)
	if err != nil {
		return nil, xerrors.Errorf("Can't parse sql update statement: %w", err)
	}
	result.OldValues = oldValues

	return result, nil
}

func parseTable(sqlChars []rune, sqlIndex *int, result *parseResult) error {
	var err error

	result.SchemaName, err = convertToName(getToken(sqlChars, sqlIndex))
	if err != nil {
		return xerrors.Errorf("Can't parse schema name: %w", err)
	}

	if getToken(sqlChars, sqlIndex) != "." {
		return xerrors.New("Can't parse table name")
	}

	result.TableName, err = convertToName(getToken(sqlChars, sqlIndex))
	if err != nil {
		return xerrors.Errorf("Can't parse table name: %w", err)
	}

	if result.SchemaName == "" {
		return xerrors.New("Can't parse schema name")
	}
	if result.TableName == "" {
		return xerrors.New("Can't parse table name")
	}

	return nil
}

func parseWhere(sqlChars []rune, sqlIndex *int) (map[string]*string, error) {
	values := map[string]*string{}
	for {
		columnName := getToken(sqlChars, sqlIndex)
		if columnName == "" {
			return nil, xerrors.New("Can't parse sql where statement")
		}
		columnName, err := convertToName(columnName)
		if err != nil {
			return nil, xerrors.Errorf("Can't parse sql where statement: %w", err)
		}

		switch nextToken := getToken(sqlChars, sqlIndex); nextToken {
		case "=":
			columnValue := getValue(sqlChars, sqlIndex)
			if columnValue == "" {
				return nil, xerrors.New("Can't parse sql where statement")
			}
			values[columnName] = &columnValue
		case "IS":
			nextToken = getToken(sqlChars, sqlIndex)
			if nextToken != "NULL" {
				return nil, xerrors.New("Can't parse sql where statement")
			}
			values[columnName] = &nextToken
		default:
			return nil, xerrors.New("Can't parse sql where statement")
		}

		switch nextToken := getToken(sqlChars, sqlIndex); nextToken {
		case "and":
			continue
		case "":
			return values, nil
		default:
			return nil, xerrors.New("Can't parse sql where statement")
		}
	}
}

func getToken(sqlChars []rune, sqlIndex *int) string {
	if *sqlIndex >= len(sqlChars) {
		return ""
	}

	nameMode := false
	stringMode := false
	builder := strings.Builder{}
	for ; *sqlIndex < len(sqlChars); *sqlIndex++ {
		char := sqlChars[*sqlIndex]
		if nameMode {
			// TODO: processing of escaped chars
			builder.WriteRune(char)
			if char == '"' { // End of string value
				*sqlIndex++
				break
			}
		} else if stringMode {
			builder.WriteRune(char)
			if char == '\'' {
				rightBorder := *sqlIndex + 2
				if rightBorder >= len(sqlChars) {
					rightBorder = len(sqlChars) - 1
				}
				charPlusLookahead := string(sqlChars[*sqlIndex:rightBorder])
				if charPlusLookahead == "''" { // just escaped single quote
					*sqlIndex++
				} else {
					if char == '\'' { // End of string value
						*sqlIndex++
						break
					}
				}
			}
		} else {
			if char == ' ' || char == '\t' || char == '\n' { // Delimiters
				if builder.Len() == 0 {
					continue
				} else {
					break
				}
			} else if char == '(' || char == ')' || char == '=' || char == '.' || char == ',' { // One char tokens
				if builder.Len() == 0 {
					*sqlIndex++
					builder.WriteRune(char)
				}
				break
			} else if char == '"' { // Begin of name value
				if builder.Len() != 0 {
					break
				} else {
					nameMode = true
					builder.WriteRune(char)
				}
			} else if char == '\'' { // Begin of string value
				if builder.Len() != 0 {
					break
				} else {
					stringMode = true
					builder.WriteRune(char)
				}
			} else if char == ';' { // End of sql statment
				break
			} else {
				builder.WriteRune(char)
			}
		}
	}
	return builder.String()
}

func getValue(sqlChars []rune, sqlIndex *int) string {
	if *sqlIndex >= len(sqlChars) {
		return ""
	}
	nextIndex := *sqlIndex

	token := getToken(sqlChars, &nextIndex)
	if token == "," || token == ")" || token == "where" || token == "and" { // Value stop tokens
		*sqlIndex = nextIndex
		return token
	}

	openBracketsCount := 0
	prevNextIndex := nextIndex
	for nextIndex < len(sqlChars) {
		nextToken := getToken(sqlChars, &nextIndex)
		if nextToken == "(" {
			openBracketsCount++
		} else {
			if openBracketsCount > 0 {
				if nextToken == ")" {
					openBracketsCount--
				}
			} else {
				if nextToken == "," || nextToken == ")" || nextToken == "where" || nextToken == "and" { // Value stop tokens
					break
				}
			}
		}
		prevNextIndex = nextIndex
	}

	builder := strings.Builder{}
	for i := *sqlIndex; i < prevNextIndex; i++ {
		char := sqlChars[i]
		if builder.Len() == 0 && (char == ' ' || char == '\t' || char == '\n') { // Delimiters
			continue
		}
		builder.WriteRune(char)
	}
	*sqlIndex = prevNextIndex
	return builder.String()
}
