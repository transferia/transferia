package table_name_policy

import (
	"strings"

	"github.com/transferia/transferia/library/go/core/xerrors"
)

type JSONTableNamePolicy int

const (
	JSONTableNamePolicyDebeziumStyle JSONTableNamePolicy = 0
	JSONTableNamePolicyTitle         JSONTableNamePolicy = 1
)

type ProtobufTableNamePolicy int

const (
	ProtobufTableNamePolicyDebeziumStyle ProtobufTableNamePolicy = 0
	ProtobufTableNamePolicyMessageName   ProtobufTableNamePolicy = 1
)

type TableNamePolicyDerived struct {
	JSONTableNamePolicy     JSONTableNamePolicy
	ProtobufTableNamePolicy ProtobufTableNamePolicy
}

type TableNamePolicyManual struct {
	TableName string
}

type TableNamePolicy struct {
	Derived TableNamePolicyDerived
	Manual  TableNamePolicyManual
}

//---

func DefaultDerivedTableNamePolicy() TableNamePolicy {
	return TableNamePolicy{
		Derived: TableNamePolicyDerived{
			JSONTableNamePolicy:     JSONTableNamePolicyDebeziumStyle,
			ProtobufTableNamePolicy: ProtobufTableNamePolicyDebeziumStyle,
		},
		Manual: TableNamePolicyManual{
			TableName: "",
		},
	}
}

func BuildProtobufTableID(tableNamePolicy TableNamePolicy, fullMessageName string) (string, string, error) {
	if tableNamePolicy.Manual.TableName != "" {
		return "", tableNamePolicy.Manual.TableName, nil
	} else {
		switch tableNamePolicy.Derived.ProtobufTableNamePolicy {
		case ProtobufTableNamePolicyDebeziumStyle:
			separatedTitle := strings.Split(fullMessageName, ".")
			if len(separatedTitle) != 4 {
				return "", "", xerrors.Errorf("Can't split recordName '%s' into schema and table names", fullMessageName)
			}
			schemaName := separatedTitle[1]
			tableName := separatedTitle[2]
			return schemaName, tableName, nil
		case ProtobufTableNamePolicyMessageName:
			arr := strings.Split(fullMessageName, ".")
			return "", arr[len(arr)-1], nil
		default:
			return "", "", xerrors.New("invalid ProtobufTableNamePolicy")
		}
	}
}

func BuildJSONTableID(tableNamePolicy TableNamePolicy, title string) (string, string, error) {
	if tableNamePolicy.Manual.TableName != "" {
		return "", tableNamePolicy.Manual.TableName, nil
	} else {
		switch tableNamePolicy.Derived.JSONTableNamePolicy {
		case JSONTableNamePolicyDebeziumStyle:
			separatedTitle := strings.SplitN(title, ".", 2)
			if len(separatedTitle) != 2 {
				return "", "", xerrors.Errorf("Can't split title '%s' from json into schema and table names", title)
			}
			schemaName := separatedTitle[0]
			tableName := separatedTitle[1]
			return schemaName, tableName, nil
		case JSONTableNamePolicyTitle:
			return "", title, nil
		default:
			return "", "", xerrors.New("invalid JSONTableNamePolicy")
		}
	}
}
