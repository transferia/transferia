package mysql2ch

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/abstract"
)

func RightStringToBase64BytesComparator(l interface{}, lCol abstract.ColSchema, r interface{}, rCol abstract.ColSchema, _ bool) (comparable bool, result bool, err error) {
	lOriginalIsMySQL := strings.HasPrefix(lCol.OriginalType, "mysql:")
	rOriginalIsClickHouse := strings.HasPrefix(rCol.OriginalType, "ch:")
	if !lOriginalIsMySQL || !rOriginalIsClickHouse {
		return false, false, nil
	}

	lBytes, lIsBytes := l.([]byte)
	if !lIsBytes {
		return false, false, nil
	}

	rString, rIsString := r.(string)
	if !rIsString {
		return false, false, nil
	}

	if len(rString) < 2 {
		return false, false, xerrors.Errorf("the right string '%s' does not have surrounding double quotes", rString)
	}
	rDecodedBytes, err := base64.StdEncoding.DecodeString(rString[1 : len(rString)-1]) // remove trailing double quotes
	if err != nil {
		return false, false, nil
	}

	return true, bytes.Equal(lBytes, rDecodedBytes), nil
}

func MySQLBytesToStringOptionalComparator(l interface{}, lCol abstract.ColSchema, r interface{}, rCol abstract.ColSchema, _ bool) (comparable bool, result bool, err error) {
	lOriginalIsMySQL := strings.HasPrefix(lCol.OriginalType, "mysql:")
	rOriginalIsMySQL := strings.HasPrefix(rCol.OriginalType, "mysql:")

	if !lOriginalIsMySQL && !rOriginalIsMySQL {
		return false, false, nil
	}

	if lOriginalIsMySQL {
		l = maybeBytesToString(l, r)
	}
	if rOriginalIsMySQL {
		r = maybeBytesToString(r, l)
	}

	if fmt.Sprintf("%v", l) == fmt.Sprintf("%v", r) {
		return true, true, nil
	}
	return false, false, nil
}

func maybeBytesToString(v any, other any) any {
	if _, otherIsString := other.(string); otherIsString {
		if vB, vIsB := v.([]byte); vIsB {
			return string(vB)
		}
	}
	return v
}
