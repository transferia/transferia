package postgres

import (
	"strings"

	"github.com/jackc/pgtype"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
)

const (
	UserDefinedEnum      = "USER-DEFINED:ENUM"
	UserDefinedDomain    = "USER-DEFINED:DOMAIN"
	UserDefinedComposite = "USER-DEFINED:COMPOSITE"
	UserDefinedHStore    = "USER-DEFINED:hstore"
	UserDefinedLTree     = "USER-DEFINED:ltree"
	UserDefinedCIText    = "USER-DEFINED:citext"

	PgUserDefinedEnum      = "pg:" + UserDefinedEnum
	PgUserDefinedDomain    = "pg:" + UserDefinedDomain
	PgUserDefinedComposite = "pg:" + UserDefinedComposite
	PgUserDefinedHStore    = "pg:" + UserDefinedHStore
	PgUserDefinedLTree     = "pg:" + UserDefinedLTree
	PgUserDefinedCIText    = "pg:" + UserDefinedCIText

	PgUserDefined = "pg:USER-DEFINED:"
)

var UserDefinedHStoreUppercase string

func init() {
	UserDefinedHStoreUppercase = strings.ToUpper(UserDefinedHStore)
}

func IsPgUserDefinedEnum(originalType string) (string, bool) {
	isFound := strings.HasPrefix(originalType, PgUserDefinedEnum+":") // for example, hstore: 'pg:USER-DEFINED:ENUM:my_enum'
	if !isFound {
		return "", false
	}
	typeName := originalType[len(PgUserDefinedEnum)+1:]
	return typeName, true
}

func IsPgUserDefinedComposite(originalType string) (string, bool) {
	isFound := strings.HasPrefix(originalType, PgUserDefinedComposite+":") // for example, hstore: 'pg:USER-DEFINED:COMPOSITE:geometry'
	if !isFound {
		return "", false
	}
	typeName := originalType[len(PgUserDefinedComposite)+1:]
	return typeName, true
}

func IsUserDefinedType(col *abstract.ColSchema) bool {
	return strings.HasPrefix(col.OriginalType, "pg:USER-DEFINED") // for example, hstore: 'pg:USER-DEFINED:hstore'
}

func deriveUserDefinedPgDataType(col *abstract.ColSchema) (string, error) {
	if result, ok := IsPgUserDefinedEnum(col.OriginalType); ok {
		return result, nil
	}
	if result, ok := IsPgUserDefinedComposite(col.OriginalType); ok {
		return result, nil
	}
	switch col.OriginalType {
	case PgUserDefinedDomain:
		return "", xerrors.New("'domain' is not supported")
	case PgUserDefinedHStore:
		return col.OriginalType[len(PgUserDefined):], nil
	case PgUserDefinedLTree:
		return "", xerrors.New("'ltree' is not supported")
	case PgUserDefinedCIText:
		return col.OriginalType[len(PgUserDefined):], nil
	default:
		return "", xerrors.Errorf("unknown user-defined type: %q", col.OriginalType)
	}
}

func buildOriginalTypeForUserDefined(dataType string, domainName *string, unpackEnumValuesF func() []string) string {
	if len(unpackEnumValuesF()) != 0 {
		// ENUM
		// {
		//  "host": "timmyb32r-dev4.vla.yp-c.yandex.net",
		//  "tSchema": "public",
		//  "tName": "user_types",
		//  "cName": "enum_col",
		//  "cDefault": "",
		//  "dataType": "user_type_enum",
		//  "dtSchema": "public",
		//  "domainName": null,
		//  "dataTypeUnderlyingUnderDomain": "USER-DEFINED",
		//  "allEnumValues": {},
		//  "expr": "",
		//  "dummy": 3,
		//  "isNullable": true
		// }
		return UserDefinedEnum + ":" + dataType
	} else if domainName != nil {
		// DOMAIN
		return UserDefinedDomain
	} else if dataType == "hstore" {
		// {
		//  "host": "timmyb32r-dev4.vla.yp-c.yandex.net",
		//  "tSchema": "public",
		//  "tName": "user_types",
		//  "cName": "hstore_col",
		//  "cDefault": "",
		//  "dataType": "hstore",
		//  "dtSchema": "public",
		//  "domainName": null,
		//  "dataTypeUnderlyingUnderDomain": "USER-DEFINED",
		//  "allEnumValues": {},
		//  "expr": "",
		//  "dummy": 4,
		//  "isNullable": true
		// }
		return UserDefinedHStore
	} else if dataType == "ltree" {
		return UserDefinedLTree
	} else if dataType == "citext" {
		// {
		//  "host": "timmyb32r-dev4.vla.yp-c.yandex.net",
		//  "tSchema": "public",
		//  "tName": "user_types",
		//  "cName": "citext_col",
		//  "cDefault": "",
		//  "dataType": "citext",
		//  "dtSchema": "public",
		//  "domainName": null,
		//  "dataTypeUnderlyingUnderDomain": "USER-DEFINED",
		//  "allEnumValues": {},
		//  "expr": "",
		//  "dummy": 5,
		//  "isNullable": true
		// }
		return UserDefinedCIText
	} else {
		// here are 'ranges' & 'composite literal' - we will assume it's composite literal
		// {
		//  "host": "timmyb32r-dev4.vla.yp-c.yandex.net",
		//  "tSchema": "public",
		//  "tName": "user_types",
		//  "cName": "price_limits_col",
		//  "cDefault": "",
		//  "dataType": "user_type_composite_type",
		//  "dtSchema": "public",
		//  "domainName": null,
		//  "dataTypeUnderlyingUnderDomain": "USER-DEFINED",
		//  "allEnumValues": {},
		//  "expr": "",
		//  "dummy": 2,
		//  "isNullable": true
		// }
		return UserDefinedComposite + ":" + dataType
	}
}

func unpackEnumValues(in interface{}) []string {
	if in != nil {
		pgGenericArray := in.(*GenericArray)
		arr, _ := pgGenericArray.ExtractValue(pgtype.NewConnInfo())
		var result []string
		for _, el := range arr.([]interface{}) {
			result = append(result, el.(string))
		}
		return result
	}
	return nil
}
