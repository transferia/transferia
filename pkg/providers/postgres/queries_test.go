package postgres

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
)

func TestAddEnumValsQuery(t *testing.T) {
	tests := []struct {
		name       string
		currentCol abstract.ColSchema
		newCol     abstract.ColSchema
		wantQuery  []string
		wantErr    bool
	}{
		{
			name: "append only",
			currentCol: abstract.ColSchema{
				OriginalType: "pg:my_enum",
				Properties: map[abstract.PropertyKey]any{
					EnumAllValues: []string{"val1", "val2"},
				},
			},
			newCol: abstract.ColSchema{
				OriginalType: "pg:my_enum",
				Properties: map[abstract.PropertyKey]any{
					EnumAllValues: []string{"val1", "val2", "val3"},
				},
			},
			wantQuery: []string{`ALTER TYPE my_enum ADD VALUE IF NOT EXISTS 'val3'`},
		},
		{
			name: "insert into the middle",
			currentCol: abstract.ColSchema{
				OriginalType: "pg:my_enum",
				Properties: map[abstract.PropertyKey]any{
					EnumAllValues: []string{"val1", "val3"},
				},
			},
			newCol: abstract.ColSchema{
				OriginalType: "pg:my_enum",
				Properties: map[abstract.PropertyKey]any{
					EnumAllValues: []string{"val1", "val2", "val3"},
				},
			},
			wantQuery: []string{`ALTER TYPE my_enum ADD VALUE IF NOT EXISTS 'val2' BEFORE 'val3'`},
		},
		{
			name: "prepend",
			currentCol: abstract.ColSchema{
				OriginalType: "pg:my_enum",
				Properties: map[abstract.PropertyKey]any{
					EnumAllValues: []string{"val2", "val3"},
				},
			},
			newCol: abstract.ColSchema{
				OriginalType: "pg:my_enum",
				Properties: map[abstract.PropertyKey]any{
					EnumAllValues: []string{"val1", "val2", "val3"},
				},
			},
			wantQuery: []string{`ALTER TYPE my_enum ADD VALUE IF NOT EXISTS 'val1' BEFORE 'val2'`},
		},
		{
			name: "drop",
			currentCol: abstract.ColSchema{
				OriginalType: "pg:my_enum",
				Properties: map[abstract.PropertyKey]any{
					EnumAllValues: []string{"val1", "val2", "val3"},
				},
			},
			newCol: abstract.ColSchema{
				OriginalType: "pg:my_enum",
				Properties: map[abstract.PropertyKey]any{
					EnumAllValues: []string{"val1", "val3"},
				},
			},
			wantQuery: []string{`ALTER TYPE my_enum DROP VALUE IF EXISTS 'val2'`},
		},
		{
			name: "drop and insert",
			currentCol: abstract.ColSchema{
				OriginalType: "pg:my_enum",
				Properties: map[abstract.PropertyKey]any{
					EnumAllValues: []string{"val1", "val3", "val4"},
				},
			},
			newCol: abstract.ColSchema{
				OriginalType: "pg:my_enum",
				Properties: map[abstract.PropertyKey]any{
					EnumAllValues: []string{"val1", "val2", "val3"},
				},
			},
			wantQuery: []string{`ALTER TYPE my_enum ADD VALUE IF NOT EXISTS 'val2' BEFORE 'val3'`, `ALTER TYPE my_enum DROP VALUE IF EXISTS 'val4'`},
		},
		{
			name: "no changes",
			currentCol: abstract.ColSchema{
				OriginalType: "pg:my_enum",
				Properties: map[abstract.PropertyKey]any{
					EnumAllValues: []string{"val1", "val2", "val3"},
				},
			},
			newCol: abstract.ColSchema{
				OriginalType: "pg:my_enum",
				Properties: map[abstract.PropertyKey]any{
					EnumAllValues: []string{"val1", "val2", "val3"},
				},
			},
			wantQuery: nil,
		},
		{
			name: "no initial state",
			currentCol: abstract.ColSchema{
				OriginalType: "pg:my_enum",
				Properties: map[abstract.PropertyKey]any{
					EnumAllValues: []string{},
				},
			},
			newCol: abstract.ColSchema{
				OriginalType: "pg:my_enum",
				Properties: map[abstract.PropertyKey]any{
					EnumAllValues: []string{"val1", "val2", "val3"},
				},
			},
			wantQuery: []string{"ALTER TYPE my_enum ADD VALUE IF NOT EXISTS 'val1'", "ALTER TYPE my_enum ADD VALUE IF NOT EXISTS 'val2'", "ALTER TYPE my_enum ADD VALUE IF NOT EXISTS 'val3'"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := addEnumValsQuery(tt.currentCol, tt.newCol)
			if (err != nil) != tt.wantErr {
				t.Errorf("addEnumValsQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			require.Equal(t, tt.wantQuery, got)
		})
	}
}

func TestAddColsQuery(t *testing.T) {
	tests := []struct {
		name    string
		ftn     string
		added   []abstract.ColSchema
		want    string
		wantErr bool
	}{
		{
			name: "single column",
			ftn:  "public.test_table",
			added: []abstract.ColSchema{
				{
					ColumnName:   "id",
					OriginalType: "pg:integer",
				},
			},
			want: `ALTER TABLE public.test_table ADD COLUMN IF NOT EXISTS "id" integer`,
		},
		{
			name: "multiple columns",
			ftn:  "public.test_table",
			added: []abstract.ColSchema{
				{
					ColumnName:   "id",
					OriginalType: "pg:integer",
				},
				{
					ColumnName:   "name",
					OriginalType: "pg:text",
				},
			},
			want: `ALTER TABLE public.test_table ADD COLUMN IF NOT EXISTS "id" integer,ADD COLUMN IF NOT EXISTS "name" text`,
		},
		{
			name: "with expr",
			ftn:  "public.test_table",
			added: []abstract.ColSchema{
				{
					ColumnName:   "created_at",
					OriginalType: "pg:timestamp",
					Expression:   "pg:DEFAULT CURRENT_TIMESTAMP",
				},
			},
			want: `ALTER TABLE public.test_table ADD COLUMN IF NOT EXISTS "created_at" timestamp DEFAULT CURRENT_TIMESTAMP`,
		},
		{
			name: "with custom type",
			ftn:  "public.test_table",
			added: []abstract.ColSchema{
				{
					ColumnName:   "status",
					OriginalType: "pg:my_schema.status_enum",
				},
			},
			want: `ALTER TABLE public.test_table ADD COLUMN IF NOT EXISTS "status" my_schema.status_enum`,
		},
		{
			name: "type conversion",
			ftn:  "public.test_table",
			added: []abstract.ColSchema{
				{
					ColumnName: "count",
					DataType:   "int64",
				},
			},
			want: `ALTER TABLE public.test_table ADD COLUMN IF NOT EXISTS "count" bigint`,
		},
		{
			name: "invalid type error",
			ftn:  "public.test_table",
			added: []abstract.ColSchema{
				{
					ColumnName: "invalid",
					DataType:   "invalid_type",
				},
			},
			wantErr: true,
		},
		{
			name: "not null",
			ftn:  "public.test_table",
			added: []abstract.ColSchema{
				{
					ColumnName:   "email",
					OriginalType: "pg:text",
					Required:     true,
				},
			},
			want: `ALTER TABLE public.test_table ADD COLUMN IF NOT EXISTS "email" text NOT NULL`,
		},
		{
			name: "default not null",
			ftn:  "public.test_table",
			added: []abstract.ColSchema{
				{
					ColumnName:   "updated_at",
					OriginalType: "pg:timestamp",
					Expression:   "pg:DEFAULT CURRENT_TIMESTAMP",
					Required:     true,
				},
			},
			want: `ALTER TABLE public.test_table ADD COLUMN IF NOT EXISTS "updated_at" timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL`,
		},
		{
			name: "camel case",
			ftn:  "public.test_table",
			added: []abstract.ColSchema{
				{
					ColumnName:   "UpdatedAt",
					OriginalType: "pg:timestamp",
					Expression:   "pg:DEFAULT CURRENT_TIMESTAMP",
					Required:     true,
				},
			},
			want: `ALTER TABLE public.test_table ADD COLUMN IF NOT EXISTS "UpdatedAt" timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := addColsQuery(tt.ftn, tt.added)
			if (err != nil) != tt.wantErr {
				t.Errorf("addColsQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("addColsQuery():\n\t%v\nwant\n\t%v", got, tt.want)
			}
		})
	}
}

func TestCreateTableQuery(t *testing.T) {
	tests := []struct {
		name    string
		ftn     string
		schema  []abstract.ColSchema
		want    string
		wantErr bool
	}{
		{
			name: "simple",
			ftn:  "public.test_table",
			schema: []abstract.ColSchema{
				{
					ColumnName:   "id",
					OriginalType: "pg:integer",
					PrimaryKey:   true,
				},
				{
					ColumnName:   "name",
					OriginalType: "pg:text",
				},
			},
			want: `CREATE TABLE IF NOT EXISTS public.test_table ("id" integer,"name" text, primary key ("id"))`,
		},
		{
			name: "composite pkey",
			ftn:  "public.test_table",
			schema: []abstract.ColSchema{
				{
					ColumnName:   "user_id",
					OriginalType: "pg:integer",
					PrimaryKey:   true,
				},
				{
					ColumnName:   "role_id",
					OriginalType: "pg:integer",
					PrimaryKey:   true,
				},
				{
					ColumnName:   "created_at",
					OriginalType: "pg:timestamp",
				},
			},
			want: `CREATE TABLE IF NOT EXISTS public.test_table ("user_id" integer,"role_id" integer,"created_at" timestamp, primary key ("user_id","role_id"))`,
		},
		{
			name: "with default",
			ftn:  "public.test_table",
			schema: []abstract.ColSchema{
				{
					ColumnName:   "id",
					OriginalType: "pg:integer",
					PrimaryKey:   true,
				},
				{
					ColumnName:   "created_at",
					OriginalType: "pg:timestamp",
					Expression:   "pg:DEFAULT CURRENT_TIMESTAMP",
				},
				{
					ColumnName:   "is_active",
					OriginalType: "pg:boolean",
					Expression:   "pg:DEFAULT true",
				},
			},
			want: `CREATE TABLE IF NOT EXISTS public.test_table ("id" integer,"created_at" timestamp DEFAULT CURRENT_TIMESTAMP,"is_active" boolean DEFAULT true, primary key ("id"))`,
		},
		{
			name: "with custom types",
			ftn:  "public.test_table",
			schema: []abstract.ColSchema{
				{
					ColumnName:   "id",
					OriginalType: "pg:integer",
					PrimaryKey:   true,
				},
				{
					ColumnName:   "status",
					OriginalType: "pg:my_schema.status_enum",
				},
				{
					ColumnName:   "metadata",
					OriginalType: "pg:my_schema.metadata_type",
				},
			},
			want: `CREATE TABLE IF NOT EXISTS public.test_table ("id" integer,"status" my_schema.status_enum,"metadata" my_schema.metadata_type, primary key ("id"))`,
		},
		{
			name: "not null",
			ftn:  "public.test_table",
			schema: []abstract.ColSchema{
				{
					ColumnName:   "id",
					OriginalType: "pg:integer",
					PrimaryKey:   true,
					Required:     true,
				},
				{
					ColumnName:   "email",
					OriginalType: "pg:text",
					Required:     true,
				},
				{
					ColumnName:   "phone",
					OriginalType: "pg:text",
				},
			},
			want: `CREATE TABLE IF NOT EXISTS public.test_table ("id" integer NOT NULL,"email" text NOT NULL,"phone" text, primary key ("id"))`,
		},
		{
			name: "with array",
			ftn:  "public.test_table",
			schema: []abstract.ColSchema{
				{
					ColumnName:   "id",
					OriginalType: "pg:integer",
					PrimaryKey:   true,
				},
				{
					ColumnName:   "tags",
					OriginalType: "pg:text[]",
				},
				{
					ColumnName:   "numbers",
					OriginalType: "pg:integer[]",
				},
			},
			want: `CREATE TABLE IF NOT EXISTS public.test_table ("id" integer,"tags" text[],"numbers" integer[], primary key ("id"))`,
		},
		{
			name: "with json",
			ftn:  "public.test_table",
			schema: []abstract.ColSchema{
				{
					ColumnName:   "id",
					OriginalType: "pg:integer",
					PrimaryKey:   true,
				},
				{
					ColumnName:   "data",
					OriginalType: "pg:jsonb",
				},
				{
					ColumnName:   "config",
					OriginalType: "pg:json",
				},
			},
			want: `CREATE TABLE IF NOT EXISTS public.test_table ("id" integer,"data" jsonb,"config" json, primary key ("id"))`,
		},
		{
			name: "invalid type",
			ftn:  "public.test_table",
			schema: []abstract.ColSchema{
				{
					ColumnName: "id",
					DataType:   "invalid_type",
				},
			},
			wantErr: true,
		},
		{
			name: "camel case",
			ftn:  "public.test_table",
			schema: []abstract.ColSchema{
				{
					ColumnName:   "UpdatedAt",
					OriginalType: "pg:timestamp",
					Expression:   "pg:DEFAULT CURRENT_TIMESTAMP",
					Required:     true,
				},
			},
			want: `CREATE TABLE IF NOT EXISTS public.test_table ("UpdatedAt" timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL)`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := CreateTableQuery(tt.ftn, tt.schema)
			if (err != nil) != tt.wantErr {
				t.Errorf("CreateTableQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("CreateTableQuery()\n\t%v\nwant\n\t%v", got, tt.want)
			}
		})
	}
}

func TestCreateEnumQuery(t *testing.T) {
	tests := []struct {
		name          string
		col           abstract.ColSchema
		expectedQuery string
		wantErr       bool
	}{
		{
			name: "simple enum with single value",
			col: abstract.ColSchema{
				OriginalType: `pg:"status"`,
				Properties: map[abstract.PropertyKey]any{
					EnumAllValues: []string{"active"},
				},
			},
			expectedQuery: `
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT
            1
        FROM
            pg_type
        WHERE
            typname = 'status') THEN
    CREATE TYPE "status" AS ENUM ('active');
	END IF;
END;
$$;`,
		},
		{
			name: "enum with multiple values",
			col: abstract.ColSchema{
				OriginalType: `pg:"user_role"`,
				Properties: map[abstract.PropertyKey]any{
					EnumAllValues: []string{"admin", "user", "guest"},
				},
			},
			expectedQuery: `
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT
            1
        FROM
            pg_type
        WHERE
            typname = 'user_role') THEN
    CREATE TYPE "user_role" AS ENUM ('admin','user','guest');
	END IF;
END;
$$;`,
		},
		{
			name: "invalid original type format",
			col: abstract.ColSchema{
				OriginalType: "invalid_format",
				Properties: map[abstract.PropertyKey]any{
					EnumAllValues: []string{"value1", "value2"},
				},
			},
			wantErr: true,
		},
		{
			name: "empty enum values",
			col: abstract.ColSchema{
				OriginalType: `pg:"empty_enum"`,
				Properties: map[abstract.PropertyKey]any{
					EnumAllValues: []string{},
				},
			},
			expectedQuery: `
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT
            1
        FROM
            pg_type
        WHERE
            typname = 'empty_enum') THEN
    CREATE TYPE "empty_enum" AS ENUM ();
	END IF;
END;
$$;`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query, err := createEnumQuery(tt.col)
			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.expectedQuery, query)
		})
	}
}
