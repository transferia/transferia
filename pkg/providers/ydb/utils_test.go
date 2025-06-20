//go:build !disable_ydb_provider

package ydb

import (
	"reflect"
	"testing"

	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

var demoYdbColumns = []options.Column{
	{
		Name:   "a",
		Type:   types.TypeBool,
		Family: "default",
	},
	{
		Name:   "b",
		Type:   types.TypeInt32,
		Family: "default",
	},
	{
		Name:   "c",
		Type:   types.TypeInt32,
		Family: "default",
	},
}

var (
	demoYdbPrimaryKey = []string{"a"}
	demoYdbTable      = options.Description{
		Name:       "test_table",
		Columns:    demoYdbColumns,
		PrimaryKey: demoYdbPrimaryKey,
	}
)

func Test_filterYdbTableColumns(t *testing.T) {
	type args struct {
		filter      []YdbColumnsFilter
		description options.Description
	}
	tests := []struct {
		name    string
		args    args
		want    []options.Column
		wantErr bool
	}{
		{
			name:    "tests filter not specified (nil)",
			args:    args{filter: nil, description: demoYdbTable},
			want:    demoYdbColumns,
			wantErr: false,
		},
		{
			name:    "tests table name does not match filter regexp",
			args:    args{filter: []YdbColumnsFilter{{TableNamesRegexp: "cockroachdb"}}, description: demoYdbTable},
			want:    demoYdbColumns,
			wantErr: false,
		},
		{
			name: "tests column name does not match filter regexp",
			args: args{
				filter: []YdbColumnsFilter{{
					TableNamesRegexp:  demoYdbTable.Name,
					ColumnNamesRegexp: "d",
					Type:              YdbColumnsBlackList,
				}},
				description: demoYdbTable,
			},
			want:    demoYdbColumns,
			wantErr: false,
		},
		{
			name: "tests blacklist primary key (error)",
			args: args{
				filter: []YdbColumnsFilter{{
					TableNamesRegexp:  demoYdbTable.Name,
					ColumnNamesRegexp: "a",
					Type:              YdbColumnsBlackList,
				}},
				description: demoYdbTable,
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "tests whitelist does not contains primary key (error)",
			args: args{
				filter: []YdbColumnsFilter{{
					TableNamesRegexp:  demoYdbTable.Name,
					ColumnNamesRegexp: "b|c",
					Type:              YdbColumnsWhiteList,
				}},
				description: demoYdbTable,
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "tests blacklist all columns (error)",
			args: args{
				filter: []YdbColumnsFilter{{
					TableNamesRegexp:  demoYdbTable.Name,
					ColumnNamesRegexp: "a|b|c",
					Type:              YdbColumnsBlackList,
				}},
				description: demoYdbTable,
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "tests whitelist with no match to any column",
			args: args{
				filter: []YdbColumnsFilter{{
					TableNamesRegexp:  demoYdbTable.Name,
					ColumnNamesRegexp: "e|f|g",
					Type:              YdbColumnsWhiteList,
				}},
				description: demoYdbTable,
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "tests blacklist columns",
			args: args{
				filter: []YdbColumnsFilter{{
					TableNamesRegexp:  demoYdbTable.Name,
					ColumnNamesRegexp: "b|c",
					Type:              YdbColumnsBlackList,
				}},
				description: demoYdbTable,
			},
			want:    []options.Column{{Name: "a", Type: types.TypeBool, Family: "default"}},
			wantErr: false,
		},
		{
			name: "tests whitelist columns",
			args: args{
				filter: []YdbColumnsFilter{{
					TableNamesRegexp:  demoYdbTable.Name,
					ColumnNamesRegexp: "a",
					Type:              YdbColumnsWhiteList,
				}},
				description: demoYdbTable,
			},
			want:    []options.Column{{Name: "a", Type: types.TypeBool, Family: "default"}},
			wantErr: false,
		},
		{
			name: "tests multiple filters",
			args: args{
				filter: []YdbColumnsFilter{{
					TableNamesRegexp:  "non-valid-table-name",
					ColumnNamesRegexp: "b|c",
					Type:              YdbColumnsBlackList,
				}, {
					TableNamesRegexp:  demoYdbTable.Name, // should be applied this one
					ColumnNamesRegexp: "b|c",
					Type:              YdbColumnsBlackList,
				}, {
					TableNamesRegexp:  demoYdbTable.Name,
					ColumnNamesRegexp: "a",
					Type:              YdbColumnsWhiteList,
				}},
				description: demoYdbTable,
			},
			want:    []options.Column{{Name: "a", Type: types.TypeBool, Family: "default"}},
			wantErr: false,
		},
		{
			name: "tests complicated regexp",
			args: args{
				filter: []YdbColumnsFilter{{
					TableNamesRegexp:  "^test.table$",
					ColumnNamesRegexp: "^a$|b+",
					Type:              YdbColumnsWhiteList,
				}},
				description: demoYdbTable,
			},
			want: []options.Column{
				{Name: "a", Type: types.TypeBool, Family: "default"},
				{Name: "b", Type: types.TypeInt32, Family: "default"},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := filterYdbTableColumns(tt.args.filter, tt.args.description)
			if (err != nil) != tt.wantErr {
				t.Errorf("filterYdbTableColumns() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("filterYdbTableColumns() got = %v, want %v", got, tt.want)
			}
		})
	}
}
