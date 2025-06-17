//go:build !disable_mysql_provider

package mysql

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
)

func TestSource_Include(t *testing.T) {
	t.Run("mysql", func(t *testing.T) {
		src := MysqlSource{}
		t.Run("regexp black white", func(t *testing.T) {
			src.IncludeTableRegex = []string{"^partner.users"}
			src.ExcludeTableRegex = []string{"^partner.statistics_dsp"}
			require.True(t, src.Include(abstract.TableID{Namespace: "partner", Name: "users"}))
			require.False(t, src.Include(abstract.TableID{Namespace: "partner", Name: "statistics_dsp"}))
		})
		t.Run("regexp black only", func(t *testing.T) {
			src.ExcludeTableRegex = []string{"^partner.statistics_dsp"}
			require.False(t, src.Include(abstract.TableID{Namespace: "partner", Name: "statistics_dsp"}))
			require.True(t, src.Include(abstract.TableID{Namespace: "partner", Name: "users"}))
			require.True(t, src.Include(abstract.TableID{Namespace: "partner", Name: "users_other"}))
		})
		t.Run("regexp blackout whole schema", func(t *testing.T) {
			src.IncludeTableRegex = []string{}
			src.ExcludeTableRegex = []string{"^partner.*"}
			require.False(t, src.Include(abstract.TableID{Namespace: "partner", Name: "statistics_dsp"}))
			require.False(t, src.Include(abstract.TableID{Namespace: "partner", Name: "users"}))
			require.False(t, src.Include(abstract.TableID{Namespace: "partner", Name: "users_other"}))
			require.True(t, src.Include(abstract.TableID{Namespace: "not_partner", Name: "users_other"}))
		})
		t.Run("regexp white only", func(t *testing.T) {
			src.IncludeTableRegex = []string{"^partner.users"}
			src.ExcludeTableRegex = []string{}
			require.False(t, src.Include(abstract.TableID{Namespace: "partner", Name: "statistics_dsp"}))
			require.False(t, src.Include(abstract.TableID{Namespace: "partner", Name: "statistics_dsp_other"}))
			require.True(t, src.Include(abstract.TableID{Namespace: "partner", Name: "users"}))
		})
		t.Run("string match black white", func(t *testing.T) {
			src.IncludeTableRegex = []string{"partner.users"}
			src.ExcludeTableRegex = []string{"partner.statistics_dsp"}
			require.False(t, src.Include(abstract.TableID{Namespace: "partner", Name: "statistics_dsp"}))
			require.True(t, src.Include(abstract.TableID{Namespace: "partner", Name: "users"}))
		})
		t.Run("string match black only", func(t *testing.T) {
			src.ExcludeTableRegex = []string{"partner.statistics_dsp"}
			require.False(t, src.Include(abstract.TableID{Namespace: "partner", Name: "statistics_dsp"}))
			require.True(t, src.Include(abstract.TableID{Namespace: "partner", Name: "users"}))
			require.True(t, src.Include(abstract.TableID{Namespace: "partner", Name: "users_other"}))
		})
		t.Run("string match white only", func(t *testing.T) {
			src.IncludeTableRegex = []string{"partner.users"}
			require.False(t, src.Include(abstract.TableID{Namespace: "partner", Name: "statistics_dsp"}))
			require.False(t, src.Include(abstract.TableID{Namespace: "partner", Name: "statistics_dsp_other"}))
			require.True(t, src.Include(abstract.TableID{Namespace: "partner", Name: "users"}))
		})
		t.Run("has database name", func(t *testing.T) {
			src.ExcludeTableRegex = []string{}
			src.IncludeTableRegex = []string{}
			src.Database = "partner_included"
			require.False(t, src.Include(abstract.TableID{Namespace: "partner", Name: "statistics_dsp"}))
			require.False(t, src.Include(abstract.TableID{Namespace: "partner", Name: "statistics_dsp_other"}))
			require.True(t, src.Include(abstract.TableID{Namespace: "partner_included", Name: "users"}))
		})
	})
}
