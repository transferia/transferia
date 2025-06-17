//go:build !disable_postgres_provider

package postgres

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSource_Include(t *testing.T) {
	t.Run("PgObjectType", func(t *testing.T) {
		t.Run("all PgDumpSteps is valid pg objects", func(t *testing.T) {
			var ps PgDumpSteps
			v := reflect.ValueOf(&ps).Elem()
			for i := 0; i < v.NumField(); i++ {
				fv := v.Field(i)
				ft := v.Type().Field(i)
				if fv.IsValid() && fv.CanSet() && fv.Kind() == reflect.Bool && ft.Name != "Default" {
					fv.SetBool(true)
				}
			}

			stepsList := ps.List()
			require.True(t, len(stepsList) > 0)
			for _, step := range stepsList {
				pot := PgObjectType(step)
				require.NoError(t, pot.IsValid())
			}
		})
	})
}
