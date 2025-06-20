//go:build disable_postgres_provider

package tasks

import "github.com/transferia/transferia/pkg/abstract"

func tryComparePg(lOriginalIsPG bool, rOriginalIsPG bool, lVal interface{}, rVal interface{}, lS string, rS string, lSchema abstract.ColSchema, rSchema abstract.ColSchema) (bool, error) {
	return false, nil
}

func tryCompareTemporalsPg(lVal interface{}, lSchema abstract.ColSchema, rVal interface{}, rSchema abstract.ColSchema) (mustReturn bool, comparable bool, result bool, err error) {
	return false, false, false, nil
}
