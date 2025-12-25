package middlewares

import (
	"fmt"
	"testing"

	"github.com/spf13/cast"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/typesystem"
	"github.com/transferia/transferia/pkg/stats"
)

type Provider string

func (p Provider) GetProviderType() abstract.ProviderType {
	return abstract.ProviderType(p)
}

func (p Provider) Validate() error {
	return nil
}

func (p Provider) WithDefaults() {
}

type MockSink struct {
	PushCallback func([]abstract.ChangeItem)
}

func (s *MockSink) Close() error {
	return nil
}

func (s *MockSink) Push(input []abstract.ChangeItem) error {
	s.PushCallback(input)
	return nil
}

func TestPrepareFallbacker(t *testing.T) {
	t.Run("noop", func(t *testing.T) {
		typesystem.AddFallbackSourceFactory(func() typesystem.Fallback {
			return typesystem.Fallback{
				To:     typesystem.LatestVersion - 1,
				Picker: typesystem.ProviderType("noop"),
				Function: func(ci *abstract.ChangeItem) (*abstract.ChangeItem, error) {
					return ci, nil // actually should return FallbackDoesNotApplyErr when a fallback does not apply
				},
			}
		})
		changes := []abstract.ChangeItem{{Kind: abstract.InsertKind, Table: "foo", ColumnValues: []interface{}{1, 2, 3}}}
		snkr := &MockSink{
			PushCallback: func(items []abstract.ChangeItem) {
				require.Equal(t, changes, items)
			},
		}
		sourceFallbacks := buildFallbacks(typesystem.SourceFallbackFactories)
		fb := prepareFallbacker(typesystem.LatestVersion-1, Provider("noop"), sourceFallbacks, logger.Log, stats.NewFallbackStatsCombination(solomon.NewRegistry(nil)).Source)
		require.NotNil(t, fb)
		require.NoError(t, fb(snkr).Push(changes))
	})

	t.Run("error", func(t *testing.T) {
		typesystem.AddFallbackSourceFactory(func() typesystem.Fallback {
			return typesystem.Fallback{
				To:     typesystem.LatestVersion - 1,
				Picker: typesystem.ProviderType("error"),
				Function: func(ci *abstract.ChangeItem) (*abstract.ChangeItem, error) {
					return ci, xerrors.Errorf("error migration")
				},
			}
		})
		changes := []abstract.ChangeItem{{Kind: abstract.InsertKind, Table: "foo", ColumnValues: []interface{}{1, 2, 3}}}
		snkr := &MockSink{
			PushCallback: func(items []abstract.ChangeItem) {
				require.Equal(t, changes, items)
			},
		}
		sourceFallbacks := buildFallbacks(typesystem.SourceFallbackFactories)
		fb := prepareFallbacker(typesystem.LatestVersion-1, Provider("error"), sourceFallbacks, logger.Log, stats.NewFallbackStatsCombination(solomon.NewRegistry(nil)).Source)
		require.NotNil(t, fb)
		require.Error(t, fb(snkr).Push(changes))
	})

	t.Run("stringer", func(t *testing.T) {
		typesystem.AddFallbackSourceFactory(func() typesystem.Fallback {
			return typesystem.Fallback{
				To:     typesystem.LatestVersion - 1,
				Picker: typesystem.ProviderType("stringer"),
				Function: func(ci *abstract.ChangeItem) (*abstract.ChangeItem, error) {
					for i := range ci.ColumnValues {
						ci.ColumnValues[i] = cast.ToString(ci.ColumnValues[i])
					}
					return ci, nil
				},
			}
		})
		changes := []abstract.ChangeItem{{Kind: abstract.InsertKind, Table: "stringer", ColumnValues: []interface{}{1, 2, 3}}}
		snkr := &MockSink{
			PushCallback: func(items []abstract.ChangeItem) {
				for _, item := range items {
					for _, val := range item.ColumnValues {
						_, ok := val.(string)
						require.True(t, ok)
					}
				}
			},
		}
		sourceFallbacks := buildFallbacks(typesystem.SourceFallbackFactories)
		fb := prepareFallbacker(typesystem.LatestVersion-1, Provider("stringer"), sourceFallbacks, logger.Log, stats.NewFallbackStatsCombination(solomon.NewRegistry(nil)).Source)
		require.NotNil(t, fb)
		require.NoError(t, fb(snkr).Push(changes))
	})

	t.Run("latest", func(t *testing.T) {
		typesystem.AddFallbackSourceFactory(func() typesystem.Fallback {
			return typesystem.Fallback{
				To:     typesystem.LatestVersion - 1,
				Picker: typesystem.ProviderType("latest"),
				Function: func(ci *abstract.ChangeItem) (*abstract.ChangeItem, error) {
					for i := range ci.ColumnValues {
						ci.ColumnValues[i] = cast.ToString(ci.ColumnValues[i])
					}
					return ci, nil
				},
			}
		})
		sourceFallbacks := buildFallbacks(typesystem.SourceFallbackFactories)
		fb := prepareFallbacker(typesystem.LatestVersion, Provider("latest"), sourceFallbacks, logger.Log, stats.NewFallbackStatsCombination(solomon.NewRegistry(nil)).Source)
		require.Nil(t, fb)
	})

	t.Run("chain-of-fallback", func(t *testing.T) {
		typesystem.AddFallbackSourceFactory(func() typesystem.Fallback {
			return typesystem.Fallback{
				To:     1,
				Picker: typesystem.ProviderType("chain-of-fallback"),
				Function: func(ci *abstract.ChangeItem) (*abstract.ChangeItem, error) {
					for i := range ci.ColumnValues {
						ci.ColumnValues[i] = fmt.Sprintf("%v_%s", ci.ColumnValues[i], "1")
					}
					return ci, nil
				},
			}
		})
		typesystem.AddFallbackSourceFactory(func() typesystem.Fallback {
			return typesystem.Fallback{
				To:     2,
				Picker: typesystem.ProviderType("chain-of-fallback"),
				Function: func(ci *abstract.ChangeItem) (*abstract.ChangeItem, error) {
					for i := range ci.ColumnValues {
						ci.ColumnValues[i] = fmt.Sprintf("%v_%s", ci.ColumnValues[i], "2")
					}
					return ci, nil
				},
			}
		})
		typesystem.AddFallbackSourceFactory(func() typesystem.Fallback {
			return typesystem.Fallback{
				To:     3,
				Picker: typesystem.ProviderType("chain-of-fallback"),
				Function: func(ci *abstract.ChangeItem) (*abstract.ChangeItem, error) {
					for i := range ci.ColumnValues {
						ci.ColumnValues[i] = fmt.Sprintf("%v_%s", ci.ColumnValues[i], "3")
					}
					return ci, nil
				},
			}
		})

		changes := []abstract.ChangeItem{{Kind: abstract.InsertKind, Table: "foo", ColumnValues: []interface{}{"change"}}}
		snkr := &MockSink{
			PushCallback: func(items []abstract.ChangeItem) {
				for _, item := range items {
					for _, val := range item.ColumnValues {
						v, ok := val.(string)
						require.True(t, ok)
						require.Equal(t, v, "change_3_2_1")
					}
				}
			},
		}
		sourceFallbacks := buildFallbacks(typesystem.SourceFallbackFactories)
		fb := prepareFallbacker(1, Provider("chain-of-fallback"), sourceFallbacks, logger.Log, stats.NewFallbackStatsCombination(solomon.NewRegistry(nil)).Source)
		require.NotNil(t, fb)
		require.NoError(t, fb(snkr).Push(changes))
	})
}
