package middlewares

import (
	"strings"

	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/abstract/model"
	"github.com/transferria/transferria/pkg/abstract/typesystem"
	"github.com/transferria/transferria/pkg/stats"
	"github.com/transferria/transferria/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
	"golang.org/x/exp/slices"
)

func SourceFallbacks(version int, typ model.EndpointParams, lgr log.Logger, stat *stats.FallbackStats) abstract.SinkOption {
	sourceFallbacks := buildFallbacks(typesystem.SourceFallbackFactories)
	lgr.Info("Prepare source fallbacks", log.Int("latest_typesystem_version", typesystem.LatestVersion), log.Int("typesystem_version", version), log.String("provider", typ.GetProviderType().Name()), log.Any("registry", fallbacksString(sourceFallbacks)))
	if result := prepareFallbacker(version, typ, sourceFallbacks, lgr, stat); result == nil {
		return func(sinker abstract.Sinker) abstract.Sinker {
			return sinker
		}
	} else {
		return result
	}
}

func TargetFallbacks(version int, typ model.EndpointParams, lgr log.Logger, stat *stats.FallbackStats) abstract.SinkOption {
	targetFallbacks := buildFallbacks(typesystem.TargetFallbackFactories)
	lgr.Info("Prepare target fallbacks", log.Int("latest_typesystem_version", typesystem.LatestVersion), log.Int("typesystem_version", version), log.String("provider", typ.GetProviderType().Name()), log.Any("registry", fallbacksString(targetFallbacks)))
	if result := prepareFallbacker(version, typ, targetFallbacks, lgr, stat); result == nil {
		return func(sinker abstract.Sinker) abstract.Sinker {
			return sinker
		}
	} else {
		return result
	}
}

func buildFallbacks(factories []typesystem.FallbackFactory) []typesystem.Fallback {
	fallbacks := make([]typesystem.Fallback, 0)
	for _, fbf := range factories {
		fallbacks = append(fallbacks, fbf())
	}

	return fallbacks
}

func prepareFallbacker(version int, typ model.EndpointParams, registry []typesystem.Fallback, lgr log.Logger, stat *stats.FallbackStats) abstract.SinkOption {
	var applicableFallbacks []typesystem.Fallback
	for _, fb := range registry {
		if fb.Applies(version, typ) {
			applicableFallbacks = append(applicableFallbacks, fb)
		}
	}
	if len(applicableFallbacks) == 0 {
		lgr.Info("No applicable typesystem fallbacks found", log.Int("latest_typesystem_version", typesystem.LatestVersion), log.Int("typesystem_version", version), log.String("provider", typ.GetProviderType().Name()), log.String("fallbacks", fallbacksString(applicableFallbacks)))
		return nil
	}

	slices.SortFunc(applicableFallbacks, func(a, b typesystem.Fallback) int {
		if a.To > b.To {
			return -1
		} else if a.To < b.To {
			return 1
		}
		return 0
	})
	lgr.Info("Applicable typesystem fallbacks found", log.Int("latest_typesystem_version", typesystem.LatestVersion), log.Int("typesystem_version", version), log.String("provider", typ.GetProviderType().Name()), log.String("fallbacks", fallbacksString(applicableFallbacks)))
	stat.Deepness.Set(float64(len(applicableFallbacks)))
	return func(sinker abstract.Sinker) abstract.Sinker {
		return newFallbacker(sinker, applicableFallbacks, lgr, stat)
	}
}

type fallbacker struct {
	sinker    abstract.Sinker
	fallbacks []typesystem.Fallback
	logger    log.Logger
	stat      *stats.FallbackStats
}

func newFallbacker(sinker abstract.Sinker, fallbacks []typesystem.Fallback, lgr log.Logger, stat *stats.FallbackStats) *fallbacker {
	return &fallbacker{
		sinker:    sinker,
		fallbacks: fallbacks,
		logger:    lgr,
		stat:      stat,
	}
}

func (f *fallbacker) Close() error {
	return f.sinker.Close()
}

func fallbacksString(fallbacks []typesystem.Fallback) string {
	result := make([]string, len(fallbacks))
	for i, fb := range fallbacks {
		result[i] = fb.String()
	}
	return strings.Join(result, ", ")
}

func (f *fallbacker) Push(items []abstract.ChangeItem) error {
	fallbackApplicationCount := int64(0)

	for i, item := range items {
		fallbackWasApplied := false
		var errs util.Errors
		for _, fallback := range f.fallbacks {
			if r, err := fallback.Function(&item); err != nil {
				if xerrors.Is(err, typesystem.FallbackDoesNotApplyErr) {
					continue
				}
				errs = append(errs, xerrors.Errorf("failed to apply %s: %w", fallback, err))
			} else {
				fallbackWasApplied = true
				items[i] = *r
			}
		}
		if len(errs) > 0 {
			f.stat.Errors.Add(int64(len(errs)))
			return xerrors.Errorf("failed to apply fallbacks: %w", errs)
		}
		if fallbackWasApplied {
			fallbackApplicationCount += 1
		}
	}

	if fallbackApplicationCount > 0 {
		f.logger.Info("fallbacks applied", log.String("fallbacks", fallbacksString(f.fallbacks)), log.Int64("items_count", fallbackApplicationCount))
		f.stat.Items.Add(fallbackApplicationCount)
	}

	return f.sinker.Push(items)
}
