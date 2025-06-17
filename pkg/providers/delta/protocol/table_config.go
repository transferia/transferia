//go:build !disable_delta_provider

package protocol

import (
	"strconv"
	"strings"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/providers/delta/action"
)

var (
	LogRetentionProp = &TableConfig[time.Duration]{
		Key:          "logRetentionDuration",
		DefaultValue: "interval 30 days",
		FromString:   parseDuration,
	}
	TombstoneRetentionProp = &TableConfig[time.Duration]{
		Key:          "deletedFileRetentionDuration",
		DefaultValue: "interval 1 week",
		FromString:   parseDuration,
	}
	DeltaConfigCheckpointInterval = &TableConfig[int]{
		Key:          "checkpointInterval",
		DefaultValue: "10",
		FromString: func(s string) (int, error) {
			return strconv.Atoi(s)
		},
	}
	EnableExpiredLogCleanupProp = &TableConfig[bool]{
		Key:          "enableExpiredLogCleanup",
		DefaultValue: "true",
		FromString: func(s string) (bool, error) {
			return strings.ToLower(s) == "true", nil
		},
	}
	IsAppendOnlyProp = &TableConfig[bool]{
		Key:          "appendOnly",
		DefaultValue: "false",
		FromString: func(s string) (bool, error) {
			return strings.ToLower(s) == "true", nil
		},
	}
)

// TableConfig generic config structure from any string-val to typed-val.
type TableConfig[T any] struct {
	Key          string
	DefaultValue string
	FromString   func(s string) (T, error)
}

func (t *TableConfig[T]) fromMetadata(metadata *action.Metadata) (T, error) {
	v, ok := metadata.Configuration[t.Key]
	if !ok {
		v = t.DefaultValue
	}
	return t.FromString(v)
}

var timeDurationUnits = map[string]string{
	"nanosecond":  "ns",
	"microsecond": "us",
	"millisecond": "ms",
	"second":      "s",
	"hour":        "h",
	"day":         "h",
	"week":        "h",
}

var timeMultiplexer = map[string]int{
	"week": 7 * 24,
	"day":  24,
}

// The string value of this config has to have the following format: interval <number> <unit>.
// Where <unit> is either week, day, hour, second, millisecond, microsecond or nanosecond.
// If it's missing in metadata then the `self.default` is used
func parseDuration(s string) (time.Duration, error) {
	fields := strings.Fields(strings.ToLower(s))
	if len(fields) != 3 {
		return 0, xerrors.Errorf("can't parse duration from string :%s", s)
	}
	if fields[0] != "interval" {
		return 0, xerrors.Errorf("this is not a valid duration starting with :%s", fields[0])
	}

	d, err := time.ParseDuration(fields[1] + timeDurationUnits[fields[2]])
	if err != nil {
		return 0, xerrors.Errorf("unable to parse: %s duration: %w", s, err)
	}
	if mx, ok := timeMultiplexer[fields[2]]; ok {
		d = time.Duration(mx) * d
	}

	return d, nil
}
