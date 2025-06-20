//go:build !disable_s3_provider

package reader

import (
	"fmt"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/parsers/generic"
	"github.com/transferia/transferia/pkg/providers/s3"
)

func handleParseError(
	tableID abstract.TableID,
	unparsedPolicy s3.UnparsedPolicy,
	filePath string,
	lineCounter int,
	parseErr error,
) (*abstract.ChangeItem, error) {
	switch unparsedPolicy {
	case s3.UnparsedPolicyFail:
		return nil, abstract.NewFatalError(xerrors.Errorf("unable to parse: %s:%v: %w", filePath, lineCounter, parseErr))
	case s3.UnparsedPolicyRetry:
		return nil, xerrors.Errorf("unable to parse: %s:%v: %w", filePath, lineCounter, parseErr)
	default:
		ci := generic.NewUnparsed(
			abstract.NewPartition(tableID.Name, 0),
			tableID.Name,
			fmt.Sprintf("%s:%v", filePath, lineCounter),
			parseErr.Error(),
			lineCounter,
			0,
			time.Now(),
		)
		return &ci, nil
	}
}
