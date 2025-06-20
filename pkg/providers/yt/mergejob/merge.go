//go:build !disable_yt_provider

package mergejob

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"go.ytsaurus.tech/yt/go/mapreduce"
	"go.ytsaurus.tech/yt/go/yson"
)

type MergeWithDeduplicationJob struct {
	mapreduce.Untyped
}

func NewMergeWithDeduplicationJob() *MergeWithDeduplicationJob {
	return &MergeWithDeduplicationJob{
		Untyped: mapreduce.Untyped{},
	}
}

func (j *MergeWithDeduplicationJob) Do(ctx mapreduce.JobContext, in mapreduce.Reader, out []mapreduce.Writer) error {
	return mapreduce.GroupKeys(in, func(r mapreduce.Reader) error {
		var row yson.RawValue
		for r.Next() {
			row = yson.RawValue{}
			if err := r.Scan(&row); err != nil {
				return xerrors.Errorf("unable to scan row: %w", err)
			}
		}
		if err := out[0].Write(row); err != nil {
			return xerrors.Errorf("unable to write row: %w", err)
		}
		return nil
	})
}
