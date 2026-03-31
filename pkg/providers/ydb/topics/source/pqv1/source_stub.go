//go:build !persqueue

package pqv1source

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/parsers"
	topicsource "github.com/transferia/transferia/pkg/providers/ydb/topics/source"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
)

var errNotEnabled = xerrors.New("persqueue support is not enabled: build with -tags persqueue")

type Source struct{}

func (p *Source) Run(_ abstract.AsyncSink) error       { return errNotEnabled }
func (p *Source) Stop()                                 {}
func (p *Source) Fetch() ([]abstract.ChangeItem, error) { return nil, errNotEnabled }
func (p *Source) YSRNamespaceID() string                { return "" }

func NewSource(_ *topicsource.Config, _ parsers.Parser, _ log.Logger, _ *stats.SourceStats) (*Source, error) {
	return nil, errNotEnabled
}
