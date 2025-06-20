//go:build !disable_clickhouse_provider

package schema

import (
	"context"
	"sync"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/base"
	"go.ytsaurus.tech/library/go/core/log"
)

type DDLLoaders interface {
	LoadTablesDDL(tables []abstract.TableID) ([]TableDDL, error)
}

type DDLSource struct {
	ch      abstract.Storage
	mutex   sync.Mutex
	running bool
	part    base.DataObjectPart
	logger  log.Logger
}

func (p *DDLSource) Running() bool {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	return p.running
}

func (p *DDLSource) Start(ctx context.Context, target base.EventTarget) error {
	p.mutex.Lock()
	p.running = true
	p.mutex.Unlock()
	defer p.finish()
	tdesc, err := p.part.ToOldTableDescription()
	if err != nil {
		return xerrors.Errorf("unable to create table description: %w", err)
	}
	loader, ok := p.ch.(DDLLoaders)
	if !ok {
		return xerrors.Errorf("unexpected storage type: %T", p.ch)
	}
	ddls, err := loader.LoadTablesDDL([]abstract.TableID{tdesc.ID()})
	if err != nil {
		return xerrors.Errorf("unable to load ddls for table %v: %w", tdesc.Fqtn(), err)
	}
	p.logger.Info("apply ddls", log.Any("ddls", ddls))
	if err := <-target.AsyncPush(NewDDLBatch(ddls)); err != nil {
		return xerrors.Errorf("unable to apply DDLs: %w", err)
	}
	return nil
}

func (p *DDLSource) Stop() error {
	return p.finish()
}

func (p *DDLSource) finish() error {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.running = false
	return nil
}

func NewDDLSource(logger log.Logger, part base.DataObjectPart, ch abstract.Storage) *DDLSource {
	return &DDLSource{
		logger:  logger,
		ch:      ch,
		mutex:   sync.Mutex{},
		running: false,
		part:    part,
	}
}
