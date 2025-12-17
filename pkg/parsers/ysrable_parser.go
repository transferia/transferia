package parsers

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"go.ytsaurus.tech/library/go/core/log"
)

type ExpirableParserFactory func() (Parser, abstract.Expirer, error)

// This parser wrapper should be used if parser uses Yandex Schema Registry (YSR) Namespace ID
// In order to check that user has permission on specified Namespace ID when
// creating/updating endpoint with such parser
type YSRableParser struct {
	Parser
	factory        ExpirableParserFactory
	expirer        abstract.Expirer
	ysrNamespaceID string
	logger         log.Logger
}

func (p *YSRableParser) renewParser() error {
	newParser, newExpirer, err := p.factory()
	if err != nil {
		return xerrors.Errorf("cannnot create new parser: %w", err)
	}
	if abstract.Expired(newExpirer) {
		return xerrors.Errorf("cannot renew parser because new issue time is less than current time")
	}
	p.logger.Info("successfully renewed parser",
		log.Any("expired_at", abstract.ExpiresAt(p.expirer)),
		log.Any("new_expired_at", abstract.ExpiresAt(newExpirer)),
	)
	p.expirer = newExpirer
	p.Parser = newParser
	return nil
}

func (p *YSRableParser) Unwrap() Parser {
	return p.Parser
}

func (p *YSRableParser) YSRNamespaceID() string {
	return p.ysrNamespaceID
}

func (p *YSRableParser) Do(msg Message, partition abstract.Partition) []abstract.ChangeItem {
	if abstract.Expired(p.expirer) {
		if err := p.renewParser(); err != nil {
			p.logger.Warn("failed to renew parser, continuing with old parser until credentials are not over", log.Error(err))
		}
	}
	return p.Parser.Do(msg, partition)
}

func (p *YSRableParser) DoBatch(batch MessageBatch) []abstract.ChangeItem {
	if abstract.Expired(p.expirer) {
		if err := p.renewParser(); err != nil {
			p.logger.Warn("failed to renew parser, continuing with old parser until credentials are not over", log.Error(err))
		}
	}
	return p.Parser.DoBatch(batch)
}

func WithYSRNamespaceIDs(factory ExpirableParserFactory, ysrNamespaceID string, logger log.Logger) (Parser, error) {
	result := &YSRableParser{
		Parser:         nil,
		expirer:        nil,
		factory:        factory,
		ysrNamespaceID: ysrNamespaceID,
		logger:         logger,
	}
	if err := result.renewParser(); err != nil {
		return nil, err
	}
	return result, nil
}
