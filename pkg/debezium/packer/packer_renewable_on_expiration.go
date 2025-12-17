package packer

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"go.ytsaurus.tech/library/go/core/log"
)

type ExpirablePackerFactory func() (Packer, abstract.Expirer, error)

type PackerRenewableOnExpiration struct {
	Packer
	factory ExpirablePackerFactory
	logger  log.Logger
	expirer abstract.Expirer
}

func (s *PackerRenewableOnExpiration) renewPacker() error {
	newPacker, newExpirer, err := s.factory()
	if err != nil {
		return xerrors.Errorf("cannnot create new packer: %w", err)
	}
	if abstract.Expired(newExpirer) {
		return xerrors.Errorf("cannot renew packer because new issue time is less than current time")
	}
	s.logger.Info("successfully renewed packer",
		log.Any("expired_at", abstract.ExpiresAt(s.expirer)),
		log.Any("new_expired_at", abstract.ExpiresAt(newExpirer)),
	)
	s.expirer = newExpirer
	s.Packer = newPacker
	return nil
}

func (s *PackerRenewableOnExpiration) Pack(
	changeItem *abstract.ChangeItem,
	payloadBuilder BuilderFunc,
	kafkaSchemaBuilder BuilderFunc,
	maybeCachedRawSchema []byte,
) ([]byte, error) {
	if abstract.Expired(s.expirer) {
		if err := s.renewPacker(); err != nil {
			return nil, xerrors.Errorf("can't renew packer on expiration: %w", err)
		}
	}
	return s.Packer.Pack(changeItem, payloadBuilder, kafkaSchemaBuilder, maybeCachedRawSchema)
}

func NewPackerRenewableOnExpiration(
	packerFactory ExpirablePackerFactory,
	logger log.Logger,
) (*PackerRenewableOnExpiration, error) {
	result := &PackerRenewableOnExpiration{
		Packer:  nil,
		factory: packerFactory,
		logger:  logger,
		expirer: nil,
	}
	if err := result.renewPacker(); err != nil {
		return nil, xerrors.Errorf("cannot construct new packer: %w", err)
	}
	return result, nil
}
