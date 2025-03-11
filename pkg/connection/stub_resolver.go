package connection

import (
	"context"

	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/abstract"
)

var _ ConnResolver = (*StubConnectionResolver)(nil)

type StubConnectionResolver struct {
	ConnectionsByID map[string]ManagedConnection
}

func (d *StubConnectionResolver) ResolveConnection(ctx context.Context, connectionID string, typ abstract.ProviderType) (ManagedConnection, error) {
	logger.Log.Infof("Resolving connection data for id %s", connectionID)
	res, ok := d.ConnectionsByID[connectionID]
	if !ok {
		return nil, xerrors.Errorf("Unable to resolve connection %s", connectionID)
	}

	switch typ {
	case "pg":
		if pgConn, ok := res.(*ConnectionPG); ok {
			return pgConn, nil
		}
		return nil, xerrors.Errorf("Unable to cast pg connection %s", connectionID)
	case "mysql":
		if mysqlConn, ok := res.(*ConnectionMySQL); ok {
			return mysqlConn, nil
		}
		return nil, xerrors.Errorf("Unable to cast mysql connection %s", connectionID)
	case "ch":
		if chConn, ok := res.(*ConnectionCH); ok {
			return chConn, nil
		}
		return nil, xerrors.Errorf("Unable to cast ch connection %s", connectionID)

	default:
		return nil, xerrors.Errorf("Not implemented for provider %s", typ)
	}
}

func (d *StubConnectionResolver) Add(connectionID string, connection any) error {
	conn, ok := connection.(ManagedConnection)
	if !ok {
		return xerrors.Errorf("Wrong connection type: %T", connection)
	}
	d.ConnectionsByID[connectionID] = conn
	return nil
}

func NewStubConnectionResolver() *StubConnectionResolver {
	return &StubConnectionResolver{
		ConnectionsByID: make(map[string]ManagedConnection),
	}
}
