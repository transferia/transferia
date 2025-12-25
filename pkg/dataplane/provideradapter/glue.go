package provideradapter

import (
	"reflect"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract/model"
	"golang.org/x/xerrors"
)

var (
	adapterRegistry = map[reflect.Type]func(val model.EndpointParams) Adapter{}
)

type Adapter interface {
	WithConfig() error
}

func Register[T model.EndpointParams](f func(val T) Adapter) {
	var t T
	adapterRegistry[reflect.TypeOf(t)] = func(val model.EndpointParams) Adapter {
		return f(val.(T))
	}
}

func ApplyForTransfer(transfer *model.Transfer) error {
	if err := ApplyForEndpoint(transfer.Src); err != nil {
		return xerrors.Errorf("unable to adapt src: %w", err)
	}
	if err := ApplyForEndpoint(transfer.Dst); err != nil {
		return xerrors.Errorf("unable to adapt dst: %w", err)
	}
	return nil
}

func ApplyForEndpoint(endpoint model.EndpointParams) error {
	if endpoint == nil {
		return nil
	}
	f, ok := adapterRegistry[reflect.TypeOf(endpoint)]
	if !ok {
		logger.Log.Infof("endpoint: %T has no adapter, skip", endpoint)
		return nil
	}
	logger.Log.Infof("endpoint: %T has adapter, execute", endpoint)
	return f(endpoint).WithConfig()
}
