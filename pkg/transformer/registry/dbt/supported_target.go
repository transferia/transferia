package dbt

import (
	"context"

	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/abstract/model"
	"github.com/transferria/transferria/pkg/middlewares"
)

type RegisterFunc = func(destination model.Destination) (SupportedDestination, error)

var NotSupportedErr = xerrors.New("DBT not supported")

var adapters []RegisterFunc

func Register(f RegisterFunc) {
	adapters = append(adapters, f)
}

func ToSupportedDestination(destination model.Destination) (SupportedDestination, error) {
	for _, adapter := range adapters {
		res, err := adapter(destination)
		if err != nil {
			continue
		}
		return res, nil
	}
	return nil, NotSupportedErr
}

type SupportedDestination interface {
	// DBTConfiguration provides a YAML-marshallable configuration of the target to be used by DBT.
	//
	// The object returned by this function must be the database-specific settings, namely the object inside `outputs`.
	// Other DBT parameters will be set automatically by the common code.
	DBTConfiguration(ctx context.Context) (any, error)
}

func init() {
	middlewares.PlugTransformer(PluggableTransformer)
}
