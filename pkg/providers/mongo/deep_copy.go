//go:build !disable_mongo_provider

package mongo

import (
	"bytes"
	"context"
	"encoding/gob"
	"sync"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/library/go/poolba"
	"github.com/transferia/transferia/pkg/util/gobwrapper"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func registerPrimitiveTypes() {
	gobwrapper.Register(primitive.Binary{})
	gobwrapper.Register(primitive.CodeWithScope{})
	gobwrapper.Register(primitive.DateTime(0))
	gobwrapper.Register(primitive.DBPointer{})
	gobwrapper.Register(primitive.Decimal128{})
	gobwrapper.Register(primitive.JavaScript(""))
	gobwrapper.Register(primitive.MaxKey{})
	gobwrapper.Register(primitive.MinKey{})
	gobwrapper.Register(primitive.Null{})
	gobwrapper.Register(primitive.ObjectID{})
	gobwrapper.Register(primitive.Regex{})
	gobwrapper.Register(primitive.Symbol(""))
	gobwrapper.Register(primitive.Timestamp{})
	gobwrapper.Register(primitive.Undefined{})
}

var (
	commonCopier      = new(gobSerializatorsPool)
	onceDoGobRegister sync.Once
)

type gobSerializator struct {
	encoder *gob.Encoder
	decoder *gob.Decoder
}

func (gs *gobSerializator) Copy(in, out any) error {
	if err := gs.encoder.Encode(in); err != nil {
		return xerrors.Errorf("failed encode value %T(%v): %w", in, in, err)
	}
	if err := gs.decoder.Decode(out); err != nil {
		return xerrors.Errorf("failed decode value %T(%v): %w", in, in, err)
	}
	return nil
}

func newGobSerializator() *gobSerializator {
	buf := new(bytes.Buffer)
	return &gobSerializator{
		encoder: gob.NewEncoder(buf),
		decoder: gob.NewDecoder(buf),
	}
}

type gobSerializatorsPool struct {
	mux  sync.Mutex
	pool *poolba.Pool[*gobSerializator]
}

func (gsp *gobSerializatorsPool) Initialized() bool {
	gsp.mux.Lock()
	defer gsp.mux.Unlock()
	return gsp.pool != nil
}

func (gsp *gobSerializatorsPool) Copy(in, out any) error {
	gs, err := gsp.pool.Borrow(context.Background())
	if err != nil {
		return xerrors.Errorf("cannot copy document: %w", err)
	}
	defer gs.Vacay()

	return gs.Value().Copy(in, out)
}

func (gsp *gobSerializatorsPool) Init(maxPoolSize int) error {
	gsp.mux.Lock()
	defer gsp.mux.Unlock()
	constructor := func(context.Context) (*gobSerializator, error) {
		return newGobSerializator(), nil
	}
	destructor := func(*gobSerializator) error { return nil }

	pool, err := poolba.PoolOf[*gobSerializator](maxPoolSize,
		poolba.WithConstructor(constructor),
		poolba.WithDestructor(destructor),
	)
	if err != nil {
		return xerrors.Errorf("cannot create pool of serializators: %w", err)
	}
	gsp.pool = pool
	return nil
}

func copier() (*gobSerializatorsPool, error) {
	if !commonCopier.Initialized() {
		onceDoGobRegister.Do(registerPrimitiveTypes)
		if err := commonCopier.Init(10); err != nil {
			return nil, xerrors.Errorf("cannot initialize copier: %w", err)
		}
	}
	return commonCopier, nil
}
