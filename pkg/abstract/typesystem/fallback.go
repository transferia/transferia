package typesystem

import (
	"fmt"

	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/abstract/model"
)

const LatestVersion int = model.LatestVersion

const NewTransfersVersion int = model.NewTransfersVersion

var FallbackDoesNotApplyErr = xerrors.NewSentinel("this fallback does not apply")

type FallbackFactory func() Fallback

// Fallback defines a transformation from a newer version of typesystem to an older one.
type Fallback struct {
	// To is the target typesystem version of this fallback
	To int
	// Picker limits the endpoints to which this fallback applies
	Picker func(m model.EndpointParams) bool
	// Function defines the transformation. Input is an item of any kind.
	//
	// If a fallback does not apply, this method MUST return an error containing FallbackDoesNotApplyErr
	Function func(ci *abstract.ChangeItem) (*abstract.ChangeItem, error)
}

func ProviderType(typ abstract.ProviderType) func(m model.EndpointParams) bool {
	return func(m model.EndpointParams) bool {
		return m.GetProviderType() == typ
	}
}

func (f Fallback) Applies(version int, provider model.EndpointParams) bool {
	return f.Picker(provider) && f.To >= version
}

func (f Fallback) String() string {
	return fmt.Sprintf("fallback [%d, %p]", f.To, f.Function)
}

// AddFallbackSourceFactory registers a fallbacks for a source of some type
//
// This method is expected to be called in the `init()` function of a module which introduces a fallback when some additional behaviour is expected
func AddFallbackSourceFactory(factory FallbackFactory) {
	registerFallbackFactory(&SourceFallbackFactories, factory)
}

// AddFallbackTargetFactory registers a fallbacks for a target of some type
//
// This method is expected to be called in the `init()` function of a module which introduces a fallback when some additional behaviour is expected
func AddFallbackTargetFactory(factory FallbackFactory) {
	registerFallbackFactory(&TargetFallbackFactories, factory)
}
