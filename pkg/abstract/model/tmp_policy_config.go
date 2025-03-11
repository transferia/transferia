package model

import (
	"fmt"

	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/abstract"
)

type TableFilter func(tableID abstract.TableID) bool

type TmpPolicyConfig struct {
	Suffix  string
	include TableFilter
}

func NewTmpPolicyConfig(suffix string, include TableFilter) *TmpPolicyConfig {
	return &TmpPolicyConfig{Suffix: suffix, include: include}
}

func (c *TmpPolicyConfig) BuildSuffix(transferID string) string {
	return fmt.Sprintf("_%v%v", transferID, c.Suffix)
}

func (c *TmpPolicyConfig) Include(tableID abstract.TableID) bool {
	if c.include == nil {
		return false
	}
	return c.include(tableID)
}

func (c *TmpPolicyConfig) WithInclude(include TableFilter) *TmpPolicyConfig {
	return NewTmpPolicyConfig(c.Suffix, include)
}

const (
	ErrInvalidTmpPolicy = "invalid tmp policy: %w"
)

type TmpPolicyProvider interface {
	//Common naive tmp policy: implemented via middleware which one just rename change items during snapshot and call sink method Move(tmp_table -> orig_table) on DoneLoadTable event.
	//Sharded snapshots are not supported in this mode.
	EnsureTmpPolicySupported() error
	//Some destinations have their own custom implementations with tmp policy logic (e.g. YT dynamic tables sink - UseStaticTablesOnSnapshot).
	//Custom policy is more preferable than common tmp policy.
	//If custom tmp policy is enabled we won`t drop tables on a destination on cleanup sinker stage.
	EnsureCustomTmpPolicySupported() error
}

func EnsureTmpPolicySupported(destination Destination, transfer *Transfer) error {
	provider, ok := destination.(TmpPolicyProvider)
	if !ok {
		return xerrors.Errorf("destination '%T' is not supported", destination)
	}

	if err := provider.EnsureCustomTmpPolicySupported(); err == nil {
		return xerrors.New("destination configuration is invalid: custom tmp policy is enabled")
	}

	if err := provider.EnsureTmpPolicySupported(); err != nil {
		return xerrors.Errorf("destination configuration is invalid: %w", err)
	}

	if destination.CleanupMode() == DisabledCleanup {
		return xerrors.New("cleanup must be enabled")
	}

	if transfer != nil {
		if transfer.IsSharded() {
			return xerrors.New("sharding is not supported")
		}
	}

	return nil
}
