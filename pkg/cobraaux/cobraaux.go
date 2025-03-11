package cobraaux

import (
	"github.com/spf13/cobra"
	"github.com/transferria/transferria/library/go/core/xerrors"
)

// RegisterCommand is like parent.AddCommand(child), but also
// makes chaining of PersistentPreRunE and PersistentPreRun
func RegisterCommand(parent, child *cobra.Command) {
	parentPpre := parent.PersistentPreRunE
	childPpre := child.PersistentPreRunE
	if child.PersistentPreRunE == nil && child.PersistentPreRun != nil {
		childPpre = func(cmd *cobra.Command, args []string) error {
			child.PersistentPreRun(cmd, args)
			return nil
		}
	}
	if childPpre != nil {
		child.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {
			if parentPpre != nil {
				err := parentPpre(cmd, args)
				if err != nil {
					return xerrors.Errorf("cannot process parent PersistentPreRunE: %w", err)
				}
			}
			return childPpre(cmd, args)
		}
	} else if parentPpre != nil {
		child.PersistentPreRunE = parentPpre
	}
	parent.AddCommand(child)
}
