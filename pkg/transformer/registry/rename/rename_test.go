package rename

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/pkg/abstract"
)

func makeDummyChangeItem(tID abstract.TableID) abstract.ChangeItem {
	chI := new(abstract.ChangeItem)
	chI.Schema = tID.Namespace
	chI.Table = tID.Name
	return *chI
}

func TestRenameTableTransformer(t *testing.T) {
	transformer := RenameTableTransformer{
		AltNames: map[abstract.TableID]abstract.TableID{
			{
				Namespace: "public",
				Name:      "objects_0",
			}: {
				Namespace: "service",
				Name:      "objects",
			},

			{
				Namespace: "public",
				Name:      "objects_1",
			}: {
				Namespace: "service",
				Name:      "objects",
			},
		},
	}
	table := abstract.TableID{
		Namespace: "public",
		Name:      "objects",
	}
	table0 := abstract.TableID{
		Namespace: "public",
		Name:      "objects_0",
	}
	renamedTable := abstract.TableID{
		Namespace: "service",
		Name:      "objects",
	}

	require.False(t, transformer.Suitable(table, nil))
	require.True(t, transformer.Suitable(table0, nil))

	chI := makeDummyChangeItem(table)
	chI0 := makeDummyChangeItem(table0)

	transformResult := transformer.Apply([]abstract.ChangeItem{chI, chI0})
	require.True(t, len(transformResult.Errors) == 0)
	require.True(t, len(transformResult.Transformed) == 2)
	require.True(t, transformResult.Transformed[0].TableID() == table)
	require.True(t, transformResult.Transformed[1].TableID() == renamedTable)
}
