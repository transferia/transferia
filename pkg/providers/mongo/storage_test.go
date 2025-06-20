//go:build !disable_mongo_provider

package mongo

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/mongo"
)

func TestFilterSystemCollections(t *testing.T) {
	specs := []*mongo.CollectionSpecification{
		{
			Name: "new",
			Type: "collection",
		},
		{
			Name: "system.roles",
			Type: "collection",
		},
		{
			Name: "test",
			Type: "collection",
		},
	}

	filtered, err := filterSystemCollections(specs)
	require.NoError(t, err)
	require.Len(t, filtered, 2)

	// adding additional system collection has no effect
	specs = append(specs, &mongo.CollectionSpecification{
		Name: "system.permissions",
	})

	filtered, err = filterSystemCollections(specs)
	require.NoError(t, err)
	require.Len(t, filtered, 2)

	// adding view is also filtered out
	specs = append(specs, &mongo.CollectionSpecification{
		Name: "test",
		Type: "view",
	})

	filtered, err = filterSystemCollections(specs)

	sort.Strings(filtered)

	require.NoError(t, err)
	require.Len(t, filtered, 2)
	require.Equal(t, []string{"new", "test"}, filtered)
}

func TestFilterSystemDbs(t *testing.T) {
	dbs := []string{
		"admin",
		"test1",
		"config",
		"test2",
		"local",
		"test3",
		"mdb_internal",
	}

	filtered := filterSystemDBs(dbs, SystemDBs)
	require.Len(t, filtered, 3)

	sort.Strings(filtered)
	require.Equal(t, []string{"test1", "test2", "test3"}, filtered)
}
