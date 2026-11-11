package mongo

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/spf13/cast"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	provider_mongo "github.com/transferia/transferia/pkg/providers/mongo"
	canon_mongo "github.com/transferia/transferia/tests/canon/mongo"
	eo "github.com/transferia/transferia/tests/e2e/ch_exactly_once_snapshot"
	_ "github.com/transferia/transferia/tests/helpers/registration/mongo"
	"github.com/transferia/transferia/tests/helpers/storage/storagecomparison"
)

func TestExactlyOnceSnapshot(test *testing.T) {
	eo.ForDatabases(test, func(test *testing.T) {
		for _, secondaryPreferred := range []bool{false, true} {
			for _, workers := range []int{1, 2} {
				test.Run(fmt.Sprintf("secondaryPreferred%t/workers%d", secondaryPreferred, workers), func(test *testing.T) {
					target := eo.NewTarget(test, "")
					databaseName := target.Database
					source := provider_mongo.RecipeSource()
					collection := "rows"
					source.Collections = []provider_mongo.MongoCollection{{DatabaseName: databaseName, CollectionName: collection}}
					source.SecondaryPreferredMode = secondaryPreferred
					source.DesiredPartSize = 1
					require.NoError(test, canon_mongo.InsertDocs(context.Background(), source, databaseName, collection, canon_mongo.SnapshotDocuments...))
					eo.Run(test, source, target, workers)
					eo.CheckRows(test, target, collection, uint64(len(canon_mongo.SnapshotDocuments)))
					require.NoError(test, storagecomparison.CompareStorages(test, source, target, storagecomparison.NewCompareStorageParams().WithTableFilter(func(tables abstract.TableMap) []abstract.TableDescription {
						return []abstract.TableDescription{{Schema: databaseName, Name: collection}}
					}).WithEqualDataTypes(func(string, string) bool { return true }).WithPriorityComparators(func(sourceValue interface{}, sourceSchema abstract.ColSchema, targetValue interface{}, targetSchema abstract.ColSchema, intoArray bool) (bool, bool, error) {
						serializedValue, err := json.Marshal(sourceValue)
						if err != nil {
							return false, false, err
						}
						return true, string(serializedValue) == cast.ToString(targetValue), nil
					})))
				})
			}
		}
	})
}
