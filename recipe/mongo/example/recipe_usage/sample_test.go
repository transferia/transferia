package recipe_usage

import (
	"testing"

	"github.com/transferia/transferia/recipe/mongo/pkg/util"
)

func TestSample(t *testing.T) {
	util.TestMongoShardedClusterRecipe(t)
}
