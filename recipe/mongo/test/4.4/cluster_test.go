package example

import (
	"testing"

	"github.com/transferria/transferria/recipe/mongo/pkg/util"
)

func TestSample(t *testing.T) {
	util.TestMongoShardedClusterRecipe(t)
}
