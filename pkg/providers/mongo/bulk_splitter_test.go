package mongo

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/mongo"
)

func TestBulkSplitter(t *testing.T) {
	model := &mongo.UpdateOneModel{}
	id1 := documentID{Raw: "id1", String: "id1"}
	id2 := documentID{Raw: "id2", String: "id2"}
	splitter := newBulkSplitter()
	splitter.Add(model, id1, false)
	splitter.Add(model, id2, false)
	splitter.Add(model, id1, false)
	splitter.Add(model, id2, true)
	bulks := splitter.Get()
	require.Equal(t, len(bulks), 3)
	require.Equal(t, len(bulks[0]), 2)
	require.Equal(t, len(bulks[1]), 1)
	require.Equal(t, len(bulks[2]), 1)
}

func TestBulkSplitter_AllUniqueIDs_SingleBulk(t *testing.T) {
	model := &mongo.UpdateOneModel{}
	splitter := newBulkSplitter()
	N := 57
	for i := 0; i < N; i++ {
		idStr := fmt.Sprintf("id%d", i)
		id := documentID{Raw: idStr, String: idStr}
		splitter.Add(model, id, false)
	}
	bulks := splitter.Get()
	require.Len(t, bulks, 1)
	require.Len(t, bulks[0], N)
}
