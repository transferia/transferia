package mongo

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/util"
	"github.com/transferia/transferia/pkg/util/gobwrapper"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func init() {
	gobwrapper.Register(map[string]interface{}{})
	gobwrapper.Register(map[string]string{})
}

func BenchmarkDeepCopy(b *testing.B) {
	copyManager, err := copier()
	require.NoError(b, err)

	in := map[string]any{
		"key1": "value1",
		"key2": []string{
			"elem1",
			"elem2",
			"elem3",
		},
		"key3": map[string]string{
			"key1": "value1",
			"key2": "value2",
			"key3": "value3",
			"key4": "value4",
		},
	}
	b.SetBytes(int64(util.DeepSizeof(in)))

	for n := 0; n < b.N; n++ {
		out := map[string]any{}
		err = copyManager.Copy(in, &out)
		require.NoError(b, err)
	}
	b.ReportAllocs()
}

func BenchmarkDeepCopyWithGetCopier(b *testing.B) {
	in := map[string]any{
		"key1": "value1",
		"key2": []string{
			"elem1",
			"elem2",
			"elem3",
		},
		"key3": map[string]string{
			"key1": "value1",
			"key2": "value2",
			"key3": "value3",
			"key4": "value4",
		},
	}
	b.SetBytes(int64(util.DeepSizeof(in)))

	for n := 0; n < b.N; n++ {
		copyManager, err := copier()
		require.NoError(b, err)

		out := map[string]any{}
		err = copyManager.Copy(in, &out)
		require.NoError(b, err)
	}
	b.ReportAllocs()
}

func BenchmarkDeepCopyParallel(b *testing.B) {
	copyManager, err := copier()
	require.NoError(b, err)
	in := map[string]any{
		"key1": "value1",
		"key2": []string{
			"elem1",
			"elem2",
			"elem3",
		},
		"key3": map[string]string{
			"key1": "value1",
			"key2": "value2",
			"key3": "value3",
			"key4": "value4",
		},
	}
	b.SetBytes(int64(util.DeepSizeof(in)))
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			out := map[string]any{}
			err = copyManager.Copy(in, &out)
			require.NoError(b, err)
		}
	})
	b.ReportAllocs()
}

func BenchmarkDeepCopyWithGetCopierParallel(b *testing.B) {
	in := map[string]any{
		"key1": "value1",
		"key2": []string{
			"elem1",
			"elem2",
			"elem3",
		},
		"key3": map[string]string{
			"key1": "value1",
			"key2": "value2",
			"key3": "value3",
			"key4": "value4",
		},
	}
	b.SetBytes(int64(util.DeepSizeof(in)))
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			copyManager, err := copier()
			require.NoError(b, err)

			out := map[string]any{}
			err = copyManager.Copy(in, &out)
			require.NoError(b, err)
		}
	})
	b.ReportAllocs()
}

func BenchmarkCopyAny(b *testing.B) {
	id := primitive.NewObjectID()
	b.SetBytes(int64(util.DeepSizeof(id)))

	for n := 0; n < b.N; n++ {
		_, err := copyAny(id)
		require.NoError(b, err)
	}
	b.ReportAllocs()
}

func BenchmarkCopyAnyParallel(b *testing.B) {
	id := primitive.NewObjectID()
	b.SetBytes(int64(util.DeepSizeof(id)))

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := copyAny(id)
			require.NoError(b, err)
		}
	})
	b.ReportAllocs()
}
