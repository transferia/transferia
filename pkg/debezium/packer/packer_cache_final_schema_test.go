package packer

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
)

type CacheKafkaSchemaTest struct {
	counter int
	t       *testing.T
}

func (c *CacheKafkaSchemaTest) GetSchemaIDResolver() SchemaIDResolver {
	return nil
}

func (c *CacheKafkaSchemaTest) Pack(
	_ *abstract.ChangeItem,
	_ BuilderFunc,
	_ BuilderFunc,
	schemaBuf []byte,
) ([]byte, error) {
	require.NotNil(c.t, schemaBuf)
	require.Equal(c.t, []byte{2}, schemaBuf)
	return nil, nil
}

func (c *CacheKafkaSchemaTest) BuildFinalSchema(changeItem *abstract.ChangeItem, kafkaSchemaBuilder BuilderFunc) ([]byte, error) {
	require.Equal(c.t, 0, c.counter)
	c.counter++
	return kafkaSchemaBuilder(changeItem)
}

func (c *CacheKafkaSchemaTest) IsDropSchema() bool {
	return false
}

func NewCacheConversionToFinalSchemaTest(t *testing.T) *CacheKafkaSchemaTest {
	return &CacheKafkaSchemaTest{
		counter: 0,
		t:       t,
	}
}

//---

func TestCacheConversionToFinalSchema(t *testing.T) {
	packerImpl := NewCacheConversionToFinalSchemaTest(t)
	packerWithCache := NewPackerCacheFinalSchema(packerImpl)
	result, err := packerWithCache.Pack(
		getTestChangeItem(),
		func(changeItem *abstract.ChangeItem) ([]byte, error) { return []byte{1}, nil },
		func(changeItem *abstract.ChangeItem) ([]byte, error) { return []byte{2}, nil },
		nil,
	)
	require.NoError(t, err)
	require.Nil(t, result)

	result, err = packerWithCache.Pack(
		getTestChangeItem(),
		func(changeItem *abstract.ChangeItem) ([]byte, error) { return []byte{1}, nil },
		func(changeItem *abstract.ChangeItem) ([]byte, error) { return []byte{2}, nil },
		nil,
	)
	require.NoError(t, err)
	require.Nil(t, result)
}
