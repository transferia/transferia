package lightningcache

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/debezium"
	"github.com/transferria/transferria/pkg/debezium/packer"
)

type PackerSchemaRegistryMocked struct {
	t              *testing.T
	packerImpl     packer.Packer
	resolvingCount int
}

func (s *PackerSchemaRegistryMocked) Pack(
	_ *abstract.ChangeItem,
	_ packer.BuilderFunc,
	_ packer.BuilderFunc,
	_ []byte,
) ([]byte, error) {
	return nil, xerrors.New("PackerSchemaRegistryMocked::BuildFinalSchema unimplemented for test")
}

func (s *PackerSchemaRegistryMocked) BuildFinalSchema(_ *abstract.ChangeItem, _ packer.BuilderFunc) ([]byte, error) {
	return nil, xerrors.New("PackerSchemaRegistryMocked::Pack unimplemented for test")
}

func (s *PackerSchemaRegistryMocked) IsDropSchema() bool {
	return false
}

func (s *PackerSchemaRegistryMocked) PackWithSchemaID(_ uint32, _ []byte) ([]byte, error) {
	return nil, xerrors.New("PackerSchemaRegistryMocked::PackWithSchemaID unimplemented for test")
}

func (s *PackerSchemaRegistryMocked) ResolveSchemaID(_ []byte, _ abstract.TableID) (uint32, error) {
	require.Equal(s.t, 0, s.resolvingCount)
	s.resolvingCount++
	return 9, nil
}

func (s *PackerSchemaRegistryMocked) GetSchemaIDResolver() packer.SchemaIDResolver { return s }

func NewPackerSchemaRegistryMocked(t *testing.T, packer packer.Packer) *PackerSchemaRegistryMocked {
	return &PackerSchemaRegistryMocked{
		t:              t,
		packerImpl:     packer,
		resolvingCount: 0,
	}
}

//---

func TestIsNewSchemaWithSchemaID(t *testing.T) {
	emitter, err := debezium.NewMessagesEmitter(nil, "1.1.2.Final", false, logger.Log)
	require.NoError(t, err)

	originalKeyPacker, originalValPacker := emitter.GetPackers()
	originalKeyPackerSRMocked := NewPackerSchemaRegistryMocked(t, originalKeyPacker)
	originalValPackerSRMocked := NewPackerSchemaRegistryMocked(t, originalValPacker)

	packerCache, err := NewSessionPackersSchemaIDCache(originalKeyPackerSRMocked, originalValPackerSRMocked)
	require.NoError(t, err)

	tableSchema0 := abstract.NewTableSchema([]abstract.ColSchema{{ColumnName: "my_col_0"}})
	tableSchema1 := abstract.NewTableSchema([]abstract.ColSchema{{ColumnName: "my_col_1"}})

	changeItemA := &abstract.ChangeItem{
		TableSchema: tableSchema0,
	}
	changeItemB := &abstract.ChangeItem{
		TableSchema: tableSchema0,
	}
	changeItemC := &abstract.ChangeItem{
		TableSchema: tableSchema1,
	}

	// key

	require.True(t, packerCache.IsNewSchema(true, changeItemA))

	err = packerCache.GetAndSaveFinalSchemaAndMaybeSchemaID(true, changeItemA, []byte{1})
	require.NoError(t, err)

	require.False(t, packerCache.IsNewSchema(true, changeItemA))

	require.False(t, packerCache.IsNewSchema(true, changeItemB))
	require.True(t, packerCache.IsNewSchema(true, changeItemC))

	// val

	require.True(t, packerCache.IsNewSchema(false, changeItemA))

	err = packerCache.GetAndSaveFinalSchemaAndMaybeSchemaID(false, changeItemA, []byte{1})
	require.NoError(t, err)

	require.False(t, packerCache.IsNewSchema(false, changeItemA))

	require.False(t, packerCache.IsNewSchema(false, changeItemB))
	require.True(t, packerCache.IsNewSchema(false, changeItemC))

	// check 'finalSchemaCacheByAddr' wasn't used

	require.Len(t, packerCache.keyPacker.schemaIDCacheByAddr, 1)
	require.Len(t, packerCache.valPacker.schemaIDCacheByAddr, 1)
	require.Len(t, packerCache.keyPacker.finalSchemaCacheByAddr, 0)
	require.Len(t, packerCache.valPacker.finalSchemaCacheByAddr, 0)
}

func TestIsNewSchemaWithFinalSchemas(t *testing.T) {
	emitter, err := debezium.NewMessagesEmitter(nil, "1.1.2.Final", false, logger.Log)
	require.NoError(t, err)

	packerCache, err := NewSessionPackersSchemaIDCache(emitter.GetPackers())
	require.NoError(t, err)

	tableSchema0 := abstract.NewTableSchema([]abstract.ColSchema{{ColumnName: "my_col_0"}})
	tableSchema1 := abstract.NewTableSchema([]abstract.ColSchema{{ColumnName: "my_col_1"}})

	changeItemA := &abstract.ChangeItem{
		TableSchema: tableSchema0,
	}
	changeItemB := &abstract.ChangeItem{
		TableSchema: tableSchema0,
	}
	changeItemC := &abstract.ChangeItem{
		TableSchema: tableSchema1,
	}

	// key

	require.True(t, packerCache.IsNewSchema(true, changeItemA))
	err = packerCache.GetAndSaveFinalSchemaAndMaybeSchemaID(true, changeItemA, []byte{1})
	require.NoError(t, err)
	require.False(t, packerCache.IsNewSchema(true, changeItemA))

	require.False(t, packerCache.IsNewSchema(true, changeItemB))

	require.True(t, packerCache.IsNewSchema(true, changeItemC))

	// val

	require.True(t, packerCache.IsNewSchema(false, changeItemA))
	err = packerCache.GetAndSaveFinalSchemaAndMaybeSchemaID(false, changeItemA, []byte{1})
	require.NoError(t, err)
	require.False(t, packerCache.IsNewSchema(false, changeItemA))

	require.False(t, packerCache.IsNewSchema(false, changeItemB))

	require.True(t, packerCache.IsNewSchema(false, changeItemC))

	// check 'schemaIDCacheByAddr' wasn't used

	require.Len(t, packerCache.keyPacker.schemaIDCacheByAddr, 0)
	require.Len(t, packerCache.valPacker.schemaIDCacheByAddr, 0)
	require.Len(t, packerCache.keyPacker.finalSchemaCacheByAddr, 1)
	require.Len(t, packerCache.valPacker.finalSchemaCacheByAddr, 1)
}
