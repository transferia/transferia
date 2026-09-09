package model

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
)

type restrictedCDCReplicationDestination struct {
	MockDestination
}

func (*restrictedCDCReplicationDestination) IsQueueOffsetDependantDestination() {}
func (*restrictedCDCReplicationDestination) IsSingleTableDestination()          {}

type tableCDCReplicationSource struct {
	MockSource
	includes []string
}

func (*tableCDCReplicationSource) IsQueueToS3Source() {}

func (*tableCDCReplicationSource) Include(abstract.TableID) bool {
	return true
}

func (s *tableCDCReplicationSource) FulfilledIncludes(abstract.TableID) []string {
	return s.includes
}

func (s *tableCDCReplicationSource) AllIncludes() []string {
	return s.includes
}

type parsedCDCReplicationSource struct {
	MockSource
	parser map[string]interface{}
}

func (*parsedCDCReplicationSource) IsQueueToS3Source() {}

func (s *parsedCDCReplicationSource) Parser() map[string]interface{} {
	return s.parser
}

func TestValidateQueueCDCReplication(t *testing.T) {
	destination := &restrictedCDCReplicationDestination{}
	transformation := &Transformation{ExtraTransformers: []abstract.Transformer{nil}}

	t.Run("transformations are forbidden", func(t *testing.T) {
		transfer := &Transfer{
			Type:           abstract.TransferTypeIncrementOnly,
			Src:            &tableCDCReplicationSource{includes: []string{"public.table"}},
			Dst:            destination,
			Transformation: transformation,
		}
		err := transfer.Validate()
		require.EqualError(t, err, "transformations are not supported for replication to mock")
	})

	t.Run("snapshot transformations are unaffected", func(t *testing.T) {
		transfer := &Transfer{
			Type:           abstract.TransferTypeSnapshotOnly,
			Src:            &tableCDCReplicationSource{includes: []string{"public.first", "public.second"}},
			Dst:            destination,
			Transformation: transformation,
		}
		require.NoError(t, transfer.Validate())
	})

	t.Run("one exact database table", func(t *testing.T) {
		transfer := &Transfer{
			Type: abstract.TransferTypeIncrementOnly,
			Src:  &tableCDCReplicationSource{includes: []string{"public.table"}},
			Dst:  destination,
		}
		require.NoError(t, transfer.Validate())
	})

	t.Run("multiple database tables", func(t *testing.T) {
		transfer := &Transfer{
			Type: abstract.TransferTypeIncrementOnly,
			Src:  &tableCDCReplicationSource{includes: []string{"public.first", "public.second"}},
			Dst:  destination,
		}
		err := transfer.Validate()
		require.EqualError(t, err, "replication to mock supports exactly one CDC table, got 2")
	})

	t.Run("transfer filter narrows database tables", func(t *testing.T) {
		transfer := &Transfer{
			Type:        abstract.TransferTypeIncrementOnly,
			Src:         &tableCDCReplicationSource{includes: []string{"public.*"}},
			Dst:         destination,
			DataObjects: &DataObjects{IncludeObjects: []string{"public.table"}},
		}
		require.NoError(t, transfer.Validate())
	})

	t.Run("database wildcard is not an exact table", func(t *testing.T) {
		transfer := &Transfer{
			Type: abstract.TransferTypeIncrementOnly,
			Src:  &tableCDCReplicationSource{includes: []string{"public.*"}},
			Dst:  destination,
		}
		err := transfer.Validate()
		require.EqualError(t, err, `replication to mock requires one exact CDC table, got "public.*"`)
	})

	t.Run("Debezium queue is forbidden regardless of topic count", func(t *testing.T) {
		for _, parserName := range []string{"debezium.common", "debezium.lb"} {
			t.Run(parserName, func(t *testing.T) {
				for _, tc := range []struct {
					name        string
					dataObjects *DataObjects
				}{
					{name: "no transfer topic filter"},
					{name: "one topic", dataObjects: &DataObjects{IncludeObjects: []string{"events"}}},
					{name: "multiple topics", dataObjects: &DataObjects{IncludeObjects: []string{"events_a", "events_b"}}},
				} {
					t.Run(tc.name, func(t *testing.T) {
						transfer := &Transfer{
							Type: abstract.TransferTypeIncrementOnly,
							Src: &parsedCDCReplicationSource{
								parser: map[string]interface{}{parserName: nil},
							},
							Dst:         destination,
							DataObjects: tc.dataObjects,
						}
						require.EqualError(t, transfer.Validate(), "Debezium parser is not supported for replication to mock: the event stream may contain multiple CDC tables")
					})
				}
			})
		}
	})

	for _, tc := range []struct {
		name         string
		parser       map[string]interface{}
		transferType abstract.TransferType
		destination  Destination
	}{
		{
			name:         "queue without parser is unaffected",
			transferType: abstract.TransferTypeIncrementOnly,
			destination:  destination,
		},
		{
			name:         "other queue parser is unaffected",
			parser:       map[string]interface{}{"json.common": nil},
			transferType: abstract.TransferTypeIncrementOnly,
			destination:  destination,
		},
		{
			name:         "Debezium to unrestricted destination is unaffected",
			parser:       map[string]interface{}{"debezium.common": nil},
			transferType: abstract.TransferTypeIncrementOnly,
			destination:  &MockDestination{},
		},
		{
			name:         "Debezium snapshot is unaffected",
			parser:       map[string]interface{}{"debezium.common": nil},
			transferType: abstract.TransferTypeSnapshotOnly,
			destination:  destination,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			transfer := &Transfer{
				Type: tc.transferType,
				Src:  &parsedCDCReplicationSource{parser: tc.parser},
				Dst:  tc.destination,
			}
			require.NoError(t, transfer.Validate())
		})
	}
}
