package parsers

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/changeitem"
)

// MockParser is a mock implementation of the Parser interface for testing
type MockParser struct {
	doFunc      func(msg Message, partition abstract.Partition) []abstract.ChangeItem
	doBatchFunc func(batch MessageBatch) []abstract.ChangeItem
}

func (m *MockParser) Do(msg Message, partition abstract.Partition) []abstract.ChangeItem {
	if m.doFunc != nil {
		return m.doFunc(msg, partition)
	}
	return nil
}

func (m *MockParser) DoBatch(batch MessageBatch) []abstract.ChangeItem {
	if m.doBatchFunc != nil {
		return m.doBatchFunc(batch)
	}
	return nil
}

// MockExpirer is a mock implementation of the Expirer interface for testing
type MockExpirer struct {
	expiresAt time.Time
}

func (m *MockExpirer) ExpiresAt() time.Time {
	return m.expiresAt
}

// TestYSRableParser_Do tests the Do method of YSRableParser
func TestYSRableParser_Do(t *testing.T) {
	// Test case 1: Parser is not expired, Do should be called directly
	t.Run("not_expired", func(t *testing.T) {
		expectedItems := []abstract.ChangeItem{
			{Kind: changeitem.InsertKind, Table: "test_table"},
		}

		mockParser := &MockParser{
			doFunc: func(msg Message, partition abstract.Partition) []abstract.ChangeItem {
				return expectedItems
			},
		}

		parser := &YSRableParser{
			Parser:  mockParser,
			expirer: &MockExpirer{expiresAt: time.Now().Add(time.Hour)}, // Not expired
			logger:  logger.Log,
		}

		msg := Message{Value: []byte("test")}
		partition := abstract.NewPartition("rt3.test--test_topic", 0)

		items := parser.Do(msg, partition)
		require.Equal(t, expectedItems, items)
	})

	// Test case 2: Parser is expired, should renew parser and then call Do
	t.Run("expired_then_renew", func(t *testing.T) {
		expectedItems := []abstract.ChangeItem{
			{Kind: changeitem.UpdateKind, Table: "test_table"},
		}

		// Create a factory that returns a new parser and expirer
		factory := func() (Parser, abstract.Expirer, error) {
			newParser := &MockParser{
				doFunc: func(msg Message, partition abstract.Partition) []abstract.ChangeItem {
					return expectedItems
				},
			}
			newExpirer := &MockExpirer{expiresAt: time.Now().Add(time.Hour)} // Not expired
			return newParser, newExpirer, nil
		}

		parser := &YSRableParser{
			Parser:  &MockParser{},                                       // Initial parser
			expirer: &MockExpirer{expiresAt: time.Now().Add(-time.Hour)}, // Expired
			factory: factory,
			logger:  logger.Log,
		}

		msg := Message{Value: []byte("test")}
		partition := abstract.NewPartition("rt3.test--test_topic", 0)

		items := parser.Do(msg, partition)
		require.Equal(t, expectedItems, items)
	})
}

// TestYSRableParser_DoBatch tests the DoBatch method of YSRableParser
func TestYSRableParser_DoBatch(t *testing.T) {
	// Test case 1: Parser is not expired, DoBatch should be called directly
	t.Run("not_expired", func(t *testing.T) {
		expectedItems := []abstract.ChangeItem{
			{Kind: changeitem.DeleteKind, Table: "test_table"},
		}

		mockParser := &MockParser{
			doBatchFunc: func(batch MessageBatch) []abstract.ChangeItem {
				return expectedItems
			},
		}

		parser := &YSRableParser{
			Parser:  mockParser,
			expirer: &MockExpirer{expiresAt: time.Now().Add(time.Hour)}, // Not expired
			logger:  logger.Log,
		}

		batch := MessageBatch{
			Topic:     "test_topic",
			Partition: 0,
			Messages:  []Message{{Value: []byte("test")}},
		}

		items := parser.DoBatch(batch)
		require.Equal(t, expectedItems, items)
	})

	// Test case 2: Parser is expired, should renew parser and then call DoBatch
	t.Run("expired_then_renew", func(t *testing.T) {
		expectedItems := []abstract.ChangeItem{
			{Kind: changeitem.InsertKind, Table: "renewed_table"},
		}

		// Create a factory that returns a new parser and expirer
		factory := func() (Parser, abstract.Expirer, error) {
			newParser := &MockParser{
				doBatchFunc: func(batch MessageBatch) []abstract.ChangeItem {
					return expectedItems
				},
			}
			newExpirer := &MockExpirer{expiresAt: time.Now().Add(time.Hour)} // Not expired
			return newParser, newExpirer, nil
		}

		parser := &YSRableParser{
			Parser:  &MockParser{},                                       // Initial parser
			expirer: &MockExpirer{expiresAt: time.Now().Add(-time.Hour)}, // Expired
			factory: factory,
			logger:  logger.Log,
		}

		batch := MessageBatch{
			Topic:     "test_topic",
			Partition: 0,
			Messages:  []Message{{Value: []byte("test")}},
		}

		items := parser.DoBatch(batch)
		require.Equal(t, expectedItems, items)
	})
}

// TestYSRableParser_Unwrap tests the Unwrap method of YSRableParser
func TestYSRableParser_Unwrap(t *testing.T) {
	mockParser := &MockParser{}
	parser := &YSRableParser{
		Parser: mockParser,
		logger: logger.Log,
	}

	unwrapped := parser.Unwrap()
	require.Equal(t, mockParser, unwrapped)
}

// TestYSRableParser_YSRNamespaceID tests the YSRNamespaceID method of YSRableParser
func TestYSRableParser_YSRNamespaceID(t *testing.T) {
	expectedNamespaceID := "test-namespace-id"
	parser := &YSRableParser{
		ysrNamespaceID: expectedNamespaceID,
		logger:         logger.Log,
	}

	namespaceID := parser.YSRNamespaceID()
	require.Equal(t, expectedNamespaceID, namespaceID)
}

// TestYSRableParser_RenewParser tests the renewParser method of YSRableParser
func TestYSRableParser_RenewParser(t *testing.T) {
	// Test case 1: Successful renewal
	t.Run("successful_renewal", func(t *testing.T) {
		factory := func() (Parser, abstract.Expirer, error) {
			newParser := &MockParser{}
			newExpirer := &MockExpirer{expiresAt: time.Now().Add(time.Hour)}
			return newParser, newExpirer, nil
		}

		parser := &YSRableParser{
			Parser:  &MockParser{},
			expirer: &MockExpirer{expiresAt: time.Now().Add(-time.Hour)},
			factory: factory,
			logger:  logger.Log,
		}

		err := parser.renewParser()
		require.NoError(t, err)
	})

	// Test case 2: Factory returns error
	t.Run("factory_error", func(t *testing.T) {
		expectedErr := xerrors.New("factory error")
		factory := func() (Parser, abstract.Expirer, error) {
			return nil, nil, expectedErr
		}

		parser := &YSRableParser{
			factory: factory,
			logger:  logger.Log,
		}

		err := parser.renewParser()
		require.Error(t, err)
		require.Contains(t, err.Error(), "cannnot create new parser: factory error")
	})

	// Test case 3: New parser is expired
	t.Run("new_parser_expired", func(t *testing.T) {
		factory := func() (Parser, abstract.Expirer, error) {
			newParser := &MockParser{}
			newExpirer := &MockExpirer{expiresAt: time.Now().Add(-time.Hour)} // Expired
			return newParser, newExpirer, nil
		}

		parser := &YSRableParser{
			factory: factory,
			logger:  logger.Log,
		}

		err := parser.renewParser()
		require.Error(t, err)
		require.Contains(t, err.Error(), "cannot renew parser because new issue time is less than current time")
	})
}

// TestWithYSRNamespaceIDs tests the WithYSRNamespaceIDs function
func TestWithYSRNamespaceIDs(t *testing.T) {
	// Test case 1: Successful creation
	t.Run("successful_creation", func(t *testing.T) {
		namespaceID := "test-namespace"
		factory := func() (Parser, abstract.Expirer, error) {
			parser := &MockParser{}
			expirer := &MockExpirer{expiresAt: time.Now().Add(time.Hour)}
			return parser, expirer, nil
		}

		parser, err := WithYSRNamespaceIDs(factory, namespaceID, logger.Log)
		require.NoError(t, err)
		require.NotNil(t, parser)

		ysrable, ok := parser.(YSRable)
		require.True(t, ok)
		require.Equal(t, namespaceID, ysrable.YSRNamespaceID())
	})

	// Test case 2: Factory returns error
	t.Run("factory_error", func(t *testing.T) {
		expectedErr := xerrors.New("factory error")
		factory := func() (Parser, abstract.Expirer, error) {
			return nil, nil, expectedErr
		}

		parser, err := WithYSRNamespaceIDs(factory, "test-namespace", logger.Log)
		require.Error(t, err)
		require.Nil(t, parser)
		require.Contains(t, err.Error(), "cannnot create new parser: factory error")
	})
}
