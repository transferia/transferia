package packer

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
)

// MockPacker is a mock implementation of the Packer interface for testing
type MockPacker struct {
	packResult []byte
	packError  error
}

func (m *MockPacker) Pack(
	changeItem *abstract.ChangeItem,
	payloadBuilder BuilderFunc,
	kafkaSchemaBuilder BuilderFunc,
	maybeCachedRawSchema []byte,
) ([]byte, error) {
	return m.packResult, m.packError
}

func (m *MockPacker) BuildFinalSchema(changeItem *abstract.ChangeItem, kafkaSchemaBuilder BuilderFunc) ([]byte, error) {
	return nil, nil
}

func (m *MockPacker) IsDropSchema() bool {
	return false
}

func (m *MockPacker) GetSchemaIDResolver() SchemaIDResolver {
	return nil
}

// MockExpirer is a mock implementation of the abstract.Expirer interface for testing
type MockExpirer struct {
	expiresAt time.Time
}

func (m *MockExpirer) ExpiresAt() time.Time {
	return m.expiresAt
}

// MockPackerFactory is a mock factory function for creating packers and expirers
func MockPackerFactory(packer Packer, expirer abstract.Expirer, factoryError error) ExpirablePackerFactory {
	return func() (Packer, abstract.Expirer, error) {
		if factoryError != nil {
			return nil, nil, factoryError
		}
		return packer, expirer, nil
	}
}

func TestNewPackerRenewableOnExpiration_Success(t *testing.T) {
	// Arrange
	mockPacker := &MockPacker{packResult: []byte("test result")}
	mockExpirer := &MockExpirer{expiresAt: time.Now().Add(time.Hour)}
	factory := MockPackerFactory(mockPacker, mockExpirer, nil)

	// Act
	reissuer, err := NewPackerRenewableOnExpiration(factory, logger.Log)

	// Assert
	require.NoError(t, err)
	require.NotNil(t, reissuer)
	require.Equal(t, mockPacker, reissuer.Packer)
	require.Equal(t, mockExpirer, reissuer.expirer)
}

func TestNewPackerRenewableOnExpiration_FactoryError(t *testing.T) {
	// Arrange
	expectedError := errors.New("factory error")
	factory := MockPackerFactory(nil, nil, expectedError)

	// Act
	reissuer, err := NewPackerRenewableOnExpiration(factory, logger.Log)

	// Assert
	require.Error(t, err)
	require.Nil(t, reissuer)
	require.Contains(t, err.Error(), "cannot construct new packer")
	require.Contains(t, err.Error(), expectedError.Error())
}

func TestNewPackerRenewableOnExpiration_ExpiredPacker(t *testing.T) {
	// Arrange
	mockPacker := &MockPacker{packResult: []byte("test result")}
	mockExpirer := &MockExpirer{expiresAt: time.Now().Add(-time.Hour)} // Expired
	factory := MockPackerFactory(mockPacker, mockExpirer, nil)

	// Act
	reissuer, err := NewPackerRenewableOnExpiration(factory, logger.Log)

	// Assert
	require.Error(t, err)
	require.Nil(t, reissuer)
	require.Contains(t, err.Error(), "cannot construct new packer")
	require.Contains(t, err.Error(), "cannot renew packer because new issue time is less than current time")
}

func TestPackerRenewableOnExpiration_Pack_NoRenewalNeeded(t *testing.T) {
	// Arrange
	mockPacker := &MockPacker{packResult: []byte("test result")}
	mockExpirer := &MockExpirer{expiresAt: time.Now().Add(time.Hour)}
	reissuer := &PackerRenewableOnExpiration{
		Packer:  mockPacker,
		expirer: mockExpirer,
		factory: MockPackerFactory(mockPacker, mockExpirer, nil),
		logger:  logger.Log,
	}

	changeItem := getTestChangeItem()
	payloadBuilder := func(changeItem *abstract.ChangeItem) ([]byte, error) { return []byte("payload"), nil }
	kafkaSchemaBuilder := func(changeItem *abstract.ChangeItem) ([]byte, error) { return []byte("schema"), nil }

	// Act
	result, err := reissuer.Pack(changeItem, payloadBuilder, kafkaSchemaBuilder, nil)

	// Assert
	require.NoError(t, err)
	require.Equal(t, []byte("test result"), result)
}

func TestPackerRenewableOnExpiration_Pack_RenewalNeeded_Success(t *testing.T) {
	// Arrange
	// Create an expired packer initially
	expiredPacker := &MockPacker{packResult: []byte("expired result")}
	expiredExpirer := &MockExpirer{expiresAt: time.Now().Add(-time.Hour)}

	// Create a valid packer for renewal
	validPacker := &MockPacker{packResult: []byte("valid result")}
	validExpirer := &MockExpirer{expiresAt: time.Now().Add(time.Hour)}

	reissuer := &PackerRenewableOnExpiration{
		Packer:  expiredPacker,
		expirer: expiredExpirer,
		factory: MockPackerFactory(validPacker, validExpirer, nil),
		logger:  logger.Log,
	}

	changeItem := getTestChangeItem()
	payloadBuilder := func(changeItem *abstract.ChangeItem) ([]byte, error) { return []byte("payload"), nil }
	kafkaSchemaBuilder := func(changeItem *abstract.ChangeItem) ([]byte, error) { return []byte("schema"), nil }

	// Act
	result, err := reissuer.Pack(changeItem, payloadBuilder, kafkaSchemaBuilder, nil)

	// Assert
	require.NoError(t, err)
	require.Equal(t, []byte("valid result"), result)
	// Check that the packer was renewed
	require.Equal(t, validPacker, reissuer.Packer)
	require.Equal(t, validExpirer, reissuer.expirer)
}

func TestPackerRenewableOnExpiration_Pack_RenewalNeeded_FactoryError(t *testing.T) {
	// Arrange
	expiredPacker := &MockPacker{packResult: []byte("expired result")}
	expiredExpirer := &MockExpirer{expiresAt: time.Now().Add(-time.Hour)}
	factoryError := errors.New("renewal factory error")

	reissuer := &PackerRenewableOnExpiration{
		Packer:  expiredPacker,
		expirer: expiredExpirer,
		factory: MockPackerFactory(nil, nil, factoryError),
		logger:  logger.Log,
	}

	changeItem := getTestChangeItem()
	payloadBuilder := func(changeItem *abstract.ChangeItem) ([]byte, error) { return []byte("payload"), nil }
	kafkaSchemaBuilder := func(changeItem *abstract.ChangeItem) ([]byte, error) { return []byte("schema"), nil }

	// Act
	result, err := reissuer.Pack(changeItem, payloadBuilder, kafkaSchemaBuilder, nil)

	// Assert
	require.Error(t, err)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "can't renew packer on expiration")
	require.Contains(t, err.Error(), factoryError.Error())
}

func TestPackerRenewableOnExpiration_Pack_RenewalNeeded_ExpiredNewPacker(t *testing.T) {
	// Arrange
	expiredPacker := &MockPacker{packResult: []byte("expired result")}
	expiredExpirer := &MockExpirer{expiresAt: time.Now().Add(-time.Hour)}

	// New packer is also expired
	newExpiredPacker := &MockPacker{packResult: []byte("new expired result")}
	newExpiredExpirer := &MockExpirer{expiresAt: time.Now().Add(-time.Hour)}

	reissuer := &PackerRenewableOnExpiration{
		Packer:  expiredPacker,
		expirer: expiredExpirer,
		factory: MockPackerFactory(newExpiredPacker, newExpiredExpirer, nil),
		logger:  logger.Log,
	}

	changeItem := getTestChangeItem()
	payloadBuilder := func(changeItem *abstract.ChangeItem) ([]byte, error) { return []byte("payload"), nil }
	kafkaSchemaBuilder := func(changeItem *abstract.ChangeItem) ([]byte, error) { return []byte("schema"), nil }

	// Act
	result, err := reissuer.Pack(changeItem, payloadBuilder, kafkaSchemaBuilder, nil)

	// Assert
	require.Error(t, err)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "can't renew packer on expiration")
	require.Contains(t, err.Error(), "cannot renew packer because new issue time is less than current time")
}

func TestPackerRenewableOnExpiration_Pack_OriginalPackerError(t *testing.T) {
	// Arrange
	packerError := errors.New("packer error")
	mockPacker := &MockPacker{packError: packerError}
	mockExpirer := &MockExpirer{expiresAt: time.Now().Add(time.Hour)}
	reissuer := &PackerRenewableOnExpiration{
		Packer:  mockPacker,
		expirer: mockExpirer,
		factory: MockPackerFactory(mockPacker, mockExpirer, nil),
		logger:  logger.Log,
	}

	changeItem := getTestChangeItem()
	payloadBuilder := func(changeItem *abstract.ChangeItem) ([]byte, error) { return []byte("payload"), nil }
	kafkaSchemaBuilder := func(changeItem *abstract.ChangeItem) ([]byte, error) { return []byte("schema"), nil }

	// Act
	result, err := reissuer.Pack(changeItem, payloadBuilder, kafkaSchemaBuilder, nil)

	// Assert
	require.Error(t, err)
	require.Nil(t, result)
	require.Equal(t, packerError, err)
}
