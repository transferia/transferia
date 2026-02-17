package serializer

import (
	"bytes"
	"context"
	"io"

	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/changeitem/strictify"
	"golang.org/x/xerrors"
)

// strictifyingSerializer wraps a Serializer and applies strictification to each ChangeItem before serialization.
// Strictification converts ColumnValues to their canonical Go types according to the declared TableSchema.
// This ensures that the downstream serializer receives values of expected types (e.g., int64 for TypeInt64).
//
// Note: strictification modifies ChangeItem.ColumnValues in-place.
type strictifyingSerializer struct {
	inner Serializer
}

var _ Serializer = (*strictifyingSerializer)(nil)

func (s *strictifyingSerializer) Serialize(item *abstract.ChangeItem) ([]byte, error) {
	if err := strictifyItem(item); err != nil {
		return nil, xerrors.Errorf("strictifyingSerializer: %w", err)
	}
	return s.inner.Serialize(item)
}

func (s *strictifyingSerializer) SerializeWithSeparatorTo(item *abstract.ChangeItem, separator []byte, buf *bytes.Buffer) error {
	if err := strictifyItem(item); err != nil {
		return xerrors.Errorf("strictifyingSerializer: %w", err)
	}
	return s.inner.SerializeWithSeparatorTo(item, separator, buf)
}

func (s *strictifyingSerializer) Close() ([]byte, error) {
	return s.inner.Close()
}

// strictifyingBatchSerializer wraps a BatchSerializer and applies strictification to each ChangeItem before serialization.
// Used for serializers that implement BatchSerializer directly (e.g., Parquet) without going through the Serializer interface.
type strictifyingBatchSerializer struct {
	inner BatchSerializer
}

var _ BatchSerializer = (*strictifyingBatchSerializer)(nil)

func (s *strictifyingBatchSerializer) Serialize(items []*abstract.ChangeItem) ([]byte, error) {
	if err := strictifyItems(items); err != nil {
		return nil, xerrors.Errorf("strictifyingBatchSerializer: %w", err)
	}
	return s.inner.Serialize(items)
}

func (s *strictifyingBatchSerializer) SerializeAndWrite(ctx context.Context, items []*abstract.ChangeItem, writer io.Writer) (int, error) {
	if err := strictifyItems(items); err != nil {
		return 0, xerrors.Errorf("strictifyingBatchSerializer: %w", err)
	}
	return s.inner.SerializeAndWrite(ctx, items, writer)
}

func (s *strictifyingBatchSerializer) Close() ([]byte, error) {
	return s.inner.Close()
}

type strictifyingStreamSerializer struct {
	inner StreamSerializer
}

var _ StreamSerializer = (*strictifyingStreamSerializer)(nil)

func (s *strictifyingStreamSerializer) Serialize(items []*abstract.ChangeItem) error {
	if err := strictifyItems(items); err != nil {
		return xerrors.Errorf("strictifyingStreamSerializer: %w", err)
	}
	return s.inner.Serialize(items)
}

func (s *strictifyingStreamSerializer) Close() error {
	return s.inner.Close()
}

func strictifyItem(item *abstract.ChangeItem) error {
	if !item.IsRowEvent() {
		return nil
	}
	if item.TableSchema == nil {
		return nil
	}
	return strictify.Strictify(item, item.TableSchema.FastColumns())
}

func strictifyItems(items []*abstract.ChangeItem) error {
	for _, item := range items {
		if err := strictifyItem(item); err != nil {
			return err
		}
	}
	return nil
}

// NewStrictifyingSerializer wraps a Serializer with strictification.
func NewStrictifyingSerializer(inner Serializer) Serializer {
	return &strictifyingSerializer{inner: inner}
}

// NewStrictifyingBatchSerializer wraps a BatchSerializer with strictification.
func NewStrictifyingBatchSerializer(inner BatchSerializer) BatchSerializer {
	return &strictifyingBatchSerializer{inner: inner}
}

// NewStrictifyingStreamSerializer wraps a StreamSerializer with strictification.
func NewStrictifyingStreamSerializer(inner StreamSerializer) StreamSerializer {
	return &strictifyingStreamSerializer{inner: inner}
}
