package oneof

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"google.golang.org/protobuf/reflect/protoreflect"
)

var FieldNotFoundError = xerrors.New("Can't find field")

// Value get single one-of value. Extract actual one-of message instead of `IsOneOfFoo` interface wrapper
func Value(root protoreflect.Message) (protoreflect.ProtoMessage, error) {
	fields := root.Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		if field.Kind() != protoreflect.MessageKind || field.ContainingOneof() == nil {
			continue
		}
		if root.Has(field) {
			return root.Get(field).Message().Interface(), nil
		}
	}
	return nil, FieldNotFoundError
}

// SetValue will assign one-of value to a container with corresponding one-of
func SetValue(container protoreflect.Message, value protoreflect.Message) error {
	fields := container.Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		if field.Kind() != protoreflect.MessageKind || field.ContainingOneof() == nil {
			continue
		}
		if field.Message().FullName() == value.Descriptor().FullName() {
			container.Set(field, protoreflect.ValueOfMessage(value))
			return nil
		}
	}
	return xerrors.Errorf("Field %q not found: %w", value.Descriptor().FullName(), FieldNotFoundError)
}
