package errors

import (
	"sort"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/errors/categories"
	"github.com/transferia/transferia/pkg/errors/coded"
	"github.com/transferia/transferia/pkg/terryid"
)

var UnspecifiedCode = coded.Register("unspecified")

type statusMessageError struct {
	statusMessage *coordinator.StatusMessage
}

func (f *statusMessageError) Error() string {
	return f.statusMessage.Heading
}

func NewStatusMessageError(msg *coordinator.StatusMessage) error {
	return &statusMessageError{
		statusMessage: msg,
	}
}

func ToTransferStatusMessage(err error) *coordinator.StatusMessage {
	var protoStatusErr *statusMessageError = nil
	if xerrors.As(err, &protoStatusErr) {
		return protoStatusErr.statusMessage
	}
	result := &coordinator.StatusMessage{
		ID:         terryid.GenerateTransferStatusMessageID(), // TODO: re-think, do we need any ID here.
		Type:       coordinator.ErrorStatusMessageType,
		Heading:    "",
		Message:    "",
		Categories: make([]string, 0),
		Code:       "",
	}

	if err == nil {
		return result
	}

	resultCategory := categories.Internal // the default category is assigned here
	var categorized Categorized = nil
	if xerrors.As(err, &categorized) {
		resultCategory = categorized.Category()
	}

	resultCode := UnspecifiedCode
	previous := err
	current := err
	for {
		// we should take only top-level code
		// so once we have our resultCode we should be fine
		var currentCoded coded.CodedError = nil
		if xerrors.As(current, &currentCoded) && resultCode == UnspecifiedCode {
			resultCode = currentCoded.Code()
		}

		currentWrapper, ok := current.(xerrors.Wrapper)
		if !ok {
			break
		}
		newCurrent := currentWrapper.Unwrap()
		if newCurrent == nil {
			break
		}
		previous = current
		current = newCurrent
	}

	result.Heading = previous.Error()
	result.Message = err.Error()
	result.Categories = []string{resultCategory.ID()}
	result.Code = resultCode

	sort.Strings(result.Categories)
	return result
}
