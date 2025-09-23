package errors

import (
	"fmt"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/library/go/core/xerrors/multierr"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/errors/categories"
	"github.com/transferia/transferia/pkg/errors/coded"
	"go.ytsaurus.tech/library/go/core/log"
)

const (
	KeyTransferID = "transfer_id"
	KeyDstType    = "labels.dst_type"
	KeySrcType    = "labels.src_type"
	Category      = "labels.category"
	Code          = "labels.code"
)

func LogFatalError(err error, transferID string, dstType abstract.ProviderType, srcType abstract.ProviderType) {
	defer func() {
		if r := recover(); r != nil {
			// If a panic occurs during logging, we log it as a critical error
			logger.FatalErrorLog.Error(
				"panic during error logging",
				log.String("panic", fmt.Sprintf("%v", r)),
				log.String(KeyTransferID, transferID),
				log.String(KeyDstType, dstType.Name()),
				log.String(KeySrcType, srcType.Name()),
				log.String(Category, string(categories.Internal)),
				log.String(Code, UnspecifiedCode.ID()),
			)
		}
	}()

	errs := multierr.Errors(err)
	for _, err := range errs {
		logFatalError(err, transferID, dstType, srcType)
	}
}

func logFatalError(err error, transferID string, dstType abstract.ProviderType, srcType abstract.ProviderType) {
	cat := categories.Internal
	var categorized Categorized = nil
	if xerrors.As(err, &categorized) {
		cat = categorized.Category()
	}
	code := UnspecifiedCode
	var codeErr coded.CodedError = nil
	if xerrors.As(err, &codeErr) {
		code = codeErr.Code()
	}
	msg := ExtractShortStackTrace(err)
	if code != UnspecifiedCode {
		msg = code.ID()
	}
	logger.FatalErrorLog.Error(
		msg,
		log.Error(err),
		log.String(KeyTransferID, transferID),
		log.String(KeyDstType, dstType.Name()),
		log.String(KeySrcType, srcType.Name()),
		log.String(Category, string(cat)),
		log.String(Code, code.ID()),
	)
}
