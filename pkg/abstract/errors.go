package abstract

import "github.com/transferria/transferria/pkg/abstract/dterrors"

type RetriablePartUploadError = dterrors.RetriablePartUploadError
type TableUploadError = dterrors.TableUploadError

var CheckErrorWrapping = dterrors.CheckErrorWrapping
var CheckOpaqueErrorWrapping = dterrors.CheckOpaqueErrorWrapping
var IsFatal = dterrors.IsFatal
var IsNonShardableError = dterrors.IsNonShardableError
var IsRetriablePartUploadError = dterrors.IsRetriablePartUploadError
var IsTableUploadError = dterrors.IsTableUploadError
var NewFatalError = dterrors.NewFatalError
var NewNonShardableError = dterrors.NewNonShardableError
var NewTableUploadError = dterrors.NewTableUploadError
var NewRetriablePartUploadError = dterrors.NewRetriablePartUploadError
