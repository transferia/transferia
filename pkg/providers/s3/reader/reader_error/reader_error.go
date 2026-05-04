package reader_error

import "fmt"

//---------------------------------------------------------------------------------------------------------------------
// main interface

type ReaderError interface {
	error
	isReaderError()
}

//---------------------------------------------------------------------------------------------------------------------
// additional enum

type DataScope string

const (
	ScopeRecord DataScope = "record"
	ScopeFile   DataScope = "file"
	ScopeSchema DataScope = "schema"
)

//---------------------------------------------------------------------------------------------------------------------
// errors

type ReaderErrorTransport struct {
	scope DataScope
	op    string
	file  string
	row   uint64
	cause error
}

var _ ReaderError = (*ReaderErrorTransport)(nil)

func (e ReaderErrorTransport) Error() string { return e.cause.Error() }
func (e ReaderErrorTransport) Unwrap() error { return e.cause }
func (ReaderErrorTransport) isReaderError()  {}

func NewReaderErrorTransport(op, file string, err error) ReaderErrorTransport {
	return ReaderErrorTransport{scope: "", op: op, file: file, row: 0, cause: err}
}

// Op returns the operation label for transport-layer errors.
func (e ReaderErrorTransport) Op() string { return e.op }

// File returns the object key / path associated with the transport error.
func (e ReaderErrorTransport) File() string { return e.file }

//-

type ReaderErrorData struct {
	Scope DataScope
	Op    string
	File  string
	Row   uint64
	Cause error
}

var _ ReaderError = (*ReaderErrorData)(nil)

func (e ReaderErrorData) Error() string { return e.Cause.Error() }
func (e ReaderErrorData) Unwrap() error { return e.Cause }
func (ReaderErrorData) isReaderError()  {}

func NewReaderErrorDataRecord(op, file string, row uint64, cause error) ReaderErrorData {
	return ReaderErrorData{Scope: ScopeRecord, Op: op, File: file, Row: row, Cause: cause}
}

func NewReaderErrorDataFile(op, file string, cause error) ReaderErrorData {
	return ReaderErrorData{Scope: ScopeFile, Op: op, File: file, Row: 0, Cause: cause}
}

func NewReaderErrorDataSchema(op, file string, cause error) ReaderErrorData {
	return ReaderErrorData{Scope: ScopeSchema, Op: op, File: file, Row: 0, Cause: cause}
}

//-

type ReaderErrorConfig struct {
	op    string
	cause error
}

var _ ReaderError = (*ReaderErrorConfig)(nil)

func (e ReaderErrorConfig) Error() string { return e.cause.Error() }
func (e ReaderErrorConfig) Unwrap() error { return e.cause }
func (ReaderErrorConfig) isReaderError()  {}

func NewReaderErrorConfig(op string, err error) ReaderErrorConfig {
	return ReaderErrorConfig{op: op, cause: err}
}

//-

type ReaderErrorSink struct {
	op    string
	file  string
	row   uint64
	cause error
}

var _ ReaderError = (*ReaderErrorSink)(nil)

func (e ReaderErrorSink) Error() string { return e.cause.Error() }
func (e ReaderErrorSink) Unwrap() error { return e.cause }
func (ReaderErrorSink) isReaderError()  {}

func NewReaderErrorSink(op, file string, err error) ReaderErrorSink {
	return ReaderErrorSink{op: op, file: file, row: 0, cause: err}
}

//-

type ReaderErrorFatal struct {
	op    string
	cause error
}

var _ ReaderError = (*ReaderErrorFatal)(nil)

func (e ReaderErrorFatal) Error() string { return e.cause.Error() }
func (e ReaderErrorFatal) Unwrap() error { return e.cause }
func (ReaderErrorFatal) isReaderError()  {}
func (ReaderErrorFatal) IsFatal()        {}

func NewReaderErrorFatal(op string, err error) ReaderErrorFatal {
	return ReaderErrorFatal{op: op, cause: err}
}

//-

// ReaderErrorNoFiles is returned when schema inference requires listing objects but none match the prefix/pattern.
type ReaderErrorNoFiles struct {
	op     string
	prefix string
}

var _ ReaderError = (*ReaderErrorNoFiles)(nil)

func (e ReaderErrorNoFiles) Error() string {
	return fmt.Sprintf("unable to resolve schema, no files found: %s", e.prefix)
}

func (ReaderErrorNoFiles) isReaderError() {}

func NewReaderErrorNoFiles(op, prefix string) ReaderErrorNoFiles {
	return ReaderErrorNoFiles{op: op, prefix: prefix}
}

// Op returns the operation label (e.g. csv.ResolveSchema).
func (e ReaderErrorNoFiles) Op() string { return e.op }

// Prefix returns the S3 path prefix used when listing.
func (e ReaderErrorNoFiles) Prefix() string { return e.prefix }
