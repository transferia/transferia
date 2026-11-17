package mongoerrors

// MongoDB server error codes from https://github.com/mongodb/mongo/blob/master/src/mongo/base/error_codes.yml
const (
	BadValueCode                = 2
	AuthenticationFailedCode    = 18
	PathNotViableCode           = 28
	NonExistentPathCode         = 29
	ChangeStreamFatalErrorCode  = 280
	ChangeStreamHistoryLostCode = 286
	BSONObjectTooLargeCode      = 10334
)

// StringTooLongCode is an assertion code absent from error_codes.yml: "Tried to create string longer than 16MB".
const StringTooLongCode = 16493
