package mongoerrors

// MongoDB server error codes from https://github.com/mongodb/mongo/blob/master/src/mongo/base/error_codes.yml
const (
	AuthenticationFailedCode    = 18
	ChangeStreamFatalErrorCode  = 280
	ChangeStreamHistoryLostCode = 286
	BSONObjectTooLargeCode      = 10334
)

// StringTooLongCode is an assertion code absent from error_codes.yml: "Tried to create string longer than 16MB".
const StringTooLongCode = 16493
