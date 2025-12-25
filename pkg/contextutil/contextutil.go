package contextutil

type contextKey struct {
	*contextKey
}

func NewContextKey() interface{} {
	x := new(contextKey)
	x.contextKey = x
	return x
}
