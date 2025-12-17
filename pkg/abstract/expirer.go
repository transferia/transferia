package abstract

import "time"

type Expirer interface {
	ExpiresAt() time.Time
}

func ExpiresAt(e Expirer) time.Time {
	if e == nil {
		return time.Time{}
	}
	return e.ExpiresAt()
}

func Expired(e Expirer) bool {
	if e == nil {
		return false
	}
	return e.ExpiresAt().Before(time.Now())
}
