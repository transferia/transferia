package batching_logger

type Throttler interface {
	WillLog()
	IsAllowed() bool
}

// absent

type AbsentThrottler struct{}

func (t *AbsentThrottler) WillLog() {}

func (t *AbsentThrottler) IsAllowed() bool {
	return true
}

func NewAbsentThrottler() *AbsentThrottler {
	return &AbsentThrottler{}
}

// once

type OnceThrottler struct {
	isAlreadyLogged bool
}

func (t *OnceThrottler) WillLog() {
	t.isAlreadyLogged = true
}

func (t *OnceThrottler) IsAllowed() bool {
	return !t.isAlreadyLogged
}

func NewOnceThrottler() *OnceThrottler {
	return &OnceThrottler{
		isAlreadyLogged: false,
	}
}

// counter

type CountThrottler struct {
	limit int
	count int
}

func (t *CountThrottler) WillLog() {
	t.count++
}

func (t *CountThrottler) IsAllowed() bool {
	return t.count < t.limit
}

func NewCountThrottler(limit int) *CountThrottler {
	return &CountThrottler{
		limit: limit,
		count: 0,
	}
}
