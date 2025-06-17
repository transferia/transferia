//go:build !disable_elastic_provider

package elastic

import (
	"net/http"
	"time"

	"go.ytsaurus.tech/library/go/core/log"
)

type eslogger struct {
	logger log.Logger
}

func (e eslogger) LogRoundTrip(request *http.Request, response *http.Response, err error, time time.Time, duration time.Duration) error {
	const logMessage = "Elasticsearch request"
	logFn := e.logger.Info
	fields := []log.Field{log.Time("start", time), log.Duration("duration", duration)}
	if request != nil {
		fields = append(fields,
			log.String("method", request.Method),
			log.String("url", request.URL.String()),
		)
	} else {
		logFn = e.logger.Warn
	}
	if response != nil {
		fields = append(fields,
			log.String("status", response.Status),
			log.Int("statusCode", response.StatusCode),
		)
	} else {
		logFn = e.logger.Warn
	}
	if err != nil {
		fields = append(fields, log.Error(err))
		logFn = e.logger.Error
	}
	logFn(logMessage, fields...)
	return nil
}

func (e eslogger) RequestBodyEnabled() bool {
	return false
}

func (e eslogger) ResponseBodyEnabled() bool {
	return false
}
