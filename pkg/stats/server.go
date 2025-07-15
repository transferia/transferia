package stats

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/transferia/transferia/library/go/core/metrics"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

type ServerMethodStat struct {
	methods  map[string]*MethodStat
	registry metrics.Registry
}

type requestResult string

const (
	requestResultOK          = "ok"
	requestResultClientError = "client_error"
	requestResultServerError = "server_error"
)

var (
	allRequestResults = []requestResult{
		requestResultOK,
		requestResultClientError,
		requestResultServerError,
	}
)

type MethodStat struct {
	counters         map[requestResult]metrics.Counter
	minDuration      atomic.Int64
	maxDuration      atomic.Int64
	minDurationGauge metrics.FuncGauge
	maxDurationGauge metrics.FuncGauge
	duration         metrics.Timer
}

func getRequestResultForCode(grpcCode codes.Code) requestResult {
	switch grpcCode {
	case
		codes.OK:

		return requestResultOK
	case
		codes.Canceled,
		codes.InvalidArgument,
		codes.NotFound,
		codes.AlreadyExists,
		codes.PermissionDenied,
		codes.Unauthenticated,
		codes.ResourceExhausted,
		codes.FailedPrecondition,
		codes.Aborted,
		codes.OutOfRange:

		return requestResultClientError

	case
		codes.Unknown,
		codes.DeadlineExceeded,
		codes.Unimplemented,
		codes.Internal,
		codes.Unavailable,
		codes.DataLoss:

		return requestResultServerError
	}
	return requestResultServerError
}

func (m *MethodStat) Code(code codes.Code, duration time.Duration) {
	result := getRequestResultForCode(code)
	m.counters[result].Inc()

	if result != requestResultOK {
		return
	}

	currentMin := m.minDuration.Load()
	if currentMin == 0 || currentMin > int64(duration) {
		m.minDuration.Store(int64(duration))
	}

	currentMax := m.maxDuration.Load()
	if currentMax < int64(duration) {
		m.maxDuration.Store(int64(duration))
	}

	m.duration.RecordDuration(duration)
}

func methodDurations() []time.Duration {
	return []time.Duration{
		100 * time.Millisecond,
		200 * time.Millisecond,
		500 * time.Millisecond,
		1 * time.Second,
		2 * time.Second,
		4 * time.Second,
		10 * time.Second,
		20 * time.Second,
		30 * time.Second,
		1 * time.Minute,
	}
}

func NewMethodStat(registry metrics.Registry, service string, info grpc.MethodInfo) *MethodStat {
	methodRegistry := registry.WithTags(map[string]string{"grpc_service": service, "method": info.Name})
	counters := map[requestResult]metrics.Counter{}
	for _, result := range allRequestResults {
		requestResultRegistry := methodRegistry.WithTags(map[string]string{"result": string(result)})
		counter := requestResultRegistry.Counter("request.count")
		counters[result] = counter
	}
	methodStat := &MethodStat{
		counters:         counters,
		minDuration:      atomic.Int64{},
		maxDuration:      atomic.Int64{},
		minDurationGauge: nil,
		maxDurationGauge: nil,
		duration:         methodRegistry.DurationHistogram("request.duration", metrics.NewDurationBuckets(methodDurations()...)),
	}
	methodStat.maxDurationGauge = methodRegistry.FuncGauge("request.max_duration", func() float64 {
		return (float64)(methodStat.maxDuration.Swap(0)) / float64(time.Second)
	})
	methodStat.minDurationGauge = methodRegistry.FuncGauge("request.min_duration", func() float64 {
		return (float64)(methodStat.minDuration.Swap(0)) / float64(time.Second)
	})
	return methodStat
}

func NewServerMethods(registry metrics.Registry) *ServerMethodStat {
	return &ServerMethodStat{
		methods:  map[string]*MethodStat{},
		registry: registry,
	}
}

func (s ServerMethodStat) Init(server *grpc.Server) {
	for service, info := range server.GetServiceInfo() {
		for _, mInfo := range info.Methods {
			s.methods[fmt.Sprintf("/%v/%v", service, mInfo.Name)] = NewMethodStat(s.registry, service, mInfo)
		}
	}
}

func (s ServerMethodStat) Method(method string) *MethodStat {
	return s.methods[method]
}
