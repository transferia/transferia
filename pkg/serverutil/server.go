package serverutil

import (
	"bytes"
	"net"
	"net/http"
	"net/http/pprof"
	"runtime/debug"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"go.ytsaurus.tech/library/go/core/log"
)

type Server struct {
	listener net.Listener
	logger   log.Logger
}

func NewServer(network, address string, logger log.Logger) (*Server, error) {
	listener, err := net.Listen(network, address)
	if err != nil {
		return nil, xerrors.Errorf("listen %s %s: %w", network, address, err)
	}

	return &Server{
		listener: listener,
		logger:   logger,
	}, nil
}

func (s *Server) Serve() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	mux.HandleFunc("/debug/heapdump", s.heapDumpHandler)

	return http.Serve(s.listener, mux)
}

func (s *Server) Close() error {
	return s.listener.Close()
}

func (s *Server) Addr() net.Addr {
	return s.listener.Addr()
}

func (s *Server) heapDumpHandler(responseWriter http.ResponseWriter, request *http.Request) {
	hijacker, ok := responseWriter.(http.Hijacker)
	if !ok {
		s.logger.Error("not a hijackable connection")
		responseWriter.WriteHeader(505)
		return
	}

	conn, _, err := hijacker.Hijack()
	if err != nil {
		s.logger.Error("hijack failed", log.Error(err))
		responseWriter.WriteHeader(500)
		return
	}
	defer conn.Close()

	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		s.logger.Error("cannot cast connection to TCPConn")
		return
	}

	file, err := tcpConn.File()
	if err != nil {
		s.logger.Error("dup failed", log.Error(err))
		return
	}
	defer file.Close()

	if err := writeHeapDumpHeader(conn, request.Proto); err != nil {
		s.logger.Error("cannot write debug header", log.Error(err))
		return
	}

	debug.WriteHeapDump(file.Fd())
}

var (
	crlf                = []byte("\r\n")
	heapDumpHeaderLines = [][]byte{
		[]byte("Content-Type: application/octet-stream"),
		[]byte("Connection: close"),
		crlf,
	}
)

func writeHeapDumpHeader(conn net.Conn, proto string) (err error) {
	headerLines := [][]byte{[]byte(proto + " 200 OK")}
	headerLines = append(headerLines, heapDumpHeaderLines...)
	for n, headerBuffer := 0, bytes.Join(headerLines, crlf); len(headerBuffer) > 0; headerBuffer = headerBuffer[n:] {
		n, err = conn.Write(headerBuffer)
		if err != nil {
			return err
		}
	}
	return nil
}
