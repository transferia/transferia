package proxy

import (
	"io"
	"net"
	"strings"

	"github.com/jackc/pgproto3/v2"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
)

// struct for testing
// you can add message handlers to the proxy
// and they will be called for each message
// if the handler returns true, the message will not be relayed to the real server
// if the handler returns false, the message will be relayed to the real server

type MessageHandler func(msg pgproto3.FrontendMessage, backend *pgproto3.Backend, frontend *pgproto3.Frontend) bool

type Proxy struct {
	listenAddr string
	mainAddr   string

	messageHandlers []MessageHandler
	listener        net.Listener
	done            chan struct{}
}

func NewProxy(listenAddr, mainAddr string, messageHandlers ...MessageHandler) *Proxy {
	return &Proxy{
		listenAddr:      listenAddr,
		mainAddr:        mainAddr,
		messageHandlers: messageHandlers,
		done:            make(chan struct{}),
	}
}

func (p *Proxy) AddMessageHandler(handler MessageHandler) {
	p.messageHandlers = append(p.messageHandlers, handler)
}

func (p *Proxy) Start() {
	logger.Log.Infof("Starting proxy for PostgreSQL on %s", p.listenAddr)
	ln, err := net.Listen("tcp", p.listenAddr)
	if err != nil {
		logger.Log.Fatalf("Error starting proxy: %v", err)
	}

	p.listener = ln
	logger.Log.Infof("Proxy for PostgreSQL started on %s", ln.Addr())

	go func() {
		for {
			select {
			case <-p.done:
				return
			default:
			}
			clientConn, err := ln.Accept()
			if err != nil {
				logger.Log.Errorf("Unable to accept connection: %v", err)
				continue
			}
			go p.handleConnection(clientConn)
		}
	}()
}

func (p *Proxy) Close() {
	close(p.done)
	if p.listener != nil {
		if err := p.listener.Close(); err != nil {
			logger.Log.Errorf("Unable to close listener: %v", err)
		}
		logger.Log.Infof("Proxy for PostgreSQL closed")
	}
}

func (p *Proxy) handleConnection(clientConn net.Conn) {
	defer clientConn.Close()
	logger.Log.Infof("New connection from client: %v", clientConn.RemoteAddr())

	serverConn, err := net.Dial("tcp", p.mainAddr)
	if err != nil {
		logger.Log.Errorf("Unable to connect to PostgreSQL server: %v", err)
		return
	}
	defer serverConn.Close()
	logger.Log.Infof("Connection to PostgreSQL server established: %v", serverConn.RemoteAddr())

	backend := pgproto3.NewBackend(pgproto3.NewChunkReader(clientConn), clientConn)
	frontend := pgproto3.NewFrontend(pgproto3.NewChunkReader(serverConn), serverConn)

	startupMessage, err := backend.ReceiveStartupMessage()
	if err != nil {
		logger.Log.Errorf("Unable to receive startup message from client: %v", err)
		return
	}
	if err = frontend.Send(startupMessage); err != nil {
		logger.Log.Errorf("Unable to send startup message to server: %v", err)
		return
	}
	if err := relayUntilReady(frontend, backend, "startup"); err != nil {
		logger.Log.Errorf("Unable to relay messages during startup: %v", err)
		return
	}
	logger.Log.Infof("Authentication passed, session is ready for queries.")

	for {
		msg, err := backend.Receive()
		if err != nil {
			if err == io.EOF {
				logger.Log.Infof("Client closed connection.")
			}
			return
		}

		messageHandled := false
		for _, handler := range p.messageHandlers {
			if handler(msg, backend, frontend) {
				messageHandled = true
				break
			}
		}
		if messageHandled {
			continue
		}

		switch m := msg.(type) {
		case *pgproto3.Query:
			logger.Log.Infof("Relaying query to real server.")
			if err := frontend.Send(m); err != nil {
				logger.Log.Errorf("Unable to send query to server: %v", err)
				return
			}
			if err := relayUntilReady(frontend, backend, m.String); err != nil {
				logger.Log.Errorf("Unable to relay response from server: %v", err)
				return
			}

		case *pgproto3.Terminate:
			logger.Log.Infof("Received termination message. Closing connection.")
			return
		default:
			logger.Log.Infof("Relaying message of type %T to real server", m)
			if err := frontend.Send(m); err != nil {
				logger.Log.Errorf("Unable to send message to server: %v", err)
				return
			}
		}
	}
}

// samle to create error handler
func (p *Proxy) AddErrorHandler(query string, pgErr *pgproto3.ErrorResponse) {
	if pgErr == nil {
		pgErr = &pgproto3.ErrorResponse{
			Severity: "ERROR",
			Code:     "42601",
			Message:  "This query was blocked by the proxy server.",
		}
	}
	p.AddMessageHandler(func(msg pgproto3.FrontendMessage, backend *pgproto3.Backend, frontend *pgproto3.Frontend) bool {
		switch m := msg.(type) {
		case *pgproto3.Query:
			receivedQuery := strings.TrimSpace(strings.ToLower(m.String))
			if strings.Contains(receivedQuery, query) {
				_ = backend.Send(pgErr)

				readyForQuery := &pgproto3.ReadyForQuery{
					TxStatus: 'I',
				}
				_ = backend.Send(readyForQuery)

				return true
			}
		}
		return false
	})
}

func (p *Proxy) AddCounterHandler(query string, counter *int) {
	p.AddMessageHandler(func(msg pgproto3.FrontendMessage, backend *pgproto3.Backend, frontend *pgproto3.Frontend) bool {
		switch m := msg.(type) {
		case *pgproto3.Query:
			if strings.Contains(m.String, query) {
				*counter++
			}
		}
		return false
	})
}

func relayUntilReady(frontend *pgproto3.Frontend, backend *pgproto3.Backend, context string) error {
	for {
		msg, err := frontend.Receive()
		if err != nil {
			return xerrors.Errorf("unable to receive message from frontend: %w", err)
		}

		if err := backend.Send(msg); err != nil {
			return xerrors.Errorf("unable to send message to server: %w", err)
		}

		if _, ok := msg.(*pgproto3.ReadyForQuery); ok {
			logger.Log.Infof("Received ReadyForQuery from server (context: %s). Relay loop completed.", context)
			return nil
		}
	}
}
