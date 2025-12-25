package proxy

import (
	"context"
	"net"
	"net/http"
)

type Worker struct {
	listener net.Listener // for allocated port
	server   *http.Server // for specified port
}

func (w *Worker) Close() error {
	if w.listener != nil {
		return w.listener.Close()
	}
	if w.server != nil {
		return w.server.Shutdown(context.TODO())
	}
	return nil
}

func WorkerFromListener(listener net.Listener) *Worker {
	return &Worker{
		listener: listener,
		server:   nil,
	}
}
func WorkerFromServer(server *http.Server) *Worker {
	return &Worker{
		listener: nil,
		server:   server,
	}
}
