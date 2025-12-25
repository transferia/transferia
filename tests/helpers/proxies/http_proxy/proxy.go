package proxy

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"path"
	"sync"
)

type HTTPProxy struct {
	ListenPort int
	targetAddr string
	pathToPem  string
	pathToKey  string
	isHTTPS    bool
	WithLogger bool

	Err         error
	mutex       sync.Mutex
	sniffedData []RequestResponse
}

func (p *HTTPProxy) GetSniffedData() []RequestResponse {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	return p.sniffedData
}

func (p *HTTPProxy) ResetSniffedData() {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.sniffedData = make([]RequestResponse, 0)
}

func (p *HTTPProxy) handleHTTP(w http.ResponseWriter, req *http.Request) {
	client := &http.Client{}

	// save 'req'
	reqBody, err := io.ReadAll(req.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}

	// build 'reqNew'
	targetURL := makePrefix(p.isHTTPS) + path.Join(p.targetAddr, req.URL.String())
	reqNew, err := http.NewRequest(req.Method, targetURL, bytes.NewBuffer(reqBody))
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	for k, v := range req.Header {
		for _, vv := range v {
			reqNew.Header.Add(k, vv)
		}
	}

	// make request
	resp, err := client.Do(reqNew)
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}

	// save 'resp'
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}

	// save results
	p.mutex.Lock()
	requestResponse := NewRequestResponse(
		req.Method,
		req.URL.String(),
		req.Header,
		reqBody,
		resp.Header,
		respBody,
	)
	p.sniffedData = append(p.sniffedData, *requestResponse)
	if p.WithLogger {
		requestResponse.Print()
	}
	p.mutex.Unlock()

	// handle result
	copyHeader(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	_, _ = w.Write(respBody)
}

func (p *HTTPProxy) RunAsync() *Worker {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodConnect {
			handleTunneling(w, r)
		} else {
			p.handleHTTP(w, r)
		}
	})

	if p.ListenPort == 0 {
		// need to allocated port
		listener, err := net.Listen("tcp", ":0")
		if err != nil {
			panic(err)
		}
		p.ListenPort = listener.Addr().(*net.TCPAddr).Port
		go func() {
			if p.isHTTPS {
				p.Err = http.ServeTLS(listener, handler, p.pathToPem, p.pathToKey)
			} else {
				p.Err = http.Serve(listener, handler)
			}
		}()
		return WorkerFromListener(listener)
	} else {
		// need to be upraised on specific port
		server := &http.Server{
			Addr:    fmt.Sprintf("localhost:%d", p.ListenPort),
			Handler: handler,
			// Disable HTTP/2.
			TLSNextProto: make(map[string]func(*http.Server, *tls.Conn, http.Handler)),
		}
		go func() {
			if p.isHTTPS {
				p.Err = server.ListenAndServeTLS(p.pathToPem, p.pathToKey)
			} else {
				p.Err = server.ListenAndServe()
			}
		}()
		return WorkerFromServer(server)
	}
}

func newHTTProxyImpl(listenPort int, targetAddr, pathToPem, pathToKey string, isHTTPS bool) *HTTPProxy {
	return &HTTPProxy{
		ListenPort:  listenPort,
		targetAddr:  targetAddr,
		pathToPem:   pathToPem,
		pathToKey:   pathToKey,
		isHTTPS:     isHTTPS,
		WithLogger:  false,
		Err:         nil,
		mutex:       sync.Mutex{},
		sniffedData: []RequestResponse{},
	}
}

func NewHTTPProxy(listenPort int, targetAddr string) *HTTPProxy {
	return newHTTProxyImpl(listenPort, targetAddr, "", "", false)
}

func NewHTTPProxyWithPortAllocation(targetAddr string) *HTTPProxy {
	return newHTTProxyImpl(0, targetAddr, "", "", false)
}

func NewHTTPSProxy(listenPort int, targetAddr, pathToPem, pathToKey string) *HTTPProxy {
	return newHTTProxyImpl(listenPort, targetAddr, pathToPem, pathToKey, true)
}

func NewHTTPSProxyWithPortAllocation(targetAddr, pathToPem, pathToKey string) *HTTPProxy {
	return newHTTProxyImpl(0, targetAddr, pathToPem, pathToKey, true)
}
