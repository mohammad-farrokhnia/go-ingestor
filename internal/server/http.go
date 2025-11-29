package server

import (
	"fmt"
	"log"
	"net"
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type HttpServer struct {
	listener net.Listener
	addr     string
}

func NewHttpServer(port int) (*HttpServer, error) {
	addr := fmt.Sprintf(":%d", port)

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("cannot bind to %s: %w", addr, err)
	}

	return &HttpServer{
		listener: lis,
		addr:     addr,
	}, nil
}

func (s *HttpServer) Start() {
	http.Handle("/metrics", promhttp.Handler())

	go func() {
		log.Printf("HTTP server listening on 127.0.0.1:%s", s.addr)
		if err := http.Serve(s.listener, nil); err != nil {
			log.Printf("HTTP server error: %v", err)
		}
	}()
}

func (s *HttpServer) Addr() string {
	return s.addr
}
