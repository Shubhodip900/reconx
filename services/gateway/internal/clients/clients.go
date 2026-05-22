// Package clients manages the gRPC and HTTP client connections to upstream services.
package clients

import (
	"fmt"
	"net/http"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	enginepb     "github.com/reconx/proto/gen/go/engine"
	ingestionpb  "github.com/reconx/proto/gen/go/ingestion"
	resolutionpb "github.com/reconx/proto/gen/go/resolution"
)

// Clients holds gRPC client stubs for all upstream services plus an HTTP
// client for the Resolution Service's REST API (used to proxy auto-resolve,
// retry, and audit routes that are not available over gRPC).
type Clients struct {
	Ingestion  ingestionpb.IngestionServiceClient
	Engine     enginepb.ReconciliationEngineClient
	Resolution resolutionpb.ResolutionServiceClient

	// ResolutionHTTP is the HTTP client for the Resolution Service REST API.
	// Base URL is set from config (e.g. http://resolution:8082).
	ResolutionHTTP *ResolutionHTTPClient

	// underlying connections — kept so they can be closed on shutdown.
	conns []*grpc.ClientConn
}

// New dials all three upstream gRPC services and returns ready-to-use clients.
// resolutionHTTPAddr is the base URL (scheme + host + port, no trailing slash)
// for the Resolution Service HTTP REST API, e.g. "http://resolution:8082".
//
// Uses insecure credentials for gRPC (TLS terminated at load balancer / service mesh).
func New(ingestionAddr, engineAddr, resolutionAddr, resolutionHTTPAddr string) (*Clients, error) {
	dial := func(addr string) (*grpc.ClientConn, error) {
		conn, err := grpc.NewClient(addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			return nil, fmt.Errorf("dial %s: %w", addr, err)
		}
		return conn, nil
	}

	ingConn, err := dial(ingestionAddr)
	if err != nil {
		return nil, err
	}
	engConn, err := dial(engineAddr)
	if err != nil {
		ingConn.Close()
		return nil, err
	}
	resConn, err := dial(resolutionAddr)
	if err != nil {
		ingConn.Close()
		engConn.Close()
		return nil, err
	}

	return &Clients{
		Ingestion:      ingestionpb.NewIngestionServiceClient(ingConn),
		Engine:         enginepb.NewReconciliationEngineClient(engConn),
		Resolution:     resolutionpb.NewResolutionServiceClient(resConn),
		ResolutionHTTP: newResolutionHTTPClient(resolutionHTTPAddr),
		conns:          []*grpc.ClientConn{ingConn, engConn, resConn},
	}, nil
}

// Close tears down all upstream connections.
func (c *Clients) Close() {
	for _, conn := range c.conns {
		conn.Close()
	}
}

// ── Resolution HTTP client ─────────────────────────────────────────────────────

// ResolutionHTTPClient is a thin HTTP client for the Resolution Service REST
// API. It is used to proxy the four routes that are only available over HTTP
// (auto-resolve, retry, audit, retry-queue).
type ResolutionHTTPClient struct {
	baseURL string
	http    *http.Client
}

func newResolutionHTTPClient(baseURL string) *ResolutionHTTPClient {
	return &ResolutionHTTPClient{
		baseURL: baseURL,
		http: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// BaseURL returns the configured base URL (e.g. "http://resolution:8082").
func (c *ResolutionHTTPClient) BaseURL() string { return c.baseURL }

// Do executes an HTTP request against the Resolution Service.
func (c *ResolutionHTTPClient) Do(req *http.Request) (*http.Response, error) {
	return c.http.Do(req)
}
