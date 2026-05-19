// Package clients manages the gRPC client connections to upstream services.
package clients

import (
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	enginepb     "github.com/reconx/proto/gen/go/engine"
	ingestionpb  "github.com/reconx/proto/gen/go/ingestion"
	resolutionpb "github.com/reconx/proto/gen/go/resolution"
)

// Clients holds gRPC client stubs for all upstream services.
type Clients struct {
	Ingestion  ingestionpb.IngestionServiceClient
	Engine     enginepb.ReconciliationEngineClient
	Resolution resolutionpb.ResolutionServiceClient

	// underlying connections — kept so they can be closed on shutdown.
	conns []*grpc.ClientConn
}

// New dials all three upstream gRPC services and returns ready-to-use clients.
// Uses insecure credentials (TLS terminated at load balancer / service mesh).
func New(ingestionAddr, engineAddr, resolutionAddr string) (*Clients, error) {
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
		Ingestion:  ingestionpb.NewIngestionServiceClient(ingConn),
		Engine:     enginepb.NewReconciliationEngineClient(engConn),
		Resolution: resolutionpb.NewResolutionServiceClient(resConn),
		conns:      []*grpc.ClientConn{ingConn, engConn, resConn},
	}, nil
}

// Close tears down all upstream connections.
func (c *Clients) Close() {
	for _, conn := range c.conns {
		conn.Close()
	}
}
