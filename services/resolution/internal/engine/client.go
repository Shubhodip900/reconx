// Package engine provides a gRPC client for the Reconciliation Engine.
//
// The Resolution Service uses this client to:
//   - Query the current reconciliation state for a transaction
//   - Trigger the engine to re-evaluate a MISMATCHED transaction
//     (ReTriggerMatch resets the retry counter and re-runs the matcher)
package engine

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	enginepb "github.com/reconx/proto/gen/go/engine"
)

// Client wraps the engine gRPC connection and provides typed methods.
type Client struct {
	conn           *grpc.ClientConn
	engine         enginepb.ReconciliationEngineClient
	requestTimeout time.Duration
	log            *zap.Logger
}

// NewClient dials the engine at address and returns a ready-to-use Client.
// The connection is kept alive with gRPC keepalives to detect half-open connections.
func NewClient(address string, dialTimeout, requestTimeout time.Duration, log *zap.Logger) (*Client, error) {
	dialCtx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()

	conn, err := grpc.DialContext(
		dialCtx,
		address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                30 * time.Second,
			Timeout:             10 * time.Second,
			PermitWithoutStream: true,
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("dial engine at %s: %w", address, err)
	}

	log.Info("connected to reconciliation engine", zap.String("address", address))

	return &Client{
		conn:           conn,
		engine:         enginepb.NewReconciliationEngineClient(conn),
		requestTimeout: requestTimeout,
		log:            log,
	}, nil
}

// GetReconState queries the engine for the current reconciliation state of a transaction.
func (c *Client) GetReconState(ctx context.Context, transactionRef string) (*enginepb.StateResponse, error) {
	rCtx, cancel := context.WithTimeout(ctx, c.requestTimeout)
	defer cancel()

	resp, err := c.engine.GetReconState(rCtx, &enginepb.StateRequest{
		TransactionRef: transactionRef,
	})
	if err != nil {
		return nil, fmt.Errorf("GetReconState(%q): %w", transactionRef, err)
	}
	return resp, nil
}

// ReTriggerMatch instructs the engine to reset and re-evaluate a transaction.
//
// This is the primary mechanism used by the retry worker: after calling
// ReTriggerMatch, the engine runs the matcher synchronously and returns
// the new state. If the state is now MATCHED, the retry worker marks
// the transaction as resolved.
func (c *Client) ReTriggerMatch(ctx context.Context, transactionRef string) (*enginepb.StateResponse, error) {
	rCtx, cancel := context.WithTimeout(ctx, c.requestTimeout)
	defer cancel()

	resp, err := c.engine.ReTriggerMatch(rCtx, &enginepb.StateRequest{
		TransactionRef: transactionRef,
	})
	if err != nil {
		return nil, fmt.Errorf("ReTriggerMatch(%q): %w", transactionRef, err)
	}
	return resp, nil
}

// IsReady returns true if the underlying gRPC connection is in READY or IDLE state.
func (c *Client) IsReady() bool {
	state := c.conn.GetState()
	return state == connectivity.Ready || state == connectivity.Idle
}

// Close closes the underlying gRPC connection. Should be called on shutdown.
func (c *Client) Close() error {
	return c.conn.Close()
}
