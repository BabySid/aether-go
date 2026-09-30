// Package worker defines the worker registration and discovery abstraction for aether.
//
// WorkerRegistry is an independent interface that manages worker lifecycle
// (registration, unregistration, discovery) separately from task lifecycle
// (which is managed by broker.TaskBroker).
//
// The separation of concerns:
//   - TaskBroker:     task dispatch, cancellation, fetching, completion
//   - WorkerRegistry: worker identity, capabilities, heartbeat, schema propagation
//
// Implementations must be safe for concurrent use by multiple goroutines.
package worker

import (
	"context"
	"errors"

	"github.com/BabySid/aether-go/wire"
)

// ErrNotFound is returned when a worker is not found in the registry.
var ErrNotFound = errors.New("worker not found")

// Registry manages worker registration and discovery.
//
// In a distributed deployment, workers call Register at startup to announce
// their identity and capabilities (supported executor types + schemas).
// The master (or broker) uses ListByExecutorType to find workers capable of
// handling a given task, enabling type-based routing.
//
// Implementations must be safe for concurrent use by multiple goroutines.
type Registry interface {
	// --- Engine side ---

	// Get returns the info for a specific worker.
	// Returns ErrNotFound if the worker is not registered.
	Get(ctx context.Context, workerID string) (*wire.WorkerInfo, error)

	// List returns all currently registered workers.
	List(ctx context.Context) ([]*wire.WorkerInfo, error)

	// ListByExecutorType returns workers that support the given executor type.
	// Returns an empty slice (not an error) if no workers support the type.
	ListByExecutorType(ctx context.Context, executorType string) ([]*wire.WorkerInfo, error)

	// --- Worker side ---

	// Register registers a worker with its capabilities.
	// If a worker with the same ID already exists, its info is updated (re-registration).
	Register(ctx context.Context, info *wire.WorkerInfo) error

	// Unregister removes a worker from the registry.
	// Returns ErrNotFound if the worker is not registered.
	Unregister(ctx context.Context, workerID string) error

	// Heartbeat reports that a worker is still alive.
	// meta carries optional extension data (e.g. load, running task list, resource usage).
	// Implementations should treat this as idempotent.
	// The registry may use heartbeat absence to detect stale workers and remove them.
	Heartbeat(ctx context.Context, workerID string, meta map[string]any) error
}

// WorkerInfo (the registration/heartbeat payload) is declared in package wire —
// it is part of the engine↔worker wire contract. See wire.WorkerInfo.
