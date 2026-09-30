// Package broker defines the task lifecycle management abstraction for aether.
//
// TaskBroker is the single bridge between Engine (master) and Worker.
// It unifies task dispatch, cancellation, fetching, and completion
// into one interface. Implementations decide how tasks are distributed:
//   - local: goroutine + in-process executor
//   - distributed: message queue, Redis, HTTP, gRPC, etc.
//
// Implementations must be safe for concurrent use by multiple goroutines.
package broker

import (
	"context"

	"github.com/BabySid/aether-go/wire"
)

// TaskBroker manages the full lifecycle of task distribution between
// the Engine (master side) and Workers (execution side).
type TaskBroker interface {
	// --- Engine side ---

	// Dispatch submits a task for execution.
	// How and where the task runs is determined by the implementation:
	//   - local: starts a goroutine and executes in-process
	//   - distributed: serializes the assignment and enqueues to MQ / Redis / HTTP
	Dispatch(ctx context.Context, assignment *wire.TaskAssignment) error

	// Cancel sends a cancellation signal to a running task.
	// Implementation decides how to propagate (context cancel / remote signal).
	Cancel(ctx context.Context, taskRunID string) error

	// --- Worker side ---

	// FetchTask pulls a pending task for execution (blocking / long-poll).
	// workerID identifies the caller for affinity / logging.
	// Returns (nil, context.DeadlineExceeded) or (nil, context.Canceled)
	// when the context expires before a task becomes available.
	FetchTask(ctx context.Context, workerID string) (*wire.TaskAssignment, error)

	// StartTask reports that a worker has begun executing a task.
	// Must be called before any actual computation starts so that the engine
	// can transition the task (and its ancestor containers) from Pending to Running.
	// The implementation decides how to deliver this event to the engine:
	//   - local: directly invokes the StartHandler
	//   - distributed: publishes to MQ, the consumer calls engine.OnTaskStarted
	StartTask(ctx context.Context, taskRunID string, workerID string) error

	// CompleteTask reports the final execution result of a task.
	// Called by the worker after task execution finishes.
	// The implementation decides how to deliver this result to the engine:
	//   - local: directly invokes the CompletionHandler
	//   - distributed: serializes the result and publishes to MQ; the consumer
	//     deserializes and calls engine.OnTaskCompleted
	CompleteTask(ctx context.Context, result *wire.TaskResult) error

	// --- Lifecycle ---

	// Close releases broker resources and waits for in-flight tasks to drain.
	Close() error
}

// StartHandler is the callback invoked when a task begins execution.
// Engine's OnTaskStarted method satisfies this signature.
//
// This type is NOT part of the TaskBroker interface contract.
// It is a convenience type used by implementations (e.g., local broker)
// that need a direct callback mechanism.
type StartHandler func(ctx context.Context, taskRunID string)

// CompletionHandler is the callback invoked when a task finishes execution.
// Engine's OnTaskCompleted method satisfies this signature.
//
// This type is NOT part of the TaskBroker interface contract.
// It is a convenience type used by implementations (e.g., local broker)
// that need a direct callback mechanism.
type CompletionHandler func(ctx context.Context, result *wire.TaskResult)

// TaskAssignment and TaskResult are declared in package wire — they are the
// engine↔worker wire contract, kept out of this behaviour-only port. See
// wire.TaskAssignment and wire.TaskResult.
