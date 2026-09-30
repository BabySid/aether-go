// Package wire defines the message types brokers and workers exchange.
package wire

import (
	"time"

	"github.com/BabySid/aether-go/model"
)

// TaskAssignment contains all information needed to execute a task.
// "Fat assignment": workers do not need to query the Store.
//
// Inputs and Resources use strong types: they follow a fixed schema known to the
// framework, and using *model.Inputs / *model.Resources eliminates manual
// marshal/unmarshal in every broker implementation.
type TaskAssignment struct {
	TaskRunID     string           `json:"taskRunID"`
	WorkflowRunID string           `json:"workflowRunID"`
	TaskName      string           `json:"taskName"`
	TemplateName  string           `json:"templateName"`
	ExecutorType  string           `json:"executorType"` // executor type identifier, e.g. "echo", "http", "shell"
	Inputs        *model.Inputs    `json:"inputs,omitempty"`
	Timeout       string           `json:"timeout,omitempty"` // e.g. "30m"
	Resources     *model.Resources `json:"resources,omitempty"`
	Priority      int              `json:"priority,omitempty"`
	RetryCount    int              `json:"retryCount,omitempty"` // number of retries already consumed (0 = first attempt)
}

// TaskResult holds the result of a completed task execution.
// It is the message a worker sends to the engine when a task finishes.
//
// Design principles:
//   - WorkflowRunID mirrors TaskAssignment so the engine can locate the scope
//     without an extra store lookup.
//   - ExecOutputs.Code carries the execution outcome; the engine maps it to
//     Phase (single Phase writer). ExecOutputs fields are JSON-flat (no wrapper
//     key) so a result reads as {"taskRunID":..., "code":..., "message":...}.
//     Code is omitempty and ExecCodeSucceeded == 0, so a successful result omits
//     "code" entirely; decoders MUST treat an absent code as 0 (success). More
//     generally, an all-zero ExecOutputs serializes to "{}" and decodes back as
//     a nil embedded pointer — the engine treats nil as Code 0 (success).
//   - Phase and Metrics are NOT included: Phase is derived by the engine from
//     Code; Metrics (StartedAt/FinishedAt/Retries) are recorded by the engine
//     in OnTaskStarted / OnTaskCompleted.
type TaskResult struct {
	TaskRunID     string `json:"taskRunID"`
	WorkflowRunID string `json:"workflowRunID"` // mirrors TaskAssignment.WorkflowRunID; avoids extra store lookup
	*model.ExecOutputs
}

// WorkerInfo describes a registered worker's identity and capabilities.
//
// When a worker registers, it declares which executor types it can handle
// and provides the full ExecutorSchema for each type. This allows the master
// to populate its schema registry without a separate round-trip, enabling
// schema-aware validation even when executor plugins run on remote workers.
type WorkerInfo struct {
	ID            string                 `json:"id"`                      // unique worker instance id
	ExecutorTypes []string               `json:"executorTypes,omitempty"` // executor types this worker can handle
	Schemas       []model.ExecutorSchema `json:"schemas,omitempty"`       // full schema for each executor type
	Tags          map[string]string      `json:"tags,omitempty"`          // optional metadata labels (reserved for future routing)
	RegisteredAt  time.Time              `json:"registeredAt,omitempty"`  // when the worker first registered (set by the registry)
}
