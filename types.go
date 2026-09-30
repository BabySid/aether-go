package aether

import (
	"time"

	"github.com/BabySid/aether-go/model"
)

// WorkflowExecution is the read-only return type of Engine.Get.
// It contains only the fields that external callers need to observe workflow
// state. Internal store fields (Token, Deadline, raw Workflow JSON, UpdatedAt)
// are deliberately excluded to keep the public API stable and decoupled from
// storage internals.
type WorkflowExecution struct {
	RunID     string          `json:"runID"`
	Status    model.Phase     `json:"status"`  // zero value ("") when not yet set
	Message   string          `json:"message"` // human-readable status message
	Outputs   *model.Outputs  `json:"outputs"` // workflow-level outputs, nil until finalized
	Metrics   *model.Metrics  `json:"metrics"` // workflow-level timing metrics
	CreatedAt time.Time       `json:"createdAt"`
	Progress  string          `json:"progress"` // "completed/total", empty when no tasks
	Tasks     []TaskExecution `json:"tasks"`    // all task runs, in creation order
}

// CronWorkflowExecution is the read-only return type of Engine.GetCronWorkflow.
type CronWorkflowExecution struct {
	ID   string              `json:"id"`
	Runs []WorkflowExecution `json:"runs"`
}

// TaskExecution is the read-only view of a single task run within a workflow.
type TaskExecution struct {
	// Immutable
	RunID         string    `json:"runID"`
	WorkflowRunID string    `json:"workflowRunID"`
	ParentRunID   string    `json:"parentRunID"` // "" = top-level scope
	Depth         int       `json:"depth"`
	Scope         string    `json:"scope"`
	TaskName      string    `json:"taskName"`
	TemplateName  string    `json:"templateName"`
	TemplateType  string    `json:"templateType"`
	CreatedAt     time.Time `json:"createdAt"`

	// Mutable (already dereferenced from store pointer types)
	Status     model.Phase    `json:"status"`
	Message    string         `json:"message"`
	Inputs     *model.Inputs  `json:"inputs"`
	Outputs    *model.Outputs `json:"outputs"`
	Metrics    *model.Metrics `json:"metrics"`
	RetryCount int            `json:"retryCount"`
}
