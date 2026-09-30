package internal

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/BabySid/aether-go/errsink"
	"github.com/BabySid/aether-go/expr"
	ivars "github.com/BabySid/aether-go/internal/vars"
	"github.com/BabySid/aether-go/model"
	"github.com/BabySid/aether-go/store"
	"github.com/BabySid/aether-go/wire"
)

// CodeToPhase maps an ExecOutputs.Code to its canonical Phase.
// This is the single authoritative Code→Phase mapping in the engine layer.
//
//	ExecCodeSucceeded → PhaseSucceeded
//	ExecCodeSuspended → PhaseSuspended (await pattern)
//	ExecCodeFailed    → PhaseFailed    (business failure)
//	ExecCodeError     → PhaseError     (system/framework-level error)
//	ExecCodeTimeout   → PhaseTimeout   (task exceeded its deadline)
//
// Any unrecognised code is treated as PhaseError (fail-safe).
func CodeToPhase(code int) model.Phase {
	switch code {
	case model.ExecCodeSucceeded:
		return model.PhaseSucceeded
	case model.ExecCodeSuspended:
		return model.PhaseSuspended
	case model.ExecCodeFailed:
		return model.PhaseFailed
	case model.ExecCodeError:
		return model.PhaseError
	case model.ExecCodeTimeout:
		return model.PhaseTimeout
	default:
		return model.PhaseError
	}
}

// EvalErrorContext carries the ErrorSink and identifiers needed to report
// expression evaluation failures. This avoids bloating function signatures
// with individual sink/workflowRunID/taskRunID parameters.
//
// Operation names the entry point that produced the error so the two phase
// contracts (leaf and container) are distinguishable in ErrorSink reports.
// When empty, it defaults to the leaf operation name.
type EvalErrorContext struct {
	Sink          errsink.ErrorSink
	WorkflowRunID string
	TaskRunID     string
	Operation     string
}

// EvalPhaseConditions evaluates phaseConditions for a leaf task to determine its
// final phase. If phaseConditions is nil, the phase derived from result.ExecOutputs.Code
// is returned as-is.
//
// # Leaf environment
//
// The leaf contract exposes the task's own result:
//
//   - phase — the phase derived from Code, before the override
//   - code  — the executor's raw ExecCode
//   - msg   — the executor's message
//   - outputs.parameters.<p> — the task's own output parameters
//
// PhaseConditions allows users to override the task phase based on custom expressions.
// For example, a task that "fails" at the executor level might be considered "succeeded"
// based on output analysis.
//
// Priority is succeeded → failed → error, first match wins. No match falls back to the
// code-derived phase. An evaluation error is reported to the ErrorSink and treated as no match.
func EvalPhaseConditions(
	ctx context.Context,
	conditions *model.PhaseConditions,
	eval expr.Evaluator,
	result *wire.TaskResult,
	errCtx *EvalErrorContext,
) model.Phase {
	// Derive the base phase from Code (single source of truth).
	var code int
	var msg string
	if result.ExecOutputs != nil {
		code = result.ExecOutputs.Code
		msg = result.ExecOutputs.Message
	}
	basePhase := CodeToPhase(code)

	if conditions == nil || eval == nil {
		return basePhase
	}

	// Build evaluation environment from result.
	env := map[string]any{
		"phase": string(basePhase),
		"code":  code,
		"msg":   msg,
	}
	if result.ExecOutputs != nil {
		addOutputParamValues(env, result.ExecOutputs.Parameters)
	}

	return applyPhaseConditions(ctx, conditions, eval, basePhase, env, errCtx)
}

// EvalContainerPhaseConditions evaluates phaseConditions at a container boundary
// (DAG today; Loop can adopt the same entry point later). It takes the aggregated
// base phase and an already-built environment (see BuildContainerPhaseEnv) and
// returns the container's final phase, which may differ from the aggregate in any
// direction — including Succeeded → Failed.
//
// Cancellation is never overridable: if aggregation produced PhaseCancelled the
// conditions are not evaluated and the phase stands. This mirrors continueOn, which
// also refuses to tolerate a cancellation. Enforcing it here means every caller
// inherits the invariant.
//
// Priority and failure semantics are identical to the leaf contract:
// succeeded → failed → error, first match wins; no match falls back to the
// aggregated phase; an evaluation error is reported to the ErrorSink and treated
// as no match.
func EvalContainerPhaseConditions(
	ctx context.Context,
	conditions *model.PhaseConditions,
	eval expr.Evaluator,
	basePhase model.Phase,
	env map[string]any,
	errCtx *EvalErrorContext,
) model.Phase {
	// A cancelled scope can never be laundered into another phase.
	if basePhase == model.PhaseCancelled {
		return basePhase
	}
	if conditions == nil || eval == nil {
		return basePhase
	}
	if env == nil {
		env = map[string]any{}
	}
	return applyPhaseConditions(ctx, conditions, eval, basePhase, env, errCtx)
}

// BuildContainerPhaseEnv builds the expression environment for container-level
// phaseConditions from sibling TaskRuns and the container's collected outputs.
//
// The container contract is a sibling of the leaf contract, not a superset of the
// binding environment. It contains exactly:
//
//   - phase — the aggregated phase, before the override
//   - msg   — the aggregated message (unstable contract)
//   - tasks.<child>.phase — always present for every child
//   - tasks.<child>.code / .msg / .outputs.parameters.<p> — present only when the
//     child produced outputs
//   - outputs.parameters.<p> — the container's collected declared outputs
//
// Deliberately absent: code, workflow.parameters.*, inputs.parameters.* and
// loop_iter.*. Build this environment separately from the output-collection
// environment, which includes workflow arguments.
func BuildContainerPhaseEnv(basePhase model.Phase, msg string, siblings []*store.TaskRun, outputs *model.Outputs) map[string]any {
	env := make(map[string]any)
	for k, v := range (&ivars.SiblingTaskRunsSource{Runs: siblings}).Vars() {
		env[k] = v
	}
	env["phase"] = string(basePhase)
	env["msg"] = msg
	if outputs != nil {
		addOutputParamValues(env, outputs.Parameters)
	}
	return env
}

// addOutputParamValues writes each output parameter into env under the
// "outputs.parameters.<name>" key, decoding the raw JSON value to its native Go
// type. Shared by the leaf and container environment builders.
func addOutputParamValues(env map[string]any, params []model.Parameter) {
	for _, p := range params {
		env["outputs.parameters."+p.Name] = unmarshalParam(p.Value)
	}
}

// applyPhaseConditions is the shared core for both the leaf and container entry
// points: it carries the priority chain, truthiness handling, error degradation
// and ErrorSink reporting. Keeping one implementation guarantees the two
// contracts cannot drift apart.
func applyPhaseConditions(
	ctx context.Context,
	conditions *model.PhaseConditions,
	eval expr.Evaluator,
	basePhase model.Phase,
	env map[string]any,
	errCtx *EvalErrorContext,
) model.Phase {
	// Evaluate conditions in priority order: succeeded > failed > error
	if conditions.Succeeded != "" {
		if evalBool(ctx, eval, conditions.Succeeded, env, errCtx) {
			return model.PhaseSucceeded
		}
	}
	if conditions.Failed != "" {
		if evalBool(ctx, eval, conditions.Failed, env, errCtx) {
			return model.PhaseFailed
		}
	}
	if conditions.Error != "" {
		if evalBool(ctx, eval, conditions.Error, env, errCtx) {
			return model.PhaseError
		}
	}

	// No condition matched — return the base phase.
	return basePhase
}

// unmarshalParam decodes a json.RawMessage into its native Go type
// (float64 for numbers, string for strings, bool for booleans, etc.)
// so that expression evaluation operates on real types instead of raw strings.
func unmarshalParam(raw json.RawMessage) any {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return string(raw)
	}
	return v
}

// evalBool evaluates an expression and returns true if the result is truthy.
// Evaluation errors are reported to the ErrorSink (if provided) and treated as false.
func evalBool(ctx context.Context, eval expr.Evaluator, expression string, env map[string]any, errCtx *EvalErrorContext) bool {
	result, err := eval.Eval(ctx, expression, env)
	if err != nil {
		if errCtx != nil && errCtx.Sink != nil {
			operation := errCtx.Operation
			if operation == "" {
				operation = "evalPhaseConditions"
			}
			errCtx.Sink.OnError(ctx, err, errsink.ErrorContext{
				WorkflowRunID: errCtx.WorkflowRunID,
				TaskRunID:     errCtx.TaskRunID,
				Operation:     operation,
				Severity:      errsink.SeverityWarning,
			})
		}
		return false
	}
	switch v := result.(type) {
	case bool:
		return v
	case string:
		return v == "true"
	default:
		return fmt.Sprintf("%v", v) == "true"
	}
}
