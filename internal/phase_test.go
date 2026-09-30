package internal

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/BabySid/aether-go/errsink"
	"github.com/BabySid/aether-go/model"
	"github.com/BabySid/aether-go/store"
	"github.com/BabySid/aether-go/wire"
)

// mockEval is a configurable mock Evaluator.
type mockEval struct {
	fn func(expr string, env map[string]any) (any, error)
}

func (e *mockEval) Eval(_ context.Context, expr string, env map[string]any) (any, error) {
	return e.fn(expr, env)
}

// ---- CodeToPhase ----

func TestCodeToPhase(t *testing.T) {
	tests := []struct {
		code int
		want model.Phase
	}{
		{model.ExecCodeSucceeded, model.PhaseSucceeded},
		{model.ExecCodeSuspended, model.PhaseSuspended},
		{model.ExecCodeFailed, model.PhaseFailed},
		{model.ExecCodeError, model.PhaseError},
		{model.ExecCodeTimeout, model.PhaseTimeout},
		{999, model.PhaseError}, // unknown code → PhaseError
		{-1, model.PhaseError},  // negative code → PhaseError
	}
	for _, tt := range tests {
		got := CodeToPhase(tt.code)
		if got != tt.want {
			t.Errorf("CodeToPhase(%d) = %q, want %q", tt.code, got, tt.want)
		}
	}
}

// ---- EvalPhaseConditions ----

func TestEvalPhaseConditions_NilConditions(t *testing.T) {
	result := &wire.TaskResult{
		ExecOutputs: &model.ExecOutputs{Code: model.ExecCodeFailed, Message: "boom"},
	}
	phase := EvalPhaseConditions(context.Background(), nil, &mockEval{fn: func(string, map[string]any) (any, error) {
		t.Fatal("eval should not be called when conditions=nil")
		return nil, nil
	}}, result, nil)
	if phase != model.PhaseFailed {
		t.Errorf("expected PhaseFailed, got %q", phase)
	}
}

func TestEvalPhaseConditions_NilEvaluator(t *testing.T) {
	result := &wire.TaskResult{
		ExecOutputs: &model.ExecOutputs{Code: model.ExecCodeSucceeded},
	}
	conditions := &model.PhaseConditions{Failed: "true"}
	phase := EvalPhaseConditions(context.Background(), conditions, nil, result, nil)
	if phase != model.PhaseSucceeded {
		t.Errorf("expected PhaseSucceeded (base phase), got %q", phase)
	}
}

func TestEvalPhaseConditions_NilExecOutputs(t *testing.T) {
	result := &wire.TaskResult{ExecOutputs: nil}
	phase := EvalPhaseConditions(context.Background(), nil, nil, result, nil)
	// code=0 → PhaseSucceeded
	if phase != model.PhaseSucceeded {
		t.Errorf("expected PhaseSucceeded for nil ExecOutputs, got %q", phase)
	}
}

func TestEvalPhaseConditions_SucceededOverride(t *testing.T) {
	eval := &mockEval{fn: func(string, map[string]any) (any, error) { return true, nil }}
	result := &wire.TaskResult{
		ExecOutputs: &model.ExecOutputs{Code: model.ExecCodeFailed},
	}
	conditions := &model.PhaseConditions{Succeeded: "always-true"}
	phase := EvalPhaseConditions(context.Background(), conditions, eval, result, nil)
	if phase != model.PhaseSucceeded {
		t.Errorf("expected PhaseSucceeded override, got %q", phase)
	}
}

func TestEvalPhaseConditions_FailedOverride(t *testing.T) {
	eval := &mockEval{fn: func(expr string, _ map[string]any) (any, error) {
		if expr == "fail-cond" {
			return true, nil
		}
		return false, nil
	}}
	result := &wire.TaskResult{
		ExecOutputs: &model.ExecOutputs{Code: model.ExecCodeSucceeded},
	}
	conditions := &model.PhaseConditions{Succeeded: "succ-cond", Failed: "fail-cond"}
	phase := EvalPhaseConditions(context.Background(), conditions, eval, result, nil)
	// Succeeded is checked first but returns false → Failed returns true
	if phase != model.PhaseFailed {
		t.Errorf("expected PhaseFailed override, got %q", phase)
	}
}

func TestEvalPhaseConditions_ErrorOverride(t *testing.T) {
	eval := &mockEval{fn: func(expr string, _ map[string]any) (any, error) {
		if expr == "err-cond" {
			return true, nil
		}
		return false, nil
	}}
	result := &wire.TaskResult{
		ExecOutputs: &model.ExecOutputs{Code: model.ExecCodeSucceeded},
	}
	conditions := &model.PhaseConditions{Error: "err-cond"}
	phase := EvalPhaseConditions(context.Background(), conditions, eval, result, nil)
	if phase != model.PhaseError {
		t.Errorf("expected PhaseError override, got %q", phase)
	}
}

func TestEvalPhaseConditions_NoConditionMatch_FallsBackToBase(t *testing.T) {
	eval := &mockEval{fn: func(string, map[string]any) (any, error) { return false, nil }}
	result := &wire.TaskResult{
		ExecOutputs: &model.ExecOutputs{Code: model.ExecCodeTimeout},
	}
	conditions := &model.PhaseConditions{Succeeded: "x", Failed: "y", Error: "z"}
	phase := EvalPhaseConditions(context.Background(), conditions, eval, result, nil)
	if phase != model.PhaseTimeout {
		t.Errorf("expected PhaseTimeout fallback, got %q", phase)
	}
}

func TestEvalPhaseConditions_EvalError_TreatedAsFalse(t *testing.T) {
	eval := &mockEval{fn: func(string, map[string]any) (any, error) {
		return nil, errors.New("eval failed")
	}}
	result := &wire.TaskResult{
		ExecOutputs: &model.ExecOutputs{Code: model.ExecCodeFailed},
	}
	conditions := &model.PhaseConditions{Succeeded: "broken"}
	phase := EvalPhaseConditions(context.Background(), conditions, eval, result, nil)
	// eval error → condition treated as false → base phase returned
	if phase != model.PhaseFailed {
		t.Errorf("expected PhaseFailed fallback on eval error, got %q", phase)
	}
}

func TestEvalPhaseConditions_SucceededPriority(t *testing.T) {
	// All conditions return true — succeeded has highest priority
	eval := &mockEval{fn: func(string, map[string]any) (any, error) { return true, nil }}
	result := &wire.TaskResult{
		ExecOutputs: &model.ExecOutputs{Code: model.ExecCodeError},
	}
	conditions := &model.PhaseConditions{Succeeded: "a", Failed: "b", Error: "c"}
	phase := EvalPhaseConditions(context.Background(), conditions, eval, result, nil)
	if phase != model.PhaseSucceeded {
		t.Errorf("expected PhaseSucceeded (highest priority), got %q", phase)
	}
}

// ---- EvalPhaseConditions env ----

// captureEval is a mock Evaluator that captures the env passed to it.
type captureEval struct {
	env map[string]any
}

func (e *captureEval) Eval(_ context.Context, _ string, env map[string]any) (any, error) {
	e.env = env
	return true, nil
}

func TestEvalPhaseConditions_EnvContainsBaseFields(t *testing.T) {
	eval := &captureEval{}
	result := &wire.TaskResult{
		ExecOutputs: &model.ExecOutputs{Code: model.ExecCodeFailed, Message: "timeout upstream"},
	}
	conditions := &model.PhaseConditions{Succeeded: "check"}
	EvalPhaseConditions(context.Background(), conditions, eval, result, nil)

	if eval.env["phase"] != string(model.PhaseFailed) {
		t.Errorf("expected phase=%q, got %v", model.PhaseFailed, eval.env["phase"])
	}
	if eval.env["code"] != model.ExecCodeFailed {
		t.Errorf("expected code=%d, got %v", model.ExecCodeFailed, eval.env["code"])
	}
	if eval.env["msg"] != "timeout upstream" {
		t.Errorf("expected msg=%q, got %v", "timeout upstream", eval.env["msg"])
	}
}

func TestEvalPhaseConditions_OutputParamTypes(t *testing.T) {
	eval := &captureEval{}
	result := &wire.TaskResult{
		ExecOutputs: &model.ExecOutputs{
			Code:    0,
			Message: "ok",
			Parameters: []model.Parameter{
				{Name: "count", Value: json.RawMessage(`42`)},
				{Name: "ratio", Value: json.RawMessage(`3.14`)},
				{Name: "msg", Value: json.RawMessage(`"hello"`)},
				{Name: "flag", Value: json.RawMessage(`true`)},
			},
		},
	}
	conditions := &model.PhaseConditions{Succeeded: "true"}

	EvalPhaseConditions(context.Background(), conditions, eval, result, nil)

	tests := []struct {
		key      string
		wantType string
		wantVal  any
	}{
		{"outputs.parameters.count", "float64", float64(42)},
		{"outputs.parameters.ratio", "float64", float64(3.14)},
		{"outputs.parameters.msg", "string", "hello"},
		{"outputs.parameters.flag", "bool", true},
	}
	for _, tt := range tests {
		v, ok := eval.env[tt.key]
		if !ok {
			t.Errorf("env missing key %q", tt.key)
			continue
		}
		if v != tt.wantVal {
			t.Errorf("env[%q] = %v (%T), want %v (%s)", tt.key, v, v, tt.wantVal, tt.wantType)
		}
	}
}

// ---- EvalContainerPhaseConditions ----

// mockSink captures ErrorSink reports for assertions.
type mockSink struct {
	calls []errsink.ErrorContext
	errs  []error
}

func (s *mockSink) OnError(_ context.Context, err error, ec errsink.ErrorContext) {
	s.calls = append(s.calls, ec)
	s.errs = append(s.errs, err)
}

func testPhase(p model.Phase) *model.Phase { return &p }

func TestEvalContainerPhaseConditions_CancelledNeverOverridden(t *testing.T) {
	eval := &mockEval{fn: func(string, map[string]any) (any, error) {
		t.Fatal("eval must not be called for a cancelled aggregate")
		return true, nil
	}}
	conditions := &model.PhaseConditions{Succeeded: "true", Failed: "true", Error: "true"}
	phase := EvalContainerPhaseConditions(context.Background(), conditions, eval, model.PhaseCancelled, map[string]any{}, nil)
	if phase != model.PhaseCancelled {
		t.Errorf("expected cancellation to stand, got %q", phase)
	}
}

func TestEvalContainerPhaseConditions_SucceededOverride(t *testing.T) {
	eval := &mockEval{fn: func(string, map[string]any) (any, error) { return true, nil }}
	conditions := &model.PhaseConditions{Succeeded: "always-true"}
	for _, base := range []model.Phase{model.PhaseError, model.PhaseTimeout} {
		phase := EvalContainerPhaseConditions(context.Background(), conditions, eval, base, map[string]any{}, nil)
		if phase != model.PhaseSucceeded {
			t.Errorf("base %q: expected PhaseSucceeded override, got %q", base, phase)
		}
	}
}

func TestEvalContainerPhaseConditions_FailedFromAllSuccess(t *testing.T) {
	eval := &mockEval{fn: func(expr string, _ map[string]any) (any, error) {
		return expr == "reject", nil
	}}
	conditions := &model.PhaseConditions{Failed: "reject"}
	phase := EvalContainerPhaseConditions(context.Background(), conditions, eval, model.PhaseSucceeded, map[string]any{}, nil)
	if phase != model.PhaseFailed {
		t.Errorf("expected all-success aggregate flipped to PhaseFailed, got %q", phase)
	}
}

func TestEvalContainerPhaseConditions_NoMatchFallsBackToBase(t *testing.T) {
	eval := &mockEval{fn: func(string, map[string]any) (any, error) { return false, nil }}
	conditions := &model.PhaseConditions{Succeeded: "x", Failed: "y", Error: "z"}
	phase := EvalContainerPhaseConditions(context.Background(), conditions, eval, model.PhaseTimeout, map[string]any{}, nil)
	if phase != model.PhaseTimeout {
		t.Errorf("expected PhaseTimeout fallback, got %q", phase)
	}
}

func TestEvalContainerPhaseConditions_PriorityOrder(t *testing.T) {
	eval := &mockEval{fn: func(string, map[string]any) (any, error) { return true, nil }}
	conditions := &model.PhaseConditions{Succeeded: "a", Failed: "b", Error: "c"}
	phase := EvalContainerPhaseConditions(context.Background(), conditions, eval, model.PhaseError, map[string]any{}, nil)
	if phase != model.PhaseSucceeded {
		t.Errorf("expected PhaseSucceeded (highest priority), got %q", phase)
	}
}

func TestEvalContainerPhaseConditions_NilConditionsAndEvaluator(t *testing.T) {
	if got := EvalContainerPhaseConditions(context.Background(), nil, &mockEval{fn: func(string, map[string]any) (any, error) {
		t.Fatal("eval must not be called")
		return nil, nil
	}}, model.PhaseSucceeded, nil, nil); got != model.PhaseSucceeded {
		t.Errorf("nil conditions: expected base phase, got %q", got)
	}
	if got := EvalContainerPhaseConditions(context.Background(), &model.PhaseConditions{Failed: "x"}, nil, model.PhaseFailed, nil, nil); got != model.PhaseFailed {
		t.Errorf("nil evaluator: expected base phase, got %q", got)
	}
}

func TestEvalContainerPhaseConditions_NilEnv(t *testing.T) {
	eval := &mockEval{fn: func(string, map[string]any) (any, error) { return true, nil }}
	phase := EvalContainerPhaseConditions(context.Background(), &model.PhaseConditions{Failed: "x"}, eval, model.PhaseSucceeded, nil, nil)
	if phase != model.PhaseFailed {
		t.Errorf("expected PhaseFailed with nil env, got %q", phase)
	}
}

func TestEvalContainerPhaseConditions_EvalErrorReportsAndFallsBack(t *testing.T) {
	sink := &mockSink{}
	eval := &mockEval{fn: func(string, map[string]any) (any, error) {
		return nil, errors.New("boom")
	}}
	conditions := &model.PhaseConditions{Succeeded: "broken"}
	phase := EvalContainerPhaseConditions(context.Background(), conditions, eval, model.PhaseFailed, map[string]any{}, &EvalErrorContext{
		Sink:          sink,
		WorkflowRunID: "wf-1",
		TaskRunID:     "dag-1",
		Operation:     "evalContainerPhaseConditions",
	})
	if phase != model.PhaseFailed {
		t.Errorf("evaluation error must degrade to base phase, got %q", phase)
	}
	if len(sink.calls) != 1 {
		t.Fatalf("expected 1 ErrorSink report, got %d", len(sink.calls))
	}
	got := sink.calls[0]
	if got.Operation != "evalContainerPhaseConditions" {
		t.Errorf("operation: got %q, want %q", got.Operation, "evalContainerPhaseConditions")
	}
	if got.WorkflowRunID != "wf-1" || got.TaskRunID != "dag-1" {
		t.Errorf("identifiers not forwarded: %+v", got)
	}
	if got.Severity != errsink.SeverityWarning {
		t.Errorf("severity: got %v, want warning", got.Severity)
	}
}

func TestEvalContainerPhaseConditions_LeafOperationFallback(t *testing.T) {
	sink := &mockSink{}
	eval := &mockEval{fn: func(string, map[string]any) (any, error) {
		return nil, errors.New("boom")
	}}
	EvalPhaseConditions(context.Background(), &model.PhaseConditions{Succeeded: "broken"}, eval,
		&wire.TaskResult{ExecOutputs: &model.ExecOutputs{Code: model.ExecCodeFailed}}, &EvalErrorContext{Sink: sink})
	if len(sink.calls) != 1 {
		t.Fatalf("expected 1 ErrorSink report, got %d", len(sink.calls))
	}
	if sink.calls[0].Operation != "evalPhaseConditions" {
		t.Errorf("operation: got %q, want fallback %q", sink.calls[0].Operation, "evalPhaseConditions")
	}
}

// ---- BuildContainerPhaseEnv ----

func TestBuildContainerPhaseEnv_Shape(t *testing.T) {
	siblings := []*store.TaskRun{
		{
			TaskName: "verifier",
			Status:   testPhase(model.PhaseSucceeded),
			Outputs: &model.Outputs{ExecOutputs: model.ExecOutputs{
				Code:       model.ExecCodeSucceeded,
				Message:    "checked",
				Parameters: []model.Parameter{{Name: "verdict", Value: json.RawMessage(`"reject"`)}},
			}},
		},
		{TaskName: "gate", Status: testPhase(model.PhaseSkipped)},
	}
	outputs := &model.Outputs{ExecOutputs: model.ExecOutputs{
		Parameters: []model.Parameter{{Name: "summary", Value: json.RawMessage(`"bad"`)}},
	}}

	env := BuildContainerPhaseEnv(model.PhaseFailed, "one or more tasks failed", siblings, outputs)

	want := map[string]any{
		"phase":                "Failed",
		"msg":                  "one or more tasks failed",
		"tasks.verifier.phase": "Succeeded",
		"tasks.verifier.code":  model.ExecCodeSucceeded,
		"tasks.verifier.msg":   "checked",
		"tasks.verifier.outputs.parameters.verdict": "reject",
		"tasks.gate.phase":                          "Skipped",
		"outputs.parameters.summary":                "bad",
	}
	for k, v := range want {
		got, ok := env[k]
		if !ok {
			t.Errorf("env missing key %q", k)
			continue
		}
		if got != v {
			t.Errorf("env[%q] = %v (%T), want %v (%T)", k, got, got, v, v)
		}
	}

	// A skipped child exposes only its phase — no fabricated output keys.
	// The container contract deliberately excludes these namespaces.
	absent := []string{
		"code",
		"tasks.gate.code",
		"tasks.gate.msg",
		"tasks.gate.outputs.parameters.any",
		"workflow.parameters.x",
		"inputs.parameters.x",
		"loop_iter.index",
	}
	for _, k := range absent {
		if _, ok := env[k]; ok {
			t.Errorf("env must not contain key %q", k)
		}
	}
}

// ---- unmarshalParam ----

func TestUnmarshalParam(t *testing.T) {
	tests := []struct {
		name string
		raw  json.RawMessage
		want any
	}{
		{"null", json.RawMessage(`null`), nil},
		{"empty", json.RawMessage(``), nil},
		{"number", json.RawMessage(`42`), float64(42)},
		{"string", json.RawMessage(`"hello"`), "hello"},
		{"bool", json.RawMessage(`true`), true},
	}
	for _, tt := range tests {
		got := unmarshalParam(tt.raw)
		if got != tt.want {
			t.Errorf("unmarshalParam(%s) = %v (%T), want %v", tt.name, got, got, tt.want)
		}
	}
}
