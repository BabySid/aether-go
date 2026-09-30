package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	aether "github.com/BabySid/aether-go"
	"github.com/BabySid/aether-go/broker"
	"github.com/BabySid/aether-go/executor"
	"github.com/BabySid/aether-go/hook"
	"github.com/BabySid/aether-go/model"
)

// controlledBroker exercises both inline callbacks and completion after Dispatch
// returns, without timers or worker scheduling affecting the verdict.
type controlledBroker struct {
	*LocalBroker
	dispatch func(context.Context, *broker.TaskAssignment) error
}

func (b *controlledBroker) Dispatch(ctx context.Context, a *broker.TaskAssignment) error {
	return b.dispatch(ctx, a)
}

type countingHooks map[hook.Type]int

func (h countingHooks) Notify(_ context.Context, event *hook.Event) error {
	if event.Scope == hook.ScopeWorkflow {
		h[event.HookType]++
	}
	return nil
}

func TestEmptyLoopScheduling(t *testing.T) {
	files, err := filepath.Glob("examples/*-empty-loop-*.json")
	if err != nil {
		t.Fatal(err)
	}
	if len(files) == 0 {
		t.Fatal("no empty-loop examples found")
	}
	for _, inline := range []bool{false, true} {
		for _, file := range files {
			mode := "queued"
			if inline {
				mode = "inline"
			}
			t.Run(filepath.Base(file)+"/"+mode, func(t *testing.T) {
				var wf model.Workflow
				readExampleJSON(t, file, &wf)
				var assertion WorkflowAssertion
				readExampleJSON(t, filepath.Join("examples/assertions", filepath.Base(file)), &assertion)
				runEmptyLoop(t, &wf, &assertion, inline)
			})
		}
	}
}

func readExampleJSON(t *testing.T, path string, dst any) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, dst); err != nil {
		t.Fatal(err)
	}
}

func runEmptyLoop(t *testing.T, wf *model.Workflow, assertion *WorkflowAssertion, inline bool) {
	t.Helper()
	ctx := context.Background()
	events := countingHooks{}
	bind := false
	for _, task := range assertion.ExpectTasks {
		if task.TaskName == "summarize" {
			_, bind = task.ExpectInputs["scores"]
		}
	}
	ms := NewMemoryStore()
	echo := newEcho()
	var eng *aether.Engine
	var queue []*broker.TaskAssignment
	var lastResult *broker.TaskResult
	seen := map[string]int{}
	execute := func(a *broker.TaskAssignment) {
		state, err := eng.Get(ctx, a.WorkflowRunID)
		if err != nil {
			t.Fatal(err)
		}
		if state.Status.IsTerminal() {
			t.Fatalf("dispatched %s after workflow finalized: %s", a.TaskName, state.Status)
		}
		if a.TaskName == "summarize" {
			for _, tr := range state.Tasks {
				if tr.TaskName == "batch" || tr.TaskName == "loop-a" || tr.TaskName == "loop-b" {
					if tr.Status != model.PhaseSucceeded {
						t.Fatalf("dependency %s: %s", tr.TaskName, tr.Status)
					}
				}
			}
			if bind {
				var in struct {
					Scores []int `json:"scores"`
				}
				if err := executor.BindInputs(a.Inputs, &in); err != nil {
					t.Fatal(err)
				}
				if in.Scores == nil || len(in.Scores) != 0 {
					t.Fatalf("scores = %#v", in.Scores)
				}
			}
		}
		eng.OnTaskStarted(ctx, a.TaskRunID)
		out, err := echo.Execute(ctx, &executor.ExecuteRequest{Inputs: a.Inputs, TaskRunID: a.TaskRunID})
		if err != nil {
			t.Fatal(err)
		}
		result := &broker.TaskResult{TaskRunID: a.TaskRunID, WorkflowRunID: a.WorkflowRunID, ExecOutputs: out}
		eng.OnTaskCompleted(ctx, result)
		lastResult = result
	}
	b := &controlledBroker{LocalBroker: NewLocalBroker(nil, nil)}
	b.dispatch = func(ctx context.Context, a *broker.TaskAssignment) error {
		if _, err := ms.GetTaskRun(ctx, a.TaskRunID); err != nil {
			t.Fatalf("unpersisted run: %v", err)
		}
		seen[a.TaskRunID]++
		if seen[a.TaskRunID] != 1 {
			t.Fatalf("duplicate dispatch: %s", a.TaskRunID)
		}
		if inline {
			execute(a)
		} else {
			queue = append(queue, a)
		}
		return nil
	}
	var err error
	eng, err = aether.New(aether.WithStore(ms), aether.WithIDGenerator(NewAtomicIDGen()), aether.WithExprEvaluator(NewSimpleEvaluator()), aether.WithTaskBroker(b), aether.WithExecutor(echo), aether.WithHookNotifier(events))
	if err != nil {
		t.Fatal(err)
	}
	id, err := eng.Submit(ctx, wf)
	if err != nil {
		t.Fatal(err)
	}
	for len(queue) > 0 {
		// Complete the most recently dispatched branch first.
		a := queue[len(queue)-1]
		queue = queue[:len(queue)-1]
		execute(a)
	}
	if lastResult != nil {
		eng.OnTaskCompleted(ctx, lastResult)
	}
	state, err := eng.Get(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	for _, failure := range Verify(state, assertion) {
		t.Error(failure)
	}
	identities := map[string]bool{}
	for _, tr := range state.Tasks {
		key := tr.ParentRunID + "/" + tr.Scope + "/" + tr.TaskName
		if identities[key] {
			t.Errorf("duplicate logical task: %s", key)
		}
		identities[key] = true
		if tr.Status != model.PhaseSucceeded {
			t.Errorf("%s = %s", key, tr.Status)
		}
		if tr.TemplateType == model.TemplateTypeTask && seen[tr.RunID] != 1 {
			t.Errorf("%s was not executed exactly once", key)
		}

	}
	if events[hook.OnSuccess] != 1 || events[hook.OnExit] != 1 {
		t.Errorf("completion hooks = %v", events)
	}
	if len(seen) == 0 && events[hook.OnStart] != 0 {
		t.Errorf("unexpected start hook: %v", events)
	}
	if len(seen) == 0 && (state.Metrics == nil || state.Metrics.StartedAt != "" || state.Metrics.FinishedAt == "") {
		t.Errorf("zero-execution metrics = %#v", state.Metrics)
	}
}

func TestEmptyLoopCancellation(t *testing.T) {
	raw, err := os.ReadFile("examples/32-empty-loop-binding.json")
	if err != nil {
		t.Fatal(err)
	}
	var wf model.Workflow
	if err = json.Unmarshal(raw, &wf); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	ms := NewMemoryStore()
	var pending *broker.TaskAssignment
	b := &controlledBroker{LocalBroker: NewLocalBroker(nil, nil)}
	b.dispatch = func(_ context.Context, a *broker.TaskAssignment) error {
		if pending != nil {
			t.Fatal("unexpected extra dispatch")
		}
		pending = a
		return nil
	}
	eng, err := aether.New(aether.WithStore(ms), aether.WithIDGenerator(NewAtomicIDGen()), aether.WithExprEvaluator(NewSimpleEvaluator()), aether.WithTaskBroker(b), aether.WithExecutor(newEcho()))
	if err != nil {
		t.Fatal(err)
	}
	id, err := eng.Submit(ctx, &wf)
	if err != nil {
		t.Fatal(err)
	}
	if pending == nil || pending.TaskName != "summarize" {
		t.Fatal("downstream was not dispatched")
	}
	if err = eng.Cancel(ctx, id); err != nil {
		t.Fatal(err)
	}
	// Late worker events must not revive a cancelled workflow or its parent DAG.
	eng.OnTaskStarted(ctx, pending.TaskRunID)
	eng.OnTaskCompleted(ctx, &broker.TaskResult{TaskRunID: pending.TaskRunID, WorkflowRunID: id, ExecOutputs: &model.ExecOutputs{Code: model.ExecCodeSucceeded}})
	state, err := eng.Get(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if state.Status != model.PhaseCancelled {
		t.Fatalf("workflow = %s", state.Status)
	}
	for _, tr := range state.Tasks {
		want := model.PhaseCancelled
		if tr.TaskName == "batch" {
			want = model.PhaseSucceeded
		}
		if tr.Status != want {
			t.Errorf("%s = %s, want %s", tr.TaskName, tr.Status, want)
		}
	}
}
