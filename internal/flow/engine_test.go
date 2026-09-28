package flow

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

type fakeActions struct{ calls []string }

func (f *fakeActions) Execute(_ context.Context, request ActionRequest) (ActionResult, error) {
	f.calls = append(f.calls, request.Action)
	return ActionResult{}, nil
}

func testEngine(t *testing.T) (*Engine, string) {
	t.Helper()
	dir := t.TempDir()
	historyPath := filepath.Join(dir, "history.jsonl")
	history, err := OpenHistory(historyPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = history.Close() })
	definition := &Definition{
		ID:    "test",
		Start: "do",
		Nodes: map[string]NodeSpec{
			"do":   {Kind: "action", Action: "do", Next: "wait"},
			"wait": {Kind: "wait", Result: "answer", TimeoutMS: 20, OnResult: "done", OnTimeout: "done"},
			"done": {Kind: "succeed"},
		},
	}
	return &Engine{Definition: definition, History: history, Actions: &fakeActions{}}, historyPath
}

func TestExternalResultIsConfirmedOnce(t *testing.T) {
	engine, historyPath := testEngine(t)
	if _, err := engine.StartRun(context.Background(), "r1"); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Resume(context.Background(), "r1", ResumeOptions{Result: "answer"}); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Resume(context.Background(), "r1", ResumeOptions{Result: "answer"}); err != nil {
		t.Fatalf("duplicate confirmed result should be ignored: %v", err)
	}

	history, err := OpenHistory(historyPath)
	if err != nil {
		t.Fatal(err)
	}
	accepted := 0
	for _, event := range history.Events() {
		if event.Type == "external_result_accepted" {
			accepted++
		}
	}
	if accepted != 1 {
		t.Fatalf("accepted = %d, want 1", accepted)
	}
}

func TestTimeoutRestoresFromPersistedDeadline(t *testing.T) {
	engine, _ := testEngine(t)
	state, err := engine.StartRun(context.Background(), "r2")
	if err != nil {
		t.Fatal(err)
	}
	if state.Status != "waiting" {
		t.Fatalf("status = %s, want waiting", state.Status)
	}
	time.Sleep(25 * time.Millisecond)
	state, err = engine.Resume(context.Background(), "r2", ResumeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if state.Status != "succeeded" {
		t.Fatalf("status = %s, want succeeded", state.Status)
	}
}
