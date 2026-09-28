package simulator

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"

	"persistent-flow/internal/flow"
)

type Executor struct {
	path             string
	crashFirstNotify bool
	mu               sync.Mutex
}

type invocation struct {
	Key     string `json:"key"`
	Action  string `json:"action"`
	Attempt int    `json:"attempt"`
}

func NewExecutor(path string, crashFirstNotify bool) *Executor {
	return &Executor{path: path, crashFirstNotify: crashFirstNotify}
}

func (e *Executor) Execute(_ context.Context, request flow.ActionRequest) (flow.ActionResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := os.MkdirAll(filepath.Dir(e.path), 0o755); err != nil {
		return flow.ActionResult{}, err
	}
	key := request.RunID + ":" + request.Action
	seen, err := e.seen(key)
	if err != nil {
		return flow.ActionResult{}, err
	}
	if request.Action == "request_approval" && !seen && e.crashFirstNotify {
		if err := e.append(invocation{Key: key, Action: request.Action, Attempt: request.Attempt}); err != nil {
			return flow.ActionResult{}, err
		}
		os.Exit(42)
	}
	duplicate := false
	if seen {
		duplicate = true
	} else {
		if err := e.append(invocation{Key: key, Action: request.Action, Attempt: request.Attempt}); err != nil {
			return flow.ActionResult{}, err
		}
	}
	detail := "external side effect executed"
	if duplicate {
		detail = "duplicate invocation detected by external idempotency key"
	}
	return flow.ActionResult{
		Detail: detail,
		Data: map[string]interface{}{
			"external_idempotency_key": key,
			"duplicate_invocation":     duplicate,
		},
	}, nil
}

func (e *Executor) seen(key string) (bool, error) {
	f, err := os.Open(e.path)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		var item invocation
		if err := json.Unmarshal(scanner.Bytes(), &item); err != nil {
			return false, err
		}
		if item.Key == key {
			return true, nil
		}
	}
	return false, scanner.Err()
}

func (e *Executor) append(item invocation) error {
	f, err := os.OpenFile(e.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	return json.NewEncoder(f).Encode(item)
}
