package flow

import (
	"context"
	"fmt"
)

type ActionRequest struct {
	RunID   string
	Action  string
	Attempt int
	Data    map[string]interface{}
}

type ActionResult struct {
	Data   map[string]interface{}
	Detail string
}

type ActionExecutor interface {
	Execute(context.Context, ActionRequest) (ActionResult, error)
}

type Engine struct {
	Definition *Definition
	History    *History
	Actions    ActionExecutor
}

type ResumeOptions struct {
	Result string
	Data   map[string]interface{}
}

func (e *Engine) StartRun(ctx context.Context, runID string) (*RunState, error) {
	states := Replay(e.Definition, e.History.Events())
	if state := states[runID]; state != nil {
		return e.Resume(ctx, runID, ResumeOptions{})
	}
	if _, err := e.History.Append(Event{
		RunID:  runID,
		Type:   "run_started",
		Node:   e.Definition.Start,
		Detail: e.Definition.ID,
	}); err != nil {
		return nil, err
	}
	return e.Resume(ctx, runID, ResumeOptions{})
}

func (e *Engine) Resume(ctx context.Context, runID string, options ResumeOptions) (*RunState, error) {
	state := Replay(e.Definition, e.History.Events())[runID]
	if state == nil {
		return nil, fmt.Errorf("unknown run %q", runID)
	}
	if state.Status == "succeeded" {
		if options.Result != "" {
			if _, err := e.History.Append(Event{
				RunID:  runID,
				Type:   "external_result_ignored",
				Node:   state.Node,
				Result: options.Result,
				Detail: "run already confirmed and completed",
			}); err != nil {
				return nil, err
			}
		}
		return state, nil
	}
	if state.Status == "failed" {
		return state, fmt.Errorf("run %q already failed", runID)
	}

	if state.Status == "waiting" {
		if options.Result != "" {
			if state.WaitResult != options.Result {
				return state, fmt.Errorf("run %q does not wait for result %q", runID, options.Result)
			}
			waitNode := e.Definition.Nodes[state.Node]
			target := waitNode.OnResult
			if target == "" {
				return state, fmt.Errorf("wait node %q has no result transition", state.Node)
			}
			if _, err := e.History.Append(Event{
				RunID:  runID,
				Type:   "external_result_accepted",
				Node:   state.Node,
				Result: options.Result,
				Detail: target,
				Data:   options.Data,
			}); err != nil {
				return nil, err
			}
		} else if state.DeadlineNS != 0 && nowUnixNano() >= state.DeadlineNS {
			waitNode := e.Definition.Nodes[state.Node]
			target := waitNode.OnTimeout
			if target == "" {
				return state, fmt.Errorf("wait node %q has no timeout transition", state.Node)
			}
			if _, err := e.History.Append(Event{
				RunID:  runID,
				Type:   "wait_timeout",
				Node:   state.Node,
				Detail: target,
			}); err != nil {
				return nil, err
			}
		} else {
			return Replay(e.Definition, e.History.Events())[runID], nil
		}
		state = Replay(e.Definition, e.History.Events())[runID]
	}

	for {
		node, ok := e.Definition.Nodes[state.Node]
		if !ok {
			return state, fmt.Errorf("unknown node %q", state.Node)
		}
		if _, err := e.History.Append(Event{
			RunID: runID,
			Type:  "node_entered",
			Node:  state.Node,
		}); err != nil {
			return nil, err
		}

		switch node.Kind {
		case "succeed":
			if _, err := e.History.Append(Event{
				RunID: runID,
				Type:  "run_succeeded",
				Node:  state.Node,
			}); err != nil {
				return nil, err
			}
			return Replay(e.Definition, e.History.Events())[runID], nil

		case "action":
			attempt := state.Attempts[node.Action] + 1
			if _, err := e.History.Append(Event{
				RunID:   runID,
				Type:    "action_started",
				Node:    state.Node,
				Action:  node.Action,
				Attempt: attempt,
			}); err != nil {
				return nil, err
			}
			result, err := e.Actions.Execute(ctx, ActionRequest{
				RunID:   runID,
				Action:  node.Action,
				Attempt: attempt,
				Data:    options.Data,
			})
			if err != nil {
				if _, appendErr := e.History.Append(Event{
					RunID:   runID,
					Type:    "action_failed",
					Node:    state.Node,
					Action:  node.Action,
					Attempt: attempt,
					Detail:  err.Error(),
				}); appendErr != nil {
					return nil, appendErr
				}
				return nil, err
			}
			if _, err := e.History.Append(Event{
				RunID:   runID,
				Type:    "action_completed",
				Node:    state.Node,
				Next:    node.Next,
				Action:  node.Action,
				Attempt: attempt,
				Detail:  result.Detail,
				Data:    result.Data,
			}); err != nil {
				return nil, err
			}
			state = Replay(e.Definition, e.History.Events())[runID]

		case "wait":
			deadline := int64(0)
			if node.TimeoutMS > 0 {
				deadline = nowUnixNano() + node.TimeoutMS*1_000_000
			}
			if _, err := e.History.Append(Event{
				RunID:      runID,
				Type:       "wait_started",
				Node:       state.Node,
				Result:     node.Result,
				DeadlineNS: deadline,
			}); err != nil {
				return nil, err
			}
			return Replay(e.Definition, e.History.Events())[runID], nil

		default:
			return state, fmt.Errorf("unsupported node kind %q", node.Kind)
		}
	}
}
