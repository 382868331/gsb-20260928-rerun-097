package engine

import "fmt"

const detailVerifiedNotOccurred = "verified not occurred; action may be retried"

// Advance 尽可能向前推进流程：执行动作、登记等待，遇到未决等待、
// 不确定动作或流程结束时停止。重复调用是安全的（所有决策只依据历史）。
func (e *Engine) Advance(actions map[string]Action) error {
	for i := range e.def.Steps {
		st := &e.steps[i]
		if st.done {
			continue
		}
		step := e.def.Steps[i]
		if !st.started {
			if err := e.record(Event{Type: EvtStepStarted, StepIndex: i, Key: step.Name}); err != nil {
				return err
			}
		}
		switch step.Kind {
		case "action":
			if err := e.runAction(i, step, actions); err != nil {
				return err // ErrActionUnknown：等待核实，不重复执行
			}
			if !st.done {
				return nil // 结果尚未核实
			}
		case "wait":
			if err := e.scheduleWait(i, step); err != nil {
				return err
			}
			if err := e.checkTimeout(i); err != nil {
				return err
			}
			if !st.done {
				return ErrWaitPending // 等待结果（无论是否已超时）
			}
		default:
			return fmt.Errorf("engine: unknown step kind %q at index %d", step.Kind, i)
		}
	}
	if !e.done {
		return e.record(Event{Type: EvtRunCompleted})
	}
	return nil
}

func (e *Engine) runAction(i int, step Step, actions map[string]Action) error {
	st := &e.steps[i]
	if st.action != nil && st.action.state == ActionStateDone {
		return e.markStepCompleted(i, st.action.token)
	}
	if st.action != nil {
		// uncertain：外部调用可能已经发生，绝不自动重试。
		return fmt.Errorf("step %d (%s): %w", i, step.Name, ErrActionUnknown)
	}
	act := actions[step.Action]
	if act == nil {
		return fmt.Errorf("engine: action %q not registered", step.Action)
	}
	token := newToken("act")
	// 调用与确认之间的窗口就是“结果未知”窗口。
	result, err := act.Invoke(e.runID, i, step.Params)
	if err != nil {
		return fmt.Errorf("invoke action %q: %w", step.Action, err)
	}
	if err := e.record(Event{
		Type:      EvtActionInvoked,
		StepIndex: i,
		Token:     token,
		Key:       step.Action,
	}); err != nil {
		return err
	}
	return e.confirmAction(token, result)
}

// VerifyAction 用于恢复后核实不确定动作：由外部系统查证真实结果后登记。
// 核实确认动作其实未发生时，传入 occurred=false，引擎随后可重新执行一次。
func (e *Engine) VerifyAction(token, result string, occurred bool) error {
	i, ok := e.actions[token]
	if !ok {
		return fmt.Errorf("%w: %s", ErrUnknownToken, token)
	}
	st := &e.steps[i]
	if st.action == nil {
		return fmt.Errorf("%w: %s", ErrUnknownToken, token)
	}
	if st.action.state == ActionStateDone {
		return e.record(Event{
			Type:      EvtExternalDuplicate,
			StepIndex: i,
			Token:     token,
			Detail:    "action re-verification ignored",
		})
	}
	if !occurred {
		// 外部核实确认副作用没有发生：追加核实事件，然后允许重新执行。
		return e.record(Event{
			Type:      EvtExternalResult,
			StepIndex: i,
			Token:     token,
			Detail:    detailVerifiedNotOccurred,
		})
	}
	return e.confirmAction(token, result)
}

func (e *Engine) confirmAction(token, result string) error {
	i := e.actions[token]
	st := &e.steps[i]
	if st.action != nil && st.action.state == ActionStateDone {
		return e.record(Event{
			Type:      EvtExternalDuplicate,
			StepIndex: i,
			Token:     token,
			Detail:    "duplicate action confirmation ignored",
		})
	}
	if err := e.record(Event{
		Type:      EvtActionAcked,
		StepIndex: i,
		Token:     token,
		Result:    result,
	}); err != nil {
		return err
	}
	return e.markStepCompleted(i, token)
}

func (e *Engine) scheduleWait(i int, step Step) error {
	st := &e.steps[i]
	if st.wait != nil {
		return nil
	}
	token := newToken("wait")
	deadline := e.nowMS() + step.TimeoutMS
	return e.record(Event{
		Type:      EvtWaitScheduled,
		StepIndex: i,
		Token:     token,
		Key:       step.Name,
		Deadline:  deadline,
	})
}

// DeliverResult 登记外部结果。重复提交只记一条 duplicate 审计事件，
// 不会让同一动作/等待生效第二次。
func (e *Engine) DeliverResult(token, result string) error {
	if i, ok := e.pending[token]; ok {
		if err := e.record(Event{Type: EvtExternalResult, StepIndex: i, Token: token, Result: result}); err != nil {
			return err
		}
		if err := e.record(Event{Type: EvtWaitFired, StepIndex: i, Token: token, Result: result}); err != nil {
			return err
		}
		return e.markStepCompleted(i, token)
	}
	if _, ok := e.actions[token]; ok {
		return e.confirmAction(token, result)
	}
	if _, known := e.tokens[token]; known {
		// 已知令牌但步骤已完成：重复投递只记一次审计，不再次生效。
		return e.record(Event{
			Type:   EvtExternalDuplicate,
			Token:  token,
			Result: result,
			Detail: "duplicate external result ignored",
		})
	}
	return fmt.Errorf("%w: %s", ErrUnknownToken, token)
}

// CheckTimeouts 检查所有未决等待是否超过截止时间。超时只表示“暂时没等到”，
// 等待保持未决；迟到的结果仍可通过 DeliverResult 完成它。
func (e *Engine) CheckTimeouts() error {
	indices := make(map[int]struct{}, len(e.pending))
	for _, i := range e.pending {
		indices[i] = struct{}{}
	}
	for i := range indices {
		if err := e.checkTimeout(i); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) checkTimeout(i int) error {
	st := &e.steps[i]
	if st.wait == nil || st.wait.state != WaitStateWaiting {
		return nil
	}
	if e.nowMS() < st.wait.deadline {
		return nil
	}
	return e.record(Event{
		Type:      EvtWaitTimedOut,
		StepIndex: i,
		Token:     st.wait.token,
	})
}

func (e *Engine) markStepCompleted(i int, token string) error {
	if e.steps[i].done {
		return nil
	}
	return e.record(Event{Type: EvtStepCompleted, StepIndex: i, Token: token})
}

// WaitToken 返回某等待步骤当前的外部结果令牌。
func (e *Engine) WaitToken(stepIndex int) (string, bool) {
	if stepIndex < 0 || stepIndex >= len(e.steps) {
		return "", false
	}
	w := e.steps[stepIndex].wait
	if w == nil {
		return "", false
	}
	return w.token, true
}

// ActionToken 返回某动作步骤的调用令牌（用于核实）。
func (e *Engine) ActionToken(stepIndex int) (string, bool) {
	if stepIndex < 0 || stepIndex >= len(e.steps) {
		return "", false
	}
	a := e.steps[stepIndex].action
	if a == nil {
		return "", false
	}
	return a.token, true
}

// Status 汇总当前进度与所有待处理事项。
func (e *Engine) Status() Status {
	st := Status{RunID: e.runID, Done: e.done, PendingWaits: []PendingItem{}, UncertainActions: []UncertainItem{}}
	for i := range e.steps {
		s := &e.steps[i]
		if s.done {
			st.CompletedSteps++
			continue
		}
		if s.wait != nil && (s.wait.state == WaitStateWaiting || s.wait.state == WaitStateTimedOut) {
			st.PendingWaits = append(st.PendingWaits, PendingItem{
				StepIndex: i,
				Name:      e.def.Steps[i].Name,
				Token:     s.wait.token,
				Deadline:  s.wait.deadline,
				TimedOut:  s.wait.state == WaitStateTimedOut,
			})
		}
		if s.action != nil && s.action.state == ActionStateUncertain {
			st.UncertainActions = append(st.UncertainActions, UncertainItem{
				StepIndex: i,
				Name:      e.def.Steps[i].Name,
				Action:    e.def.Steps[i].Action,
				Token:     s.action.token,
			})
		}
	}
	if !e.done && st.CompletedSteps < len(e.steps) {
		for i := range e.steps {
			if !e.steps[i].done {
				st.CurrentStep = i
				break
			}
		}
	}
	return st
}
