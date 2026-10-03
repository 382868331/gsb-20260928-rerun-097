package engine

import (
	"fmt"
	"time"
)

// DeliverBranchResult 向等待组分支投递一个成功结果。
// 结果先落盘；若组已有决策，结果仍记录为迟到结果，但不改变决策快照。
func (e *Engine) DeliverBranchResult(token, result string) error {
	return e.deliverBranch(token, result, outcomeSuccess, EvtExternalResult)
}

// DeliverBranchFailure 向等待组分支投递一个显式失败结果。失败是否计入
// 本组条件取决于该组的 CountFailures 配置。
func (e *Engine) DeliverBranchFailure(token, failure string) error {
	return e.deliverBranch(token, failure, outcomeFailure, EvtBranchFailure)
}

// CancelBranch 对等待组分支投递取消信号。取消按失败终态参与裁决，
// 是否满足本组条件同样由 CountFailures 明确决定。
func (e *Engine) CancelBranch(token, reason string) error {
	return e.deliverBranch(token, reason, outcomeCancel, EvtExternalCancel)
}

func (e *Engine) deliverBranch(token, value, kind string, evType EventType) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	b, ok := e.branchByToken[token]
	if !ok {
		return fmt.Errorf("engine: unknown branch token %q", token)
	}
	// 已终态：重复/迟到结果仍记录，但绝不改写已选定的结果集合。
	if b.terminal {
		if kind == outcomeSuccess && b.outcome.kind == outcomeSuccess && b.outcome.result == value {
			return e.appendLocked(Event{Type: EvtExternalDuplicate, Token: token, Result: value, Detail: "branch"})
		}
		b.lates = append(b.lates, branchOutcome{kind: kind, result: value, attempt: b.attempt})
		return e.appendLocked(Event{Type: evType, Token: token, Result: value, Detail: "late"})
	}
	if b.actionPending {
		// 分支停在未核实外部效果上：外部结果不能覆盖它，只能先核实。
		return fmt.Errorf("engine: branch %s has an unverified effect; confirm it first", token)
	}
	detail := ""
	if b.parent.decided {
		detail = "late"
	}
	if err := e.appendLocked(Event{Type: evType, Token: token, Result: value, Detail: detail}); err != nil {
		return err
	}
	if b.parent.decided {
		// 决策后到达：结果仍归属于该分支并展示；决策快照保持不变。
		return e.recordBranchOutcomeLocked(b, kind, value)
	}
	if err := e.recordBranchOutcomeLocked(b, kind, value); err != nil {
		return err
	}
	return e.evaluateGroupLocked(b.parent)
}

// recordBranchOutcomeLocked 记录分支终态（历史已由调用方落盘）。
func (e *Engine) recordBranchOutcomeLocked(b *branchState, kind, result string) error {
	b.terminal = true
	b.outcome = &branchOutcome{kind: kind, result: result, attempt: b.attempt}
	return nil
}

// RetryBranch 在分支尚未给出有效终态时对其进行部分重试。
// 已结束的分支可以重试并开始新的一轮（attempt+1），新结果只影响尚未决策的组；
// 根组一旦形成决策，重试被拒绝（迟到结果仍可通过 Deliver* 记录）。
func (e *Engine) RetryBranch(token string, actions map[string]Action) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	b, ok := e.branchByToken[token]
	if !ok {
		return fmt.Errorf("engine: unknown branch token %q", token)
	}
	if b.parent.decided {
		return ErrGroupDecided
	}
	if b.actionPending {
		return fmt.Errorf("engine: branch %s has an unverified effect; ConfirmBranchAction first", token)
	}
	b.attempt++
	b.terminal = false
	b.outcome = nil
	b.hasDeadline = b.spec.TimeoutMS > 0
	if b.hasDeadline {
		b.deadline = e.clock.Now().Add(time.Duration(b.spec.TimeoutMS) * time.Millisecond)
	}
	b.timerArmed = false
	b.cancelAsked = false
	if err := e.appendLocked(Event{
		Type:      EvtBranchRetried,
		StepIndex: b.parent.stepIndex,
		Token:     token,
		Result:    fmt.Sprintf("attempt=%d", b.attempt),
	}); err != nil {
		return err
	}
	if b.spec.Action != "" {
		key := branchActionKey(token, b.attempt)
		b.actionKey = key
		if err := e.appendLocked(Event{Type: EvtBranchActionInvoked, StepIndex: b.parent.stepIndex, Token: token, Key: key, Result: b.spec.Action, Detail: fmt.Sprintf("attempt=%d", b.attempt)}); err != nil {
			return err
		}
		act, ok := actions[b.spec.Action]
		if !ok {
			return fmt.Errorf("engine: no action registered for %q", b.spec.Action)
		}
		result, invokeErr := act.Invoke(e.runID, b.parent.stepIndex, b.spec.Params)
		if e.hooks.CrashAfterInvoke != nil && e.hooks.CrashAfterInvoke(e.runID, b.parent.stepIndex) {
			return ErrCrashed
		}
		if invokeErr != nil {
			b.actionPending = true
			e.groupUncertain = append(e.groupUncertain, UncertainEffect{StepIndex: b.parent.stepIndex, Token: token, Attempt: b.attempt, Key: key, Action: b.spec.Action})
			return ErrUncertain
		}
		if err := e.appendLocked(Event{Type: EvtBranchActionAcked, StepIndex: b.parent.stepIndex, Token: token, Key: key, Result: result, Detail: fmt.Sprintf("attempt=%d", b.attempt)}); err != nil {
			return err
		}
		if err := e.recordBranchOutcomeLocked(b, outcomeSuccess, result); err != nil {
			return err
		}
		return e.evaluateGroupLocked(b.parent)
	}
	if b.hasDeadline {
		b.timerArmed = true
		e.scheduleBranchTimeout(b)
	}
	return nil
}

// ConfirmBranchAction 在人工核实分支 action 确实发生后补记确认。
// 确认归属到重试轮次（attempt），重复确认只生效一次。
func (e *Engine) ConfirmBranchAction(token string, attempt int, result string) (bool, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	b, ok := e.branchByToken[token]
	if !ok {
		return false, fmt.Errorf("engine: unknown branch token %q", token)
	}
	if attempt <= 0 {
		attempt = b.attempt
	}
	key := branchActionKey(token, attempt)
	var found int = -1
	for i, u := range e.groupUncertain {
		if u.Token == token && u.Key == key {
			found = i
			break
		}
	}
	if found < 0 {
		return false, nil
	}
	if err := e.appendLocked(Event{Type: EvtBranchActionAcked, StepIndex: b.parent.stepIndex, Token: token, Key: key, Result: result, Detail: fmt.Sprintf("attempt=%d", attempt)}); err != nil {
		return false, err
	}
	e.groupUncertain = append(e.groupUncertain[:found], e.groupUncertain[found+1:]...)
	if attempt == b.attempt {
		b.actionPending = false
		if !b.parent.decided {
			if err := e.recordBranchOutcomeLocked(b, outcomeSuccess, result); err != nil {
				return false, err
			}
			if err := e.evaluateGroupLocked(b.parent); err != nil {
				return false, err
			}
		}
	}
	return true, nil
}

// UncertainEffects 返回等待组中所有已触发但尚未核实的外部效果。
// 即使组已决策、分支不再被需要，这些效果仍单独保留。
func (e *Engine) UncertainEffects() []UncertainEffect {
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.groupUncertain) == 0 {
		return nil
	}
	out := make([]UncertainEffect, len(e.groupUncertain))
	copy(out, e.groupUncertain)
	return out
}

// GroupStatus 返回指定 group 步骤的可观测状态（各分支结果、参与决策的
// 分支、尚未完成的分支，以及失败是否计入条件）。
func (e *Engine) GroupStatus(stepIndex int) *GroupStatus {
	e.mu.Lock()
	defer e.mu.Unlock()
	tok := groupToken(e.runID, stepIndex, "")
	root, ok := e.groupByToken[tok]
	if !ok {
		return nil
	}
	return e.groupStatusLocked(root)
}

func (e *Engine) groupStatusLocked(g *groupState) *GroupStatus {
	st := &GroupStatus{
		Token:            g.token,
		Mode:             g.spec.Mode,
		CountFailures:    g.spec.CountFailures,
		CollectRemaining: g.spec.CollectRemaining,
		Decided:          g.decided,
		Satisfied:        g.satisfied,
		SelectedTokens:   append([]string{}, g.selected...),
		Groups:           map[string]GroupStatus{},
	}
	selectedSet := map[string]bool{}
	for _, s := range g.selected {
		selectedSet[s] = true
	}
	for i := range g.spec.Branches {
		if c := g.children[i]; c != nil {
			child := e.groupStatusLocked(c)
			st.Groups[c.token] = *child
			bs := BranchStatus{
				Token:      c.token,
				Name:       g.spec.Branches[i].Name,
				Pending:    !c.decided,
				Terminal:   c.decided,
				Outcome:    "subgroup",
				InDecision: selectedSet[c.token],
			}
			st.Branches = append(st.Branches, bs)
			continue
		}
		b := e.branchByToken[branchToken(g.token, i)]
		bs := BranchStatus{
			Token:       b.token,
			Name:        b.name,
			Attempt:     b.attempt,
			Pending:     !b.terminal,
			Terminal:    b.terminal,
			CancelAsked: b.cancelAsked,
			Unverified:  b.actionPending,
			InDecision:  selectedSet[b.token],
			LateCount:   len(b.lates),
		}
		if b.outcome != nil {
			bs.Outcome = b.outcome.kind
			bs.Result = b.outcome.result
		}
		st.Branches = append(st.Branches, bs)
	}
	if len(st.Groups) == 0 {
		st.Groups = nil
	}
	return st
}
