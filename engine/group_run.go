package engine

import (
	"fmt"
	"time"
)

// runGroupStep 驱动一个并行等待组步骤，直到组形成唯一决策并完成。
func (e *Engine) runGroupStep(stepIndex int, st Step, actions map[string]Action) error {
	e.mu.Lock()
	root := e.currentGroup
	recovered := root != nil && root.stepIndex == stepIndex
	if !recovered {
		root = buildGroupTree(e.runID, stepIndex, st.Group)
		e.currentGroup = root
		e.registerGroupLocked(root)
	}
	now := e.clock.Now()
	if !recovered {
		if err := e.appendLocked(Event{Type: EvtStepStarted, StepIndex: stepIndex, Detail: st.Name}); err != nil {
			e.mu.Unlock()
			return err
		}
		if err := e.openGroupLocked(root, now); err != nil {
			e.mu.Unlock()
			return err
		}
		var uncertainErr error
		if err := e.armGroupTreeLocked(root, now, actions, &uncertainErr); err != nil {
			e.mu.Unlock()
			return err
		}
		if uncertainErr != nil {
			// 组已打开、分支已调度，仅本分支停在未核实效果上。
			e.mu.Unlock()
			return uncertainErr
		}
	} else if len(e.groupUncertain) > 0 {
		e.mu.Unlock()
		return ErrUncertain
	}
	// 恢复且组已有决策：直接完成步骤，沿用已确认的结果归属。
	if recovered && root.decided {
		err := e.completeGroupStepLocked(root)
		e.mu.Unlock()
		return err
	}
	ch := make(chan struct{}, 1)
	e.groupWaitCh[root.token] = ch
	if root.decided {
		delete(e.groupWaitCh, root.token)
		err := e.completeGroupStepLocked(root)
		e.mu.Unlock()
		return err
	}
	if recovered {
		// 恢复后重新挂超时：截止时间沿用历史中确认过的 deadline。
		e.armRecoveredTimersLocked(root, now)
	} else {
		e.armGroupTimersLocked(root)
	}
	e.mu.Unlock()

	select {
	case <-ch:
	case <-e.stopCh:
		return nil
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.groupWaitCh, root.token)
	return e.completeGroupStepLocked(root)
}

// openGroupLocked 递归落盘 group_opened 与每个叶分支的 branch_scheduled。
func (e *Engine) openGroupLocked(g *groupState, now time.Time) error {
	if !g.opened {
		g.opened = true
		g.deadline = now.Add(time.Duration(g.spec.TimeoutMS) * time.Millisecond)
		g.hasDeadline = g.spec.TimeoutMS > 0
		if err := e.appendLocked(Event{
			Type:      EvtGroupOpened,
			StepIndex: g.stepIndex,
			Token:     g.token,
			Deadline:  g.deadline.UnixMilli(),
			Result:    string(g.spec.Mode),
			Detail:    g.path,
			Extra: map[string]interface{}{
				"mode":              string(g.spec.Mode),
				"quota":             g.spec.Quota,
				"count_failures":    g.spec.CountFailures,
				"collect_remaining": g.spec.CollectRemaining,
			},
		}); err != nil {
			return err
		}
	}
	for i, b := range g.spec.Branches {
		if c := g.children[i]; c != nil {
			if err := e.openGroupLocked(c, now); err != nil {
				return err
			}
			continue
		}
		bs := e.branchByToken[branchToken(g.token, i)]
		if bs.scheduled {
			continue
		}
		bs.scheduled = true
		bs.attempt = 1
		if b.TimeoutMS > 0 {
			bs.hasDeadline = true
			bs.deadline = now.Add(time.Duration(b.TimeoutMS) * time.Millisecond)
		}
		if err := e.appendLocked(Event{
			Type:      EvtBranchScheduled,
			StepIndex: g.stepIndex,
			Token:     bs.token,
			Deadline:  bs.deadline.UnixMilli(),
			Result:    bs.attemptResult(),
			Detail:    b.Name,
		}); err != nil {
			return err
		}
	}
	return nil
}

// armGroupTreeLocked 处理每个叶分支的前置 action，并触发即时裁决。
// action 返回错误不代表没有发生：保留为未核实外部效果，绝不自动重试。
func (e *Engine) armGroupTreeLocked(g *groupState, now time.Time, actions map[string]Action, uncertainErr *error) error {
	for i, b := range g.spec.Branches {
		if c := g.children[i]; c != nil {
			if err := e.armGroupTreeLocked(c, now, actions, uncertainErr); err != nil {
				return err
			}
			continue
		}
		bs := e.branchByToken[branchToken(g.token, i)]
		if b.Action == "" || bs.actionPending || bs.terminal {
			continue
		}
		key := branchActionKey(bs.token, bs.attempt)
		bs.actionKey = key
		if err := e.appendLocked(Event{
			Type:      EvtBranchActionInvoked,
			StepIndex: g.stepIndex,
			Token:     bs.token,
			Key:       key,
			Result:    b.Action,
			Detail:    fmt.Sprintf("attempt=%d", bs.attempt),
		}); err != nil {
			return err
		}
		act, ok := actions[b.Action]
		if !ok {
			return fmt.Errorf("engine: no action registered for %q", b.Action)
		}
		result, invokeErr := act.Invoke(e.runID, g.stepIndex, b.Params)
		if e.hooks.CrashAfterInvoke != nil && e.hooks.CrashAfterInvoke(e.runID, g.stepIndex) {
			return ErrCrashed
		}
		if invokeErr != nil {
			bs.actionPending = true
			e.groupUncertain = append(e.groupUncertain, UncertainEffect{
				StepIndex: g.stepIndex, Token: bs.token, Attempt: bs.attempt, Key: key, Action: b.Action,
			})
			uerr := ErrUncertain
			*uncertainErr = uerr
			// 其余分支仍然打开并继续收集，故不 return，只标记本分支未核实。
			continue
		}
		if err := e.appendLocked(Event{
			Type:      EvtBranchActionAcked,
			StepIndex: g.stepIndex,
			Token:     bs.token,
			Key:       key,
			Result:    result,
			Detail:    fmt.Sprintf("attempt=%d", bs.attempt),
		}); err != nil {
			return err
		}
		if err := e.recordBranchOutcomeLocked(bs, outcomeSuccess, result); err != nil {
			return err
		}
	}
	return nil
}

func (bs *branchState) attemptResult() string {
	return fmt.Sprintf("attempt=%d", bs.attempt)
}

// armGroupTimersLocked 为所有未终态、有截止时间的分支与组挂超时定时器。
func (e *Engine) armGroupTimersLocked(g *groupState) {
	if g.hasDeadline && !g.timerArmed && !g.decided {
		g.timerArmed = true
		e.scheduleGroupTimeout(g)
	}
	for i := range g.spec.Branches {
		if c := g.children[i]; c != nil {
			e.armGroupTimersLocked(c)
			continue
		}
		bs := e.branchByToken[branchToken(g.token, i)]
		if bs != nil && bs.hasDeadline && !bs.timerArmed && !bs.terminal {
			bs.timerArmed = true
			e.scheduleBranchTimeout(bs)
		}
	}
}

func (e *Engine) scheduleGroupTimeout(g *groupState) {
	remaining := time.Until(g.deadline)
	if remaining < 0 {
		remaining = 0
	}
	timer := time.NewTimer(remaining)
	go func() {
		<-timer.C
		e.mu.Lock()
		defer e.mu.Unlock()
		if e.closed {
			return
		}
		cur, ok := e.groupByToken[g.token]
		if !ok || cur != g || g.decided {
			return
		}
		_ = e.timeoutGroupLocked(g)
	}()
}

func (e *Engine) scheduleBranchTimeout(b *branchState) {
	remaining := time.Until(b.deadline)
	if remaining < 0 {
		remaining = 0
	}
	timer := time.NewTimer(remaining)
	go func() {
		<-timer.C
		e.mu.Lock()
		defer e.mu.Unlock()
		if e.closed {
			return
		}
		cur, ok := e.branchByToken[b.token]
		if !ok || cur != b || b.terminal {
			return
		}
		_ = e.timeoutBranchLocked(b)
	}()
}

// timeoutGroupLocked：整组截止。按当时已收集结果形成唯一决策；
// 未完成分支不记终态（在不收集模式下随后随取消请求处理）。
func (e *Engine) timeoutGroupLocked(g *groupState) error {
	if g.decided {
		return nil
	}
	counted := 0
	for i := 0; i < len(g.spec.Branches); i++ {
		_, c := e.directOutcomeLocked(g, i)
		if c {
			counted++
		}
	}
	satisfied := false
	switch g.spec.Mode {
	case ModeAll:
		satisfied = false // 有分支未到终态，all 不可能满足
	case ModeAny:
		satisfied = counted >= 1
	case ModeQuorum:
		q := g.spec.Quota
		if q <= 0 {
			q = 1
		}
		satisfied = counted >= q
	}
	g.decided = true
	g.satisfied = satisfied
	g.exhausted = !satisfied
	g.selected = selectTokensLocked(e, g)
	if err := e.appendGroupDecidedLocked(g); err != nil {
		return err
	}
	if satisfied && !g.spec.CollectRemaining {
		if err := e.requestGroupCancelsLocked(g); err != nil {
			return err
		}
	}
	if g.parent != nil {
		return e.evaluateGroupLocked(g.parent)
	}
	if ch, ok := e.groupWaitCh[g.token]; ok && ch != nil {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
	return nil
}

// timeoutBranchLocked：单个分支截止，形成失败终态并参与组裁决。
func (e *Engine) timeoutBranchLocked(b *branchState) error {
	if b.terminal {
		return nil
	}
	if err := e.appendLocked(Event{
		Type:      EvtBranchTimedOut,
		StepIndex: b.parent.stepIndex,
		Token:     b.token,
		Result:    fmt.Sprintf("attempt=%d", b.attempt),
	}); err != nil {
		return err
	}
	if err := e.recordBranchOutcomeLocked(b, outcomeTimeout, ""); err != nil {
		return err
	}
	return e.evaluateGroupLocked(b.parent)
}

// completeGroupStepLocked 在根组决策后落盘 group_completed 并推进步骤。
func (e *Engine) completeGroupStepLocked(root *groupState) error {
	if !root.completed {
		root.completed = true
		kind := "exhausted"
		if root.satisfied {
			kind = "satisfied"
		}
		if err := e.appendLocked(Event{
			Type:      EvtGroupCompleted,
			StepIndex: root.stepIndex,
			Token:     root.token,
			Detail:    kind,
		}); err != nil {
			return err
		}
		if err := e.appendLocked(Event{
			Type:      EvtStepCompleted,
			StepIndex: root.stepIndex,
			Detail:    kind,
		}); err != nil {
			return err
		}
	}
	e.completed[root.stepIndex] = true
	e.step = root.stepIndex + 1
	e.currentGroup = nil
	return nil
}

// armRecoveredTimersLocked 恢复后按已确认的截止时间重新挂超时；
// 已过期的截止时间立即触发超时裁决（仍是唯一决策）。
func (e *Engine) armRecoveredTimersLocked(g *groupState, now time.Time) {
	if g.hasDeadline && !g.timerArmed && !g.decided {
		g.timerArmed = true
		if !g.deadline.After(now) {
			_ = e.timeoutGroupLocked(g)
			return
		}
		e.scheduleGroupTimeout(g)
	}
	for i := range g.spec.Branches {
		if c := g.children[i]; c != nil {
			e.armRecoveredTimersLocked(c, now)
			continue
		}
		bs := e.branchByToken[branchToken(g.token, i)]
		if bs != nil && bs.hasDeadline && !bs.timerArmed && !bs.terminal {
			bs.timerArmed = true
			if !bs.deadline.After(now) {
				_ = e.timeoutBranchLocked(bs)
				continue
			}
			e.scheduleBranchTimeout(bs)
		}
	}
}
