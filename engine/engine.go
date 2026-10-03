package engine

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

var engineGeneration atomic.Uint64

// ErrUncertain 表示流程停在一个"已调用但未确认"的外部动作上：
// 动作超时/出错不代表没有发生，引擎不会自动重试，需要人工核实后
// 调用 ConfirmAction 再继续。
var ErrUncertain = errors.New("engine: action outcome uncertain, verification required")

// ErrCrashed 由崩溃注入钩子触发，模拟进程在动作已执行、
// 确认事件落盘之前退出。
var ErrCrashed = errors.New("engine: simulated crash")

// Hooks 是测试用的故障注入点。
type Hooks struct {
	// CrashAfterInvoke 在动作 Invoke 返回后、ActionAcked 落盘前调用；
	// 返回 true 表示模拟崩溃（确认丢失）。
	CrashAfterInvoke func(runID string, stepIndex int) bool
}

// UncertainAction 描述一个结果不确定、等待核实的动作。
type UncertainAction struct {
	StepIndex int
	Key       string
	Action    string
}

type waitState struct {
	token    string
	deadline time.Time
}

type waitSignal struct {
	result   string
	canceled bool
}

type waitResolution struct {
	timedOut bool
	canceled bool
	result   string
	err      error
}

// Engine 驱动单个 run。所有状态迁移先写历史再生效，
// 因此任何时候进程退出都能从 Store 重放到同一进度。
type Engine struct {
	store *Store
	clock Clock
	hooks Hooks

	def   Definition
	runID string
	seq   int64
	gen   uint64

	mu             sync.Mutex
	step           int
	done           bool
	started        bool
	closed         bool
	running        bool
	stopCh         chan struct{}
	wait           *waitState
	waitCh         chan waitSignal
	timeoutCh      chan waitResolution
	pending        *waitState        // 恢复出的、尚未重新武装的等待
	results        map[string]string // token -> 已到达的外部结果
	canceled       map[string]bool   // token -> 已到达的取消信号
	decided        map[string]EventType
	recorded       map[string]bool
	completed      map[int]bool
	acked          map[string]bool // 已确认的动作 key
	ackedResult    map[string]string
	uncertain      *UncertainAction
	currentGroup   *groupState
	groupByToken   map[string]*groupState
	branchByToken  map[string]*branchState
	groupWaitCh    map[string]chan struct{}
	groupUncertain []UncertainEffect
}

func NewEngine(store *Store, clock Clock, hooks Hooks) *Engine {
	if clock == nil {
		clock = systemClock{}
	}
	e := &Engine{
		store:         store,
		clock:         clock,
		hooks:         hooks,
		results:       map[string]string{},
		canceled:      map[string]bool{},
		decided:       map[string]EventType{},
		recorded:      map[string]bool{},
		completed:     map[int]bool{},
		acked:         map[string]bool{},
		ackedResult:   map[string]string{},
		groupByToken:  map[string]*groupState{},
		branchByToken: map[string]*branchState{},
		groupWaitCh:   map[string]chan struct{}{},
	}
	e.stopCh = make(chan struct{})
	e.gen = engineGeneration.Add(1)
	return e
}

func (e *Engine) nowMs() int64 { return e.clock.Now().UnixMilli() }

// append 分配序号并落盘一条事件。
func (e *Engine) append(ev Event) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.appendLocked(ev)
}

func (e *Engine) appendLocked(ev Event) error {
	if e.gen == 0 {
		return errors.New("engine: closed execution context")
	}
	e.seq++
	ev.Seq = e.seq
	ev.RunID = e.runID
	ev.DefID = e.def.ID
	ev.At = e.nowMs()
	return e.store.Append(ev)
}

// Start 开启一个新 run 并同步驱动，直到完成、崩溃或停在不确定动作上。
func (e *Engine) Start(def Definition, runID string, actions map[string]Action) error {
	e.def = def
	e.runID = runID
	e.started = true
	if err := e.append(Event{Type: EvtRunStarted, Detail: def.Name}); err != nil {
		return err
	}
	return e.Run(actions)
}

// Run 从当前进度（新的或恢复的）继续驱动流程。
func (e *Engine) Run(actions map[string]Action) error {
	for {
		e.mu.Lock()
		if e.closed {
			e.mu.Unlock()
			return nil
		}
		if e.running {
			e.mu.Unlock()
			return nil
		}
		if e.done {
			e.mu.Unlock()
			return nil
		}
		if e.uncertain != nil {
			e.mu.Unlock()
			return ErrUncertain
		}
		if len(e.groupUncertain) > 0 {
			e.mu.Unlock()
			return ErrUncertain
		}
		if e.step >= len(e.def.Steps) {
			e.done = true
			e.mu.Unlock()
			return e.append(Event{Type: EvtRunCompleted})
		}
		stepIndex := e.step
		st := e.def.Steps[stepIndex]
		pending := e.pending
		e.pending = nil
		var pendingDeadline time.Time
		if pending != nil {
			pendingDeadline = pending.deadline
			pending = nil
		}
		key := fmt.Sprintf("%s:action:%d", e.runID, stepIndex)
		if st.Kind == "action" && e.acked[key] && !e.completed[stepIndex] {
			result := e.ackedResult[key]
			e.mu.Unlock()
			if err := e.append(Event{Type: EvtStepCompleted, StepIndex: stepIndex, Result: result}); err != nil {
				return err
			}
			e.mu.Lock()
			e.completed[stepIndex] = true
			e.step = stepIndex + 1
			e.mu.Unlock()
			continue
		}
		e.mu.Unlock()

		var active bool

		var err error
		switch st.Kind {
		case "action":
			err = e.runActionStep(stepIndex, st, actions)
		case "group":
			e.mu.Lock()
			e.running = true
			e.mu.Unlock()
			active = true
			err = e.runGroupStep(stepIndex, st, actions)
		case "wait":
			e.mu.Lock()
			token := fmt.Sprintf("%s:wait:%d", e.runID, stepIndex)
			e.running = true
			_, hasResult := e.results[token]
			hasInput := hasResult || e.canceled[token] || e.decided[token] != ""
			e.mu.Unlock()
			active = true
			if hasInput {
				deadline := pendingDeadline
				if deadline.IsZero() {
					deadline = e.clock.Now()
				}
				err = e.await(token, stepIndex, deadline)
			} else if pendingDeadline.IsZero() {
				err = e.runWaitStep(stepIndex, st)
			} else {
				err = e.await(token, stepIndex, pendingDeadline)
			}
		default:
			err = fmt.Errorf("engine: unknown step kind %q", st.Kind)
		}
		if active {
			e.mu.Lock()
			e.running = false
			e.mu.Unlock()
		}
		if err != nil {
			return err
		}
	}
}

func (e *Engine) runActionStep(stepIndex int, st Step, actions map[string]Action) error {
	key := fmt.Sprintf("%s:action:%d", e.runID, stepIndex)
	if err := e.append(Event{Type: EvtStepStarted, StepIndex: stepIndex, Detail: st.Name}); err != nil {
		return err
	}
	if err := e.append(Event{Type: EvtActionInvoked, StepIndex: stepIndex, Key: key, Detail: st.Action}); err != nil {
		return err
	}

	act, ok := actions[st.Action]
	if !ok {
		return fmt.Errorf("engine: no action registered for %q", st.Action)
	}
	result, invokeErr := act.Invoke(e.runID, stepIndex, st.Params)

	// 崩溃注入：副作用可能已发生，但确认永远丢失。
	if e.hooks.CrashAfterInvoke != nil && e.hooks.CrashAfterInvoke(e.runID, stepIndex) {
		return ErrCrashed
	}

	if invokeErr != nil {
		// 超时/失败不代表没发生：保留不确定状态，绝不自动重试。
		e.mu.Lock()
		e.uncertain = &UncertainAction{StepIndex: stepIndex, Key: key, Action: st.Action}
		e.mu.Unlock()
		return ErrUncertain
	}
	return e.completeAction(stepIndex, key, result)
}

func (e *Engine) completeAction(stepIndex int, key, result string) error {
	if err := e.append(Event{Type: EvtActionAcked, StepIndex: stepIndex, Key: key, Result: result}); err != nil {
		return err
	}
	if err := e.append(Event{Type: EvtStepCompleted, StepIndex: stepIndex, Result: result}); err != nil {
		return err
	}
	e.mu.Lock()
	e.acked[key] = true
	e.ackedResult[key] = result
	e.completed[stepIndex] = true
	e.step = stepIndex + 1
	e.mu.Unlock()
	return nil
}

// ConfirmAction 在人工核实外部动作确实发生后补记确认。
// 重复确认只生效一次（按 key 去重），返回是否真正补记。
func (e *Engine) ConfirmAction(stepIndex int, result string) (bool, error) {
	key := fmt.Sprintf("%s:action:%d", e.runID, stepIndex)
	e.mu.Lock()
	if e.acked[key] {
		e.mu.Unlock()
		return false, nil // 重复确认：同一动作只影响一次
	}
	uncertain := e.uncertain
	e.mu.Unlock()
	if uncertain == nil || uncertain.Key != key {
		return false, fmt.Errorf("engine: step %d is not awaiting confirmation", stepIndex)
	}
	if err := e.completeAction(stepIndex, key, result); err != nil {
		return false, err
	}
	e.mu.Lock()
	e.uncertain = nil
	e.mu.Unlock()
	return true, nil
}

// UncertainActions 返回当前所有已调用但结果未确认的外部动作。
func (e *Engine) UncertainActions() []UncertainAction {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.uncertain == nil {
		return nil
	}
	return []UncertainAction{*e.uncertain}
}

func (e *Engine) runWaitStep(stepIndex int, st Step) error {
	token := fmt.Sprintf("%s:wait:%d", e.runID, stepIndex)
	deadline := e.clock.Now().Add(time.Duration(st.TimeoutMS) * time.Millisecond)
	if err := e.append(Event{Type: EvtStepStarted, StepIndex: stepIndex, Detail: st.Name}); err != nil {
		return err
	}
	if err := e.append(Event{Type: EvtWaitScheduled, StepIndex: stepIndex, Token: token, Deadline: deadline.UnixMilli()}); err != nil {
		return err
	}
	return e.await(token, stepIndex, deadline)
}

// await 等待外部结果、取消或超时。状态迁移与历史写入在同一临界区完成，
// 因此保存点只能落在完整、可解释的状态边界上。
func (e *Engine) await(token string, stepIndex int, deadline time.Time) error {
	e.mu.Lock()
	if decision, ok := e.decided[token]; ok {
		resolution := waitResolution{result: e.results[token]}
		if decision == EvtWaitTimedOut {
			resolution.timedOut = true
		}
		if decision == EvtWaitCanceled {
			resolution.canceled = true
		}
		delete(e.results, token)
		err := e.finishWaitLocked(stepIndex, token, resolution)
		e.mu.Unlock()
		return err
	}
	if e.canceled[token] {
		err := e.finishWaitLocked(stepIndex, token, waitResolution{canceled: true})
		e.mu.Unlock()
		return err
	}
	if result, ok := e.results[token]; ok {
		err := e.finishWaitLocked(stepIndex, token, waitResolution{result: result})
		e.mu.Unlock()
		return err
	}
	ch := make(chan waitSignal, 1)
	timeoutCh := make(chan waitResolution, 1)
	e.wait = &waitState{token: token, deadline: deadline}
	e.waitCh = ch
	e.timeoutCh = timeoutCh
	e.mu.Unlock()

	remaining := time.Until(deadline)
	if remaining < 0 {
		remaining = 0
	}
	timer := time.NewTimer(remaining)
	defer timer.Stop()
	go func() {
		<-timer.C
		e.mu.Lock()
		defer e.mu.Unlock()
		if e.closed {
			return
		}
		if e.wait == nil || e.wait.token != token {
			return
		}
		ch := e.timeoutCh
		resolution := waitResolution{timedOut: true}
		resolution.err = e.recordTimeoutDecisionLocked(stepIndex, token, resolution)
		e.clearWaitLocked(token)
		if ch != nil {
			ch <- resolution
		}
	}()

	select {
	case sig := <-ch:
		e.mu.Lock()
		defer e.mu.Unlock()
		e.clearWaitLocked(token)
		return e.recordExternalDecisionLocked(stepIndex, token, sig)
	case res := <-timeoutCh:
		e.mu.Lock()
		defer e.mu.Unlock()
		e.clearWaitLocked(token)
		if res.err != nil {
			return res.err
		}
		return e.recordTimeoutDecisionLocked(stepIndex, token, res)
	case <-e.stopCh:
		return nil
	}
}

// Close 标记本进程实例不再等待。持久历史不会被修改；之后可由 Recover 重开。
func (e *Engine) Close() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return
	}
	e.closed = true
	e.gen = 0
	e.wait = nil
	e.waitCh = nil
	e.timeoutCh = nil
	close(e.stopCh)
}

func (e *Engine) clearWaitLocked(token string) {
	if e.wait != nil && e.wait.token == token {
		e.wait = nil
		e.waitCh = nil
		e.timeoutCh = nil
	}
}

func (e *Engine) finishWaitLocked(stepIndex int, token string, resolution waitResolution) error {
	if _, ok := e.decided[token]; !ok {
		if resolution.timedOut {
			e.decided[token] = EvtWaitTimedOut
		} else if resolution.canceled {
			e.decided[token] = EvtWaitCanceled
		} else {
			e.decided[token] = EvtWaitFired
			e.results[token] = resolution.result
		}
	}
	return e.completeDecidedWaitLocked(stepIndex, token)
}

func (e *Engine) completeDecidedWaitLocked(stepIndex int, token string) error {
	if e.completed[stepIndex] {
		e.step = stepIndex + 1
		return nil
	}
	if e.recorded[token] {
		detail := ""
		if e.decided[token] == EvtWaitTimedOut {
			detail = "timeout"
		}
		if e.decided[token] == EvtWaitCanceled {
			detail = "canceled"
		}
		result := e.results[token]
		if err := e.appendLocked(Event{Type: EvtStepCompleted, StepIndex: stepIndex, Result: result, Detail: detail}); err != nil {
			return err
		}
		e.completed[stepIndex] = true
		e.step = stepIndex + 1
		return nil
	}
	e.recorded[token] = true
	switch e.decided[token] {
	case EvtWaitTimedOut:
		if err := e.appendLocked(Event{Type: EvtWaitTimedOut, StepIndex: stepIndex, Token: token}); err != nil {
			return err
		}
		if err := e.appendLocked(Event{Type: EvtStepCompleted, StepIndex: stepIndex, Detail: "timeout"}); err != nil {
			return err
		}
	case EvtWaitCanceled:
		if err := e.appendLocked(Event{Type: EvtWaitCanceled, StepIndex: stepIndex, Token: token}); err != nil {
			return err
		}
		if err := e.appendLocked(Event{Type: EvtStepCompleted, StepIndex: stepIndex, Detail: "canceled"}); err != nil {
			return err
		}
	default:
		result := e.results[token]
		if err := e.appendLocked(Event{Type: EvtWaitFired, StepIndex: stepIndex, Token: token, Result: result}); err != nil {
			return err
		}
		if err := e.appendLocked(Event{Type: EvtStepCompleted, StepIndex: stepIndex, Result: result}); err != nil {
			return err
		}
	}
	e.completed[stepIndex] = true
	e.step = stepIndex + 1
	return nil
}

func (e *Engine) recordExternalDecisionLocked(stepIndex int, token string, signal waitSignal) error {
	if _, ok := e.decided[token]; ok {
		e.step = stepIndex + 1
		return nil
	}
	if signal.canceled {
		e.canceled[token] = true
		e.decided[token] = EvtWaitCanceled
	} else {
		e.results[token] = signal.result
		e.decided[token] = EvtWaitFired
	}
	return e.completeDecidedWaitLocked(stepIndex, token)
}

func (e *Engine) recordTimeoutDecisionLocked(stepIndex int, token string, _ waitResolution) error {
	if _, ok := e.decided[token]; ok {
		e.step = stepIndex + 1
		return nil
	}
	e.decided[token] = EvtWaitTimedOut
	return e.completeDecidedWaitLocked(stepIndex, token)
}

// DeliverResult 投递一个外部结果。历史先持久化，然后唤醒等待方。
func (e *Engine) DeliverResult(token, result string) error {
	return e.deliverExternal(token, result, false)
}

// CancelWait 投递外部取消信号。它与结果共用同一先到先得裁决。
func (e *Engine) CancelWait(token, reason string) error {
	return e.deliverExternal(token, reason, true)
}

func (e *Engine) deliverExternal(token, value string, canceled bool) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	duplicate := (!canceled && e.results[token] != "") || (canceled && e.canceled[token])
	if duplicate {
		detail := "result"
		if canceled {
			detail = "cancel"
		}
		return e.appendLocked(Event{Type: EvtExternalDuplicate, Token: token, Result: value, Detail: detail})
	}

	waiting := e.wait != nil && e.wait.token == token
	ch := e.waitCh
	evType := EvtExternalResult
	if canceled {
		evType = EvtExternalCancel
		e.canceled[token] = true
	} else {
		e.results[token] = value
	}
	detail := ""
	if !waiting {
		detail = "late"
	}
	if err := e.appendLocked(Event{Type: evType, Token: token, Result: value, Detail: detail}); err != nil {
		return err
	}
	if waiting && ch != nil {
		signal := waitSignal{result: value, canceled: canceled}
		if err := e.recordExternalDecisionLocked(e.waitStepIndexLocked(token), token, signal); err != nil {
			return err
		}
		e.clearWaitLocked(token)
		ch <- signal
	}
	return nil
}

func (e *Engine) waitStepIndexLocked(token string) int {
	for i := range e.def.Steps {
		if e.def.Steps[i].Kind == "wait" && fmt.Sprintf("%s:wait:%d", e.runID, i) == token {
			return i
		}
	}
	return -1
}

// Recover 从落盘历史重建执行器，恢复到退出前的同一进度。
func Recover(store *Store, clock Clock, def Definition, runID string, hooks Hooks) (*Engine, error) {
	e := NewEngine(store, clock, hooks)
	e.def = def
	e.runID = runID

	events, err := store.Load(runID)
	if err != nil {
		return nil, err
	}
	stepDone := map[int]bool{}
	waitScheduled := map[string]int64{} // token -> deadline_ms
	waitDecision := map[string]EventType{}
	invoked := map[int]string{} // stepIndex -> key
	groupTree := map[int]*groupState{}
	branchInvokedUnacked := map[string]bool{}
	for _, ev := range events {
		if ev.Seq > e.seq {
			e.seq = ev.Seq
		}
		switch ev.Type {
		case EvtRunStarted:
			e.started = true
		case EvtActionInvoked:
			invoked[ev.StepIndex] = ev.Key
		case EvtActionAcked:
			e.acked[ev.Key] = true
			e.ackedResult[ev.Key] = ev.Result
			delete(invoked, ev.StepIndex)
		case EvtGroupOpened:
			g, ok := groupTree[ev.StepIndex]
			if !ok {
				g = buildGroupTree(e.runID, ev.StepIndex, def.Steps[ev.StepIndex].Group)
				groupTree[ev.StepIndex] = g
				e.registerGroupLocked(g)
			}
			if target := findGroupByToken(g, ev.Token); target != nil {
				target.opened = true
				target.deadline = time.UnixMilli(ev.Deadline)
				target.hasDeadline = ev.Deadline != 0
			}
		case EvtBranchScheduled:
			if b := e.branchByToken[ev.Token]; b != nil {
				b.scheduled = true
				if attempt := parseAttempt(ev.Result); attempt > 0 {
					b.attempt = attempt
				}
				b.deadline = time.UnixMilli(ev.Deadline)
				b.hasDeadline = ev.Deadline > 0
			}
		case EvtBranchActionInvoked:
			branchInvokedUnacked[ev.Key] = true
			if b := e.branchByToken[ev.Token]; b != nil {
				b.actionKey = ev.Key
				if a := parseAttempt(ev.Detail); a > 0 {
					b.attempt = a
				}
			}
		case EvtBranchActionAcked:
			delete(branchInvokedUnacked, ev.Key)
			if b := e.branchByToken[ev.Token]; b != nil {
				if a := parseAttempt(ev.Detail); a > 0 && a == b.attempt {
					b.actionPending = false
				}
			}
		case EvtBranchRetried:
			if b := e.branchByToken[ev.Token]; b != nil {
				if a := parseAttempt(ev.Result); a > 0 {
					b.attempt = a
				}
				b.terminal = false
				b.outcome = nil
				b.cancelAsked = false
			}
		case EvtExternalResult:
			if b := e.branchByToken[ev.Token]; b != nil {
				restoreBranchTerminal(b, outcomeSuccess, ev.Result, ev.Detail == "late")
			} else {
				e.results[ev.Token] = ev.Result
			}
		case EvtBranchFailure:
			if b := e.branchByToken[ev.Token]; b != nil {
				restoreBranchTerminal(b, outcomeFailure, ev.Result, ev.Detail == "late")
			}
		case EvtExternalCancel:
			if b := e.branchByToken[ev.Token]; b != nil {
				restoreBranchTerminal(b, outcomeCancel, ev.Result, ev.Detail == "late")
			} else {
				e.canceled[ev.Token] = true
			}
		case EvtBranchTimedOut:
			if b := e.branchByToken[ev.Token]; b != nil {
				restoreBranchTerminal(b, outcomeTimeout, "", false)
			}
		case EvtGroupCancelRequested:
			if b := e.branchByToken[ev.Token]; b != nil {
				b.cancelAsked = true
			}
		case EvtGroupDecided:
			if g := findGroupByToken(groupTree[ev.StepIndex], ev.Token); g != nil {
				g.decided = true
				g.satisfied = ev.Detail == "satisfied"
				g.exhausted = !g.satisfied
				g.selected = parseSelected(ev.Extra)
				if !g.satisfied && !g.spec.CollectRemaining {
					g.cancelSent = true
				}
			}
		case EvtGroupCompleted:
			if g := groupTree[ev.StepIndex]; g != nil && g.token == ev.Token {
				g.completed = true
			}
		case EvtWaitScheduled:
			waitScheduled[ev.Token] = ev.Deadline
		case EvtWaitFired:
			waitDecision[ev.Token] = ev.Type
			e.recorded[ev.Token] = true
			delete(e.results, ev.Token)
		case EvtWaitTimedOut:
			waitDecision[ev.Token] = ev.Type
			e.recorded[ev.Token] = true
		case EvtWaitCanceled:
			waitDecision[ev.Token] = ev.Type
			e.recorded[ev.Token] = true
		case EvtStepCompleted:
			stepDone[ev.StepIndex] = true
		case EvtRunCompleted:
			e.done = true
		}
	}
	// 下一个未完成的步骤。
	for e.step < len(def.Steps) && stepDone[e.step] {
		e.step++
	}
	for stepIndex := range stepDone {
		e.completed[stepIndex] = true
	}
	if !e.done && e.step < len(def.Steps) {
		st := def.Steps[e.step]
		if key, ok := invoked[e.step]; ok && st.Kind == "action" {
			// 已调用未确认：不确定，禁止重试，等待核实。
			e.uncertain = &UncertainAction{StepIndex: e.step, Key: key, Action: st.Action}
		}
		if st.Kind == "wait" {
			token := fmt.Sprintf("%s:wait:%d", e.runID, e.step)
			if decision, ok := waitDecision[token]; ok {
				e.decided[token] = decision
			} else {
				if _, resultArrived := e.results[token]; !resultArrived && !e.canceled[token] {
					if dl, ok := waitScheduled[token]; ok {
						e.pending = &waitState{token: token, deadline: time.UnixMilli(dl)}
					}
				}
			}
		}
		if st.Kind == "group" {
			if g, ok := groupTree[e.step]; ok {
				e.currentGroup = g
				// 已触发未确认的分支 action：单独保留为未核实外部效果。
				for _, b := range g.allLeaves(e) {
					key := branchActionKey(b.token, b.attempt)
					if branchInvokedUnacked[key] {
						b.actionPending = true
						e.groupUncertain = append(e.groupUncertain, UncertainEffect{
							StepIndex: e.step, Token: b.token, Attempt: b.attempt, Key: key, Action: b.spec.Action,
						})
					}
				}
			}
		}
	}
	return e, nil
}

// Pending 用人类可读的条目说明当前还有什么待处理。
func (e *Engine) Pending() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	var out []string
	if e.done {
		return []string{"run completed, nothing pending"}
	}
	if e.uncertain != nil {
		out = append(out, fmt.Sprintf(
			"step %d: action %q invoked but unconfirmed (key %s) — verify externally, then ConfirmAction; it will NOT be retried automatically",
			e.uncertain.StepIndex, e.uncertain.Action, e.uncertain.Key))
		return out
	}
	for _, u := range e.groupUncertain {
		out = append(out, fmt.Sprintf(
			"group step %d: branch %s action %q invoked but unconfirmed (key %s) — retained as an unverified external effect; verify externally, then ConfirmBranchAction; it will NOT vanish or be retried",
			u.StepIndex, u.Token, u.Action, u.Key))
	}
	if e.currentGroup != nil {
		root := e.currentGroup
		st := e.groupStatusLocked(root)
		if st != nil {
			for _, b := range st.Branches {
				if b.Terminal {
					in := "not in decision"
					if b.InDecision {
						in = "participates in decision"
					}
					out = append(out, fmt.Sprintf("group %s branch %s: %s (%s)%s", st.Token, b.Name, b.Outcome, b.Result, "; "+in))
				} else {
					note := "pending"
					if b.CancelAsked {
						note = "cancel requested"
					}
					if b.Unverified {
						note = "unverified external effect"
					}
					out = append(out, fmt.Sprintf("group %s branch %s: %s", st.Token, b.Name, note))
				}
			}
			if st.Decided {
				out = append(out, fmt.Sprintf("group %s decided (satisfied=%t) with %d selected branches; failures counted=%t",
					st.Token, st.Satisfied, len(st.SelectedTokens), st.CountFailures))
			}
		}
	}
	if e.step < len(e.def.Steps) {
		st := e.def.Steps[e.step]
		out = append(out, fmt.Sprintf("step %d (%s %q): next to execute", e.step, st.Kind, st.Name))
	}
	for token := range e.results {
		out = append(out, fmt.Sprintf("external result for token %s arrived, not yet consumed", token))
	}
	return out
}

// RunID 返回当前 run 的 ID。
func (e *Engine) RunID() string { return e.runID }
