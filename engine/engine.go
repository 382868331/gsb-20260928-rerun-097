package engine

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

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

// Engine 驱动单个 run。所有状态迁移先写历史再生效，
// 因此任何时候进程退出都能从 Store 重放到同一进度。
type Engine struct {
	store *Store
	clock Clock
	hooks Hooks

	def   Definition
	runID string
	seq   int64

	mu        sync.Mutex
	step      int
	done      bool
	started   bool
	wait      *waitState
	waitCh    chan string
	pending   *waitState        // 恢复出的、尚未重新武装的等待
	results   map[string]string // token -> 已到达的外部结果
	seen      map[string]bool   // 投递过的 token（含已消费），用于去重
	acked     map[string]bool   // 已确认的动作 key
	uncertain *UncertainAction
}

func NewEngine(store *Store, clock Clock, hooks Hooks) *Engine {
	if clock == nil {
		clock = systemClock{}
	}
	return &Engine{
		store:   store,
		clock:   clock,
		hooks:   hooks,
		results: map[string]string{},
		seen:    map[string]bool{},
		acked:   map[string]bool{},
	}
}

func (e *Engine) nowMs() int64 { return e.clock.Now().UnixMilli() }

// append 分配序号并落盘一条事件。
func (e *Engine) append(ev Event) error {
	e.mu.Lock()
	e.seq++
	ev.Seq = e.seq
	e.mu.Unlock()
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
		if e.done {
			e.mu.Unlock()
			return nil
		}
		if e.uncertain != nil {
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
		e.mu.Unlock()

		var err error
		switch st.Kind {
		case "action":
			err = e.runActionStep(stepIndex, st, actions)
		case "wait":
			if pending != nil {
				// 恢复场景：沿用原 token 与截止时间继续等。
				err = e.await(pending.token, stepIndex, pending.deadline)
			} else {
				err = e.runWaitStep(stepIndex, st)
			}
		default:
			err = fmt.Errorf("engine: unknown step kind %q", st.Kind)
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

// await 等待外部结果或超时，先到先得；两者同时到达时按实际
// 被处理的顺序落盘，后到的事件（如迟到结果）仍会补记。
func (e *Engine) await(token string, stepIndex int, deadline time.Time) error {
	e.mu.Lock()
	if res, ok := e.results[token]; ok {
		// 结果在调度前已到达（恢复场景）。
		delete(e.results, token)
		e.mu.Unlock()
		return e.finishWait(stepIndex, token, res, false)
	}
	ch := make(chan string, 1)
	e.wait = &waitState{token: token, deadline: deadline}
	e.waitCh = ch
	e.mu.Unlock()

	remaining := time.Until(deadline)
	if remaining < 0 {
		remaining = 0
	}
	timer := time.NewTimer(remaining)
	defer timer.Stop()

	select {
	case res := <-ch:
		return e.finishWait(stepIndex, token, res, false)
	case <-timer.C:
		return e.finishWait(stepIndex, token, "", true)
	}
}

func (e *Engine) finishWait(stepIndex int, token, result string, timedOut bool) error {
	e.mu.Lock()
	if e.wait != nil && e.wait.token == token {
		e.wait = nil
		e.waitCh = nil
	}
	e.mu.Unlock()

	if timedOut {
		if err := e.append(Event{Type: EvtWaitTimedOut, StepIndex: stepIndex, Token: token}); err != nil {
			return err
		}
		if err := e.append(Event{Type: EvtStepCompleted, StepIndex: stepIndex, Detail: "timeout"}); err != nil {
			return err
		}
	} else {
		if err := e.append(Event{Type: EvtWaitFired, StepIndex: stepIndex, Token: token, Result: result}); err != nil {
			return err
		}
		if err := e.append(Event{Type: EvtStepCompleted, StepIndex: stepIndex, Result: result}); err != nil {
			return err
		}
	}
	e.mu.Lock()
	e.step = stepIndex + 1
	e.mu.Unlock()
	return nil
}

// DeliverResult 投递一个外部结果。按实际到达顺序落盘：
// 等待中的 token 正常生效；重复投递记 duplicate 且只影响一次；
// 等待已结束（如已超时）时记为迟到结果，不改变已定的步骤结果。
func (e *Engine) DeliverResult(token, result string) error {
	e.mu.Lock()
	if e.seen[token] {
		e.mu.Unlock()
		return e.append(Event{Type: EvtExternalDuplicate, Token: token, Result: result})
	}
	e.seen[token] = true
	e.results[token] = result
	waiting := e.wait != nil && e.wait.token == token
	ch := e.waitCh
	e.mu.Unlock()

	detail := ""
	if !waiting {
		detail = "late" // 没有对应等待：迟到或超前到达
	}
	if err := e.append(Event{Type: EvtExternalResult, Token: token, Result: result, Detail: detail}); err != nil {
		return err
	}
	if waiting && ch != nil {
		e.mu.Lock()
		delete(e.results, token) // 由等待方消费
		e.mu.Unlock()
		ch <- result
	}
	return nil
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
	waitResolved := map[string]bool{}
	waitScheduled := map[string]int64{} // token -> deadline_ms
	invoked := map[int]string{}         // stepIndex -> key
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
			delete(invoked, ev.StepIndex)
		case EvtExternalResult:
			e.seen[ev.Token] = true
			e.results[ev.Token] = ev.Result
		case EvtWaitScheduled:
			waitScheduled[ev.Token] = ev.Deadline
		case EvtWaitFired, EvtWaitTimedOut:
			waitResolved[ev.Token] = true
			delete(e.results, ev.Token)
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
	if !e.done && e.step < len(def.Steps) {
		st := def.Steps[e.step]
		if key, ok := invoked[e.step]; ok && st.Kind == "action" {
			// 已调用未确认：不确定，禁止重试，等待核实。
			e.uncertain = &UncertainAction{StepIndex: e.step, Key: key, Action: st.Action}
		}
		if st.Kind == "wait" {
			token := fmt.Sprintf("%s:wait:%d", e.runID, e.step)
			if dl, ok := waitScheduled[token]; ok && !waitResolved[token] {
				e.pending = &waitState{token: token, deadline: time.UnixMilli(dl)}
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
