package engine

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
)

// 动作执行状态：副作用与本地确认之间可能崩溃，因此需要区分
// “尚未发出”与“可能已经发出、结果未知”。
const (
	ActionStateUncertain = "uncertain" // 已调用外部，但未确认结果
	ActionStateDone      = "done"      // 结果已确认并落盘
)

// 等待状态：超时不代表结果不会再到达，超时后等待仍保持未决，
// 直到结果到达才完成；结果先到则超时作废。
const (
	WaitStateWaiting  = "waiting"
	WaitStateTimedOut = "timed_out"
	WaitStateFired    = "fired"
)

var (
	ErrWaitPending   = errors.New("engine: wait is still pending")
	ErrActionUnknown = errors.New("engine: action result unknown, verify required")
	ErrUnknownToken  = errors.New("engine: unknown wait token")
)

// CrashHook 在某条事件落盘后被调用，返回 true 时模拟进程立刻退出
// （panic(Crash)）。事件已经持久化，因此重建引擎后不会丢进度。
var Crash = errors.New("engine: simulated crash after durable event")

type CrashHook func(evt Event) bool

type actionState struct {
	token  string
	state  string
	result string
}

type waitState struct {
	token    string
	deadline int64
	state    string
	result   string
}

type stepState struct {
	started bool
	done    bool
	action  *actionState
	wait    *waitState
}

// PendingItem 描述一个尚未结束的等待：超时后它仍然可能收到结果。
type PendingItem struct {
	StepIndex int    `json:"step_index"`
	Name      string `json:"name"`
	Token     string `json:"token"`
	Deadline  int64  `json:"deadline_ms"`
	TimedOut  bool   `json:"timed_out"`
}

// UncertainItem 描述一个“已发出但结果未知”的外部动作，
// 必须先核实，不能直接重新执行。
type UncertainItem struct {
	StepIndex int    `json:"step_index"`
	Name      string `json:"name"`
	Action    string `json:"action"`
	Token     string `json:"token"`
}

// Status 是执行器当前进度的可读快照，完全由历史重放得到。
type Status struct {
	RunID            string          `json:"run_id"`
	Done             bool            `json:"done"`
	CompletedSteps   int             `json:"completed_steps"`
	CurrentStep      int             `json:"current_step"`
	PendingWaits     []PendingItem   `json:"pending_waits"`
	UncertainActions []UncertainItem `json:"uncertain_actions"`
}

// Engine 是单流程运行的执行器。它本身不做任何内存检查点：
// 所有状态都从 Store 重放得到，重建 Engine 即恢复。
type Engine struct {
	def   Definition
	store Store
	clock Clock

	runID string
	seq   int64
	steps []stepState
	done  bool

	pending map[string]int    // wait token -> step index
	actions map[string]int    // action token -> step index
	tokens  map[string]string // token -> wait/action，用于识别对已完成等待的重复投递
	hook    CrashHook
}

// NewEngine 创建执行器：如果 Store 中已有该流程的历史，则重放恢复。
// 一个 Store 文件对应一次 RunID；首次运行时 runID 自动生成。
func NewEngine(def Definition, store Store, clock Clock) (*Engine, error) {
	if clock == nil {
		clock = systemClock{}
	}
	e := &Engine{
		def:     def,
		store:   store,
		clock:   clock,
		steps:   make([]stepState, len(def.Steps)),
		pending: make(map[string]int),
		actions: make(map[string]int),
		tokens:  make(map[string]string),
	}
	events, err := store.Read()
	if err != nil {
		return nil, err
	}
	for _, evt := range events {
		if err := e.apply(evt); err != nil {
			return nil, fmt.Errorf("replay event seq=%d: %w", evt.Seq, err)
		}
		e.seq = evt.Seq
	}
	if e.runID == "" {
		e.runID = newToken("run")
		if err := e.record(Event{
			Type:  EvtRunStarted,
			DefID: def.ID,
		}); err != nil {
			return nil, err
		}
	}
	return e, nil
}

func (e *Engine) SetCrashHook(h CrashHook) { e.hook = h }

func (e *Engine) RunID() string { return e.runID }

func newToken(prefix string) string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return prefix + "_" + hex.EncodeToString(b[:])
}

func (e *Engine) nowMS() int64 { return e.clock.Now().UnixMilli() }

func (e *Engine) record(evt Event) error {
	e.seq++
	evt.Seq = e.seq
	evt.RunID = e.runID
	evt.At = e.nowMS()
	if err := e.store.Append(evt); err != nil {
		return err
	}
	return e.apply(evt)
}

// apply 把一条历史事件并入内存投影。新记录与重放走同一条路径。
func (e *Engine) apply(evt Event) error {
	if e.runID == "" {
		e.runID = evt.RunID
	}
	stepAt := func() *stepState {
		if evt.StepIndex < 0 || evt.StepIndex >= len(e.steps) {
			return nil
		}
		return &e.steps[evt.StepIndex]
	}
	s := stepAt()
	switch evt.Type {
	case EvtRunStarted:
		// 仅用于建立 runID。
	case EvtRunCompleted:
		e.done = true
	case EvtStepStarted:
		if s != nil {
			s.started = true
		}
	case EvtStepCompleted:
		if s != nil {
			s.done = true
		}
		delete(e.pending, evt.Token)
	case EvtActionInvoked:
		if s != nil {
			s.action = &actionState{token: evt.Token, state: ActionStateUncertain}
		}
		e.actions[evt.Token] = evt.StepIndex
		e.tokens[evt.Token] = "action"
	case EvtActionAcked:
		if s != nil {
			if s.action == nil {
				s.action = &actionState{token: evt.Token}
			}
			s.action.state = ActionStateDone
			s.action.result = evt.Result
		}
	case EvtWaitScheduled:
		if s != nil {
			s.wait = &waitState{token: evt.Token, deadline: evt.Deadline, state: WaitStateWaiting}
		}
		e.pending[evt.Token] = evt.StepIndex
		e.tokens[evt.Token] = "wait"
	case EvtWaitTimedOut:
		if s != nil && s.wait != nil && s.wait.state == WaitStateWaiting {
			s.wait.state = WaitStateTimedOut
		}
	case EvtWaitFired:
		if s != nil && s.wait != nil {
			s.wait.state = WaitStateFired
			s.wait.result = evt.Result
		}
		delete(e.pending, evt.Token)
	case EvtExternalResult:
		// 外部核实确认副作用并未发生：清除不确定标记，允许重新执行一次。
		if evt.Detail == detailVerifiedNotOccurred {
			if s != nil {
				s.action = nil
			}
			delete(e.actions, evt.Token)
		}
		// 其余情况由 wait_fired / action_acked 改变状态，本事件仅审计留痕。
	case EvtExternalDuplicate:
		// 重复确认仅留痕，不产生任何状态变化。
	}
	if e.hook != nil && e.hook(evt) {
		panic(Crash)
	}
	return nil
}
