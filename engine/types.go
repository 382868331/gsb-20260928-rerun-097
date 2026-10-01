package engine

import "time"

// Definition 是流程定义。定义与运行历史分开存储：
// 定义是不可变的声明数据，历史是只追加的运行事实。
type Definition struct {
	ID    int    `json:"id"`
	Name  string `json:"name"`
	Steps []Step `json:"steps"`
}

type Step struct {
	// Kind: action / wait
	Kind      string            `json:"kind"`
	Name      string            `json:"name"`
	Action    string            `json:"action,omitempty"`
	Params    map[string]string `json:"params,omitempty"`
	TimeoutMS int64             `json:"timeout_ms,omitempty"`
}

// EventType 是历史事件类型。
type EventType string

const (
	EvtRunStarted        EventType = "run_started"
	EvtStepStarted       EventType = "step_started"
	EvtWaitScheduled     EventType = "wait_scheduled"
	EvtExternalResult    EventType = "external_result"
	EvtExternalDuplicate EventType = "external_result_duplicate"
	EvtWaitTimedOut      EventType = "wait_timed_out"
	EvtWaitCancelled     EventType = "wait_cancelled"
	EvtWaitFired         EventType = "wait_fired"
	EvtActionInvoked     EventType = "action_invoked"
	EvtActionAcked       EventType = "action_acked"
	EvtActionRetried     EventType = "action_retried"
	EvtStepCompleted     EventType = "step_completed"
	EvtRunCompleted      EventType = "run_completed"
)

// Event 是历史日志中的一条不可变记录。
type Event struct {
	Seq       int64                  `json:"seq"`
	RunID     string                 `json:"run_id"`
	DefID     int                    `json:"def_id"`
	Type      EventType              `json:"type"`
	StepIndex int                    `json:"step_index,omitempty"`
	Token     string                 `json:"token,omitempty"`
	Key       string                 `json:"key,omitempty"`
	Result    string                 `json:"result,omitempty"`
	Deadline  int64                  `json:"deadline_ms,omitempty"`
	Detail    string                 `json:"detail,omitempty"`
	At        int64                  `json:"at_ms"`
	Extra     map[string]interface{} `json:"extra,omitempty"`
}

// Action 是外部副作用的本地模拟。
type Action interface {
	// Invoke 执行副作用，返回结果。真实环境中可能成功、失败，
	// 或在返回后、本地确认前崩溃 —— 见 Engine 的崩溃注入钩子。
	Invoke(runID string, stepIndex int, params map[string]string) (result string, err error)
	Name() string
}

// Clock 抽象时间，便于测试超时与恢复顺序。
type Clock interface {
	Now() time.Time
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

type fakeClock struct{ t time.Time }

func (c *fakeClock) Now() time.Time { return c.t }
