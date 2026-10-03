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
	// Kind: action / wait / group
	Kind   string `json:"kind"`
	Name   string `json:"name"`
	Action string `json:"action,omitempty"`
	// Params is kept for backwards-compatible plain action steps.
	Params    map[string]string `json:"params,omitempty"`
	TimeoutMS int64             `json:"timeout_ms,omitempty"`
	// Group 仅在 Kind == "group" 时使用：声明一个并行等待组。
	Group WaitGroup `json:"group,omitempty"`
}

// WaitMode 是等待组的满足方式。
type WaitMode string

const (
	// ModeAll：所有参与分支都给出终态后才满足（失败是否算数由 CountFailures 决定）。
	ModeAll WaitMode = "all"
	// ModeAny：任意一个分支给出满足条件的终态即满足。
	ModeAny WaitMode = "any"
	// ModeQuorum：达到 Quota 个满足条件的有效结果后满足。
	ModeQuorum WaitMode = "quorum"
)

// WaitGroup 声明一组并行等待的分支及其继续条件。
//
// 每个分支要么是一个外部等待（外部通过 Deliver*/Cancel* 投递），
// 要么在开始等待前先触发一个外部 action；action 失败同样使分支进入
// "已触发但未核实" 的保留状态，需要人工核实，绝不自动消失。
//
// 分支可以嵌套子组：子组作为父组的一个分支，子组先形成自己唯一的
// 决策，再把该决策上报给父组。
type WaitGroup struct {
	// Mode: all / any / quorum。
	Mode WaitMode `json:"mode"`
	// Quota 仅 Mode == quorum 时有意义：需要多少个满足条件的终态。
	Quota int `json:"quota,omitempty"`
	// CountFailures 声明失败/取消/超时结果是否计入本组的满足数量。
	// 为 false 时只有成功结果算数；失败仍展示，但不推进条件。
	CountFailures bool `json:"count_failures,omitempty"`
	// TimeoutMS 是整组的截止时间；超时按当时已收集的结果做唯一决策。
	TimeoutMS int64 `json:"timeout_ms"`
	// CollectRemaining 为 true 时，组决策后继续收集剩余分支的迟到结果；
	// 为 false 时，决策后对尚未完成的分支发出取消请求。
	// 无论哪种模式，已产生但尚未核实的外部效果都单独保留。
	CollectRemaining bool     `json:"collect_remaining,omitempty"`
	Branches         []Branch `json:"branches"`
}

// Branch 是等待组中的一个并行分支。
type Branch struct {
	Name      string            `json:"name"`
	Action    string            `json:"action,omitempty"`
	Params    map[string]string `json:"params,omitempty"`
	TimeoutMS int64             `json:"timeout_ms,omitempty"`
	// Subgroup 非空时本分支是一个嵌套等待组。
	Subgroup *WaitGroup `json:"subgroup,omitempty"`
}

// EventType 是历史事件类型。
type EventType string

const (
	EvtRunStarted           EventType = "run_started"
	EvtStepStarted          EventType = "step_started"
	EvtWaitScheduled        EventType = "wait_scheduled"
	EvtExternalResult       EventType = "external_result"
	EvtExternalCancel       EventType = "external_cancellation"
	EvtExternalDuplicate    EventType = "external_result_duplicate"
	EvtWaitTimedOut         EventType = "wait_timed_out"
	EvtWaitCanceled         EventType = "wait_canceled"
	EvtWaitFired            EventType = "wait_fired"
	EvtActionInvoked        EventType = "action_invoked"
	EvtActionAcked          EventType = "action_acked"
	EvtActionRetried        EventType = "action_retried"
	EvtGroupOpened          EventType = "group_opened"
	EvtBranchScheduled      EventType = "branch_scheduled"
	EvtBranchActionInvoked  EventType = "branch_action_invoked"
	EvtBranchActionAcked    EventType = "branch_action_acked"
	EvtBranchRetried        EventType = "branch_retried"
	EvtBranchTimedOut       EventType = "branch_timed_out"
	EvtBranchFailure        EventType = "branch_failure"
	EvtGroupDecided         EventType = "group_decided"
	EvtGroupCompleted       EventType = "group_completed"
	EvtGroupCancelRequested EventType = "group_cancel_requested"
	EvtStepCompleted        EventType = "step_completed"
	EvtRunCompleted         EventType = "run_completed"
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
