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
	// Kind: action / wait / parallel_wait
	Kind      string            `json:"kind"`
	Name      string            `json:"name"`
	Action    string            `json:"action,omitempty"`
	Params    map[string]string `json:"params,omitempty"`
	TimeoutMS int64             `json:"timeout_ms,omitempty"`
	// Group 仅 Kind == parallel_wait 时使用：描述一个并行等待组。
	Group *WaitGroupSpec `json:"group,omitempty"`
}

// WaitMode 是等待组的满足条件。
type WaitMode string

const (
	// WaitAll 要求所有分支都产生有效（成功）结果。
	WaitAll WaitMode = "all"
	// WaitAny 任意一个分支产生有效结果即满足。
	WaitAny WaitMode = "any"
	// WaitQuorum 要求至少 Quota 个有效结果。
	WaitQuorum WaitMode = "quorum"
)

// AfterMode 决定等待组达到条件后如何处理其余分支。
type AfterMode string

const (
	// AfterCollect 继续收集剩余分支的迟到结果（不取消外部请求）。
	AfterCollect AfterMode = "collect"
	// AfterCancel 对剩余分支发出取消请求。
	AfterCancel AfterMode = "cancel"
)

// WaitGroupSpec 声明一个并行等待组。
type WaitGroupSpec struct {
	// Mode: all / any / quorum
	Mode WaitMode `json:"mode"`
	// Quota 仅 Mode == quorum 时有效：需要的有效结果数量。
	Quota int `json:"quota,omitempty"`
	// FailuresCount 为 true 时失败结果也算作满足条件的有效结果；
	// 默认 false：失败结果明确不满足本组条件。
	FailuresCount bool `json:"failures_count,omitempty"`
	// After: collect（默认）/ cancel，达到条件后对剩余分支的处置。
	After AfterMode `json:"after,omitempty"`
	// TimeoutMS 为整组截止；0 表示不超时。
	TimeoutMS int64        `json:"group_timeout_ms,omitempty"`
	Branches  []BranchSpec `json:"branches"`
}

// BranchSpec 声明等待组中的一个分支：外部等待、动作或嵌套等待组。
type BranchSpec struct {
	Name      string            `json:"name"`
	Wait      bool              `json:"wait,omitempty"`
	TimeoutMS int64             `json:"branch_timeout_ms,omitempty"`
	Action    string            `json:"action,omitempty"`
	Params    map[string]string `json:"params,omitempty"`
	Group     *WaitGroupSpec    `json:"group,omitempty"`
}

// EventType 是历史事件类型。
type EventType string

const (
	EvtRunStarted        EventType = "run_started"
	EvtStepStarted       EventType = "step_started"
	EvtWaitScheduled     EventType = "wait_scheduled"
	EvtExternalResult    EventType = "external_result"
	EvtExternalCancel    EventType = "external_cancellation"
	EvtExternalDuplicate EventType = "external_result_duplicate"
	EvtWaitTimedOut      EventType = "wait_timed_out"
	EvtWaitCanceled      EventType = "wait_canceled"
	EvtWaitFired         EventType = "wait_fired"
	EvtActionInvoked     EventType = "action_invoked"
	EvtActionAcked       EventType = "action_acked"
	EvtActionRetried     EventType = "action_retried"
	EvtStepCompleted     EventType = "step_completed"
	EvtRunCompleted      EventType = "run_completed"

	// 并行等待组事件。
	EvtGroupScheduled     EventType = "group_scheduled"
	EvtGroupResult        EventType = "group_branch_result"
	EvtGroupFailure       EventType = "group_branch_failure"
	EvtGroupCanceled      EventType = "group_branch_canceled"
	EvtGroupTimedOut      EventType = "group_branch_timed_out"
	EvtGroupRetried       EventType = "group_branch_retried"
	EvtGroupCancelReq     EventType = "group_cancel_requested"
	EvtGroupTimedOutAll   EventType = "group_timed_out"
	EvtGroupDecided       EventType = "group_decided"
	EvtGroupActionInvoked EventType = "group_action_invoked"
	EvtGroupActionAcked   EventType = "group_action_acked"
	EvtGroupUncertain     EventType = "group_action_uncertain"
)

// 等待组事件在 Extra 中使用的键。
const (
	ExtraGroupID       = "group_id"
	ExtraBranch        = "branch_path"
	ExtraAttempt       = "attempt"
	ExtraMode          = "mode"
	ExtraQuota         = "quota"
	ExtraSatisfied     = "satisfied"
	ExtraSelected      = "selected_branches"
	ExtraFailuresCount = "failures_count"
	ExtraAfter         = "after"
	ExtraAction        = "action"
	ExtraReason        = "reason"
	ExtraStale         = "stale"
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
