package engine

import (
	"errors"
	"fmt"
	"sort"
	"time"
)

// 并行等待组（parallel wait group）的实现。所有状态迁移先写历史再生效；
// 结果、超时、取消在同一把锁内裁决，每个等待组在任何竞争下只会形成一个
// 可追溯的决策。决策之后到达的结果仍按历史记录，但不会悄悄替换决策时
// 选定的结果集合。

// ErrGroupDecided 表示等待组已经形成唯一决策；迟到的重试不再被受理。
var ErrGroupDecided = errors.New("engine: wait group already reached its decision")

// UncertainEffect 描述等待组中一个已触发但未核实的外部效果。
// 组不再需要某个分支不代表该效果消失：它被单独保留，等待人工核实。
type UncertainEffect struct {
	StepIndex int
	Token     string
	Attempt   int
	Key       string
	Action    string
}

const (
	outcomeSuccess = "success"
	outcomeFailure = "failure"
	outcomeTimeout = "timeout"
	outcomeCancel  = "cancel"
)

type branchOutcome struct {
	kind    string
	result  string
	attempt int
}

type branchState struct {
	token         string
	name          string
	path          string
	parent        *groupState
	index         int
	spec          Branch
	attempt       int
	scheduled     bool
	actionPending bool
	actionKey     string
	terminal      bool
	outcome       *branchOutcome
	lates         []branchOutcome
	deadline      time.Time
	hasDeadline   bool
	timerArmed    bool
	cancelAsked   bool
}

type groupState struct {
	token       string
	stepIndex   int
	path        string
	spec        WaitGroup
	parent      *groupState
	children    []*groupState
	opened      bool
	decided     bool
	satisfied   bool
	exhausted   bool
	selected    []string
	completed   bool
	cancelSent  bool
	deadline    time.Time
	hasDeadline bool
	timerArmed  bool
}

// BranchStatus 是等待组中单个分支的可观测状态。
type BranchStatus struct {
	Token       string `json:"token"`
	Name        string `json:"name"`
	Attempt     int    `json:"attempt"`
	Pending     bool   `json:"pending"`
	Terminal    bool   `json:"terminal"`
	Outcome     string `json:"outcome,omitempty"`
	Result      string `json:"result,omitempty"`
	CancelAsked bool   `json:"cancel_asked"`
	Unverified  bool   `json:"unverified_effect"`
	InDecision  bool   `json:"in_decision"`
	LateCount   int    `json:"late_count"`
}

// GroupStatus 是一个等待组的可观测快照。
type GroupStatus struct {
	Token            string                 `json:"token"`
	Mode             WaitMode               `json:"mode"`
	CountFailures    bool                   `json:"count_failures"`
	CollectRemaining bool                   `json:"collect_remaining"`
	Decided          bool                   `json:"decided"`
	Satisfied        bool                   `json:"satisfied"`
	SelectedTokens   []string               `json:"selected_tokens"`
	Branches         []BranchStatus         `json:"branches"`
	Groups           map[string]GroupStatus `json:"groups,omitempty"`
}

func groupToken(runID string, stepIndex int, path string) string {
	if path == "" {
		return fmt.Sprintf("%s:group:%d", runID, stepIndex)
	}
	return fmt.Sprintf("%s:group:%d.%s", runID, stepIndex, path)
}

func branchToken(groupTok string, index int) string {
	return fmt.Sprintf("%s:b%d", groupTok, index)
}

func branchActionKey(tok string, attempt int) string {
	return fmt.Sprintf("%s:a%d", tok, attempt)
}

// buildGroupTree 依据定义构建嵌套组的内存结构（不产生任何事件）。
func buildGroupTree(runID string, stepIndex int, spec WaitGroup) *groupState {
	var build func(spec WaitGroup, parent *groupState, path string) *groupState
	build = func(spec WaitGroup, parent *groupState, path string) *groupState {
		g := &groupState{
			token:     groupToken(runID, stepIndex, path),
			stepIndex: stepIndex,
			path:      path,
			spec:      spec,
			parent:    parent,
			children:  make([]*groupState, len(spec.Branches)),
		}
		for i, b := range spec.Branches {
			if b.Subgroup != nil {
				childPath := fmt.Sprintf("%d", i)
				if path != "" {
					childPath = path + "." + childPath
				}
				g.children[i] = build(*b.Subgroup, g, childPath)
			}
		}
		return g
	}
	return build(spec, nil, "")
}

// registerGroupLocked 把组树及叶分支登记到引擎的 token 索引中。
func (e *Engine) registerGroupLocked(root *groupState) {
	var walk func(g *groupState)
	walk = func(g *groupState) {
		e.groupByToken[g.token] = g
		for i, b := range g.spec.Branches {
			if c := g.children[i]; c != nil {
				walk(c)
			} else {
				tpath := fmt.Sprintf("%d", i)
				if g.path != "" {
					tpath = g.path + "." + tpath
				}
				bs := &branchState{
					token:  branchToken(g.token, i),
					name:   b.Name,
					path:   tpath,
					parent: g,
					index:  i,
					spec:   b,
				}
				e.branchByToken[bs.token] = bs
			}
		}
	}
	walk(root)
}

func (g *groupState) allLeaves(e *Engine) []*branchState {
	var out []*branchState
	var walk func(x *groupState)
	walk = func(x *groupState) {
		for i := range x.spec.Branches {
			if c := x.children[i]; c != nil {
				walk(c)
			} else if b, ok := e.branchByToken[branchToken(x.token, i)]; ok {
				out = append(out, b)
			}
		}
	}
	walk(g)
	return out
}

// directOutcomeLocked 返回某个直接分支对本组的贡献：是否已终态、是否计入条件。
func (e *Engine) directOutcomeLocked(g *groupState, i int) (terminal bool, counted bool) {
	if c := g.children[i]; c != nil {
		if !c.decided {
			return false, false
		}
		return true, c.satisfied
	}
	b, exists := e.branchByToken[branchToken(g.token, i)]
	if !exists || !b.terminal {
		return false, false
	}
	return true, b.outcome.kind == outcomeSuccess || g.spec.CountFailures
}

// evaluateGroupLocked 检查组是否已满足或已耗尽；条件成立则形成唯一决策，
// 并向父组上报。重复调用对已决策组是幂等的。
func (e *Engine) evaluateGroupLocked(g *groupState) error {
	if g.decided {
		return nil
	}
	n := len(g.spec.Branches)
	terminals, counted := 0, 0
	for i := 0; i < n; i++ {
		t, c := e.directOutcomeLocked(g, i)
		if t {
			terminals++
		}
		if c {
			counted++
		}
	}
	satisfied := false
	switch g.spec.Mode {
	case ModeAll:
		satisfied = terminals == n && counted == n
	case ModeAny:
		satisfied = counted >= 1
	case ModeQuorum:
		q := g.spec.Quota
		if q <= 0 {
			q = 1
		}
		satisfied = counted >= q
	default:
		satisfied = terminals == n && counted == n
	}
	exhausted := terminals == n && !satisfied
	if !satisfied && !exhausted {
		return nil
	}
	g.decided = true
	g.satisfied = satisfied
	g.exhausted = exhausted
	g.selected = selectTokensLocked(e, g)
	if err := e.appendGroupDecidedLocked(g); err != nil {
		return err
	}
	// 满足且不继续收集：对仍未完成的叶分支发出取消请求。
	if satisfied && !g.spec.CollectRemaining {
		if err := e.requestGroupCancelsLocked(g); err != nil {
			return err
		}
	}
	if g.parent != nil {
		if err := e.evaluateGroupLocked(g.parent); err != nil {
			return err
		}
	}
	if g.parent == nil {
		if ch, ok := e.groupWaitCh[g.token]; ok && ch != nil {
			select {
			case ch <- struct{}{}:
			default:
			}
		}
	}
	return nil
}

func (e *Engine) requestGroupCancelsLocked(g *groupState) error {
	if g.cancelSent {
		return nil
	}
	g.cancelSent = true
	for _, b := range g.allLeaves(e) {
		if b.terminal {
			continue
		}
		b.cancelAsked = true
		if err := e.appendLocked(Event{
			Type:      EvtGroupCancelRequested,
			StepIndex: g.stepIndex,
			Token:     b.token,
			Detail:    "group satisfied; cancellation requested",
		}); err != nil {
			return err
		}
	}
	return nil
}

// selectTokensLocked 返回决策快照中真正计入条件的分支 token。
func selectTokensLocked(e *Engine, g *groupState) []string {
	var selected []string
	for i := 0; i < len(g.spec.Branches); i++ {
		t, c := e.directOutcomeLocked(g, i)
		if t && c {
			if child := g.children[i]; child != nil {
				selected = append(selected, child.token)
			} else {
				selected = append(selected, branchToken(g.token, i))
			}
		}
	}
	return selected
}

func (e *Engine) appendGroupDecidedLocked(g *groupState) error {
	selected := append([]string{}, g.selected...)
	sort.Strings(selected)
	kind := "satisfied"
	if !g.satisfied {
		kind = "exhausted"
	}
	return e.appendLocked(Event{
		Type:      EvtGroupDecided,
		StepIndex: g.stepIndex,
		Token:     g.token,
		Detail:    kind,
		Result:    string(g.spec.Mode),
		Extra: map[string]interface{}{
			"satisfied":      g.satisfied,
			"mode":           string(g.spec.Mode),
			"count_failures": g.spec.CountFailures,
			"selected":       selected,
		},
	})
}
