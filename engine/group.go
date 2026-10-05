package engine

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

// 并行等待组（wait group）实现。
//
// 每个 parallel_wait 步骤是一个根组，组内分支可以是：
//   - 外部等待（token 投递结果/失败/取消）
//   - 动作分支（立即调用 Action，失败按不确定效果保留）
//   - 嵌套等待组
//
// 关键不变量：
//  1. 每个组有且仅有一个 group_decided 决策；结果、超时、取消竞争时，
//     第一个落盘的终局决定 selected 集合，迟到结果照常记录但标 stale，
//     不会改变已选定的结果归属。
//  2. 决策满足后，after=collect 继续收迟到结果，after=cancel 对剩余分支
//     追加取消请求；已产生但未核实的外部效果始终单独保留。
//  3. 一切状态迁移先写历史，因此恢复后沿用已确认的决策与结果归属。

const (
	branchPending    = "pending"
	branchSucceeded  = "succeeded"
	branchFailed     = "failed"
	branchCanceled   = "canceled"
	branchTimedOut   = "timed_out"
	branchUncertain  = "uncertain"
	branchCancelSent = "cancel_requested"
)

type attemptOutcome struct {
	attempt int
	kind    EventType // EvtGroupResult / Failure / Canceled / TimedOut
	result  string
	seq     int64 // 到达顺序，用于 quorum/any 选择最早的有效结果
	stale   bool  // 决策后到达、不参与本组决策的结果
}

type leafState struct {
	spec           *BranchSpec
	path           string // 相对根组的点分路径，如 "0.2"
	token          string // 当前 attempt 的投递 token
	key            string // 当前 attempt 的动作 key（动作分支）
	attempts       []*attemptOutcome
	outcome        *attemptOutcome // 第一个终局；nil 表示尚未完成
	cancelReq      bool            // 已对当前 attempt 发出取消请求
	uncertain      string          // 已调用但未核实的动作 key；空表示无
	timer          *time.Timer     // 分支级超时（外部等待分支）
	invoked        bool            // 动作分支：当前 attempt 已调用
	invokedAttempt int             // 已调用的 attempt（恢复时若调用未确认则置 -1）
	timerArmed     bool
	liveAttempt    int // 当前在途 attempt 编号
}

type groupState struct {
	id         string
	rootID     string
	stepIndex  int // 仅根组有意义
	spec       *WaitGroupSpec
	parent     *groupState
	path       string // 相对根组的路径，根为 ""
	root       *groupState
	subgroups  map[string]*groupState // 相对本组路径 -> 子组
	subtree    []string               // 本组（含嵌套）所有叶子路径
	flatLeaves map[string]*leafState  // 仅根组使用：全部叶子
	deadline   time.Time
	timer      *time.Timer // 整组超时（仅根组武装）
	decided    bool
	satisfied  bool
	selected   []string
	decidedSeq int64

	// 调度/运行期状态（不从声明重建，恢复时按历史重新设置）。
	scheduled     bool
	timerArmed    bool
	waitCh        chan struct{}
	completedStep bool
}

func groupRootID(runID string, stepIndex int) string {
	return fmt.Sprintf("%s:wg:%d", runID, stepIndex)
}

func leafToken(groupID, path string, attempt int) string {
	return fmt.Sprintf("%s:b:%s:a:%d", groupID, path, attempt)
}

// buildGroup 按声明构造根组及其全部嵌套子组与叶子。
func buildGroup(runID string, stepIndex int, spec *WaitGroupSpec) *groupState {
	rootID := groupRootID(runID, stepIndex)
	root := &groupState{
		id:         rootID,
		rootID:     rootID,
		stepIndex:  stepIndex,
		spec:       spec,
		subgroups:  map[string]*groupState{},
		flatLeaves: map[string]*leafState{},
	}
	root.root = root
	buildInto(root, "", spec)
	return root
}

func buildInto(g *groupState, prefix string, spec *WaitGroupSpec) {
	for i, b := range spec.Branches {
		path := strconv.Itoa(i)
		if prefix != "" {
			path = prefix + "." + path
		}
		if b.Group != nil {
			id := g.rootID + ":" + path
			child := &groupState{
				id:         id,
				rootID:     g.rootID,
				stepIndex:  -1,
				spec:       b.Group,
				parent:     g,
				path:       path,
				root:       g.root,
				subgroups:  map[string]*groupState{},
				flatLeaves: g.root.flatLeaves,
			}
			rel := strings.TrimPrefix(path, g.path)
			rel = strings.TrimPrefix(rel, ".")
			g.subgroups[rel] = child
			buildInto(child, path, b.Group)
			g.subtree = append(g.subtree, child.subtree...)
			continue
		}
		tok := leafToken(g.rootID, path, 0)
		b := b
		leaf := &leafState{spec: &b, path: path, token: tok, key: tok + ":action", liveAttempt: 0, invokedAttempt: -1}
		g.root.flatLeaves[path] = leaf
		g.subtree = append(g.subtree, path)
	}
}

func effectiveAfter(a AfterMode) AfterMode {
	if a == AfterCancel {
		return AfterCancel
	}
	return AfterCollect
}

func collectLeafSpecs(spec *WaitGroupSpec, prefix string, out map[string]*BranchSpec) {
	for i, b := range spec.Branches {
		path := strconv.Itoa(i)
		if prefix != "" {
			path = prefix + "." + path
		}
		if b.Group != nil {
			collectLeafSpecs(b.Group, path, out)
			continue
		}
		b := b
		out[path] = &b
	}
}

// GroupBranchView 描述一个分支在等待组视图中的状态。
type GroupBranchView struct {
	Path            string             `json:"path"`
	Name            string             `json:"name"`
	Status          string             `json:"status"`
	Result          string             `json:"result,omitempty"`
	Attempt         int                `json:"attempt"`
	AttemptResults  []string           `json:"attempt_results,omitempty"`
	Attempts        []GroupAttemptView `json:"attempts,omitempty"`
	Selected        bool               `json:"selected"`
	Stale           bool               `json:"stale,omitempty"`
	CancelRequested bool               `json:"cancel_requested,omitempty"`
	UncertainAction string             `json:"uncertain_action,omitempty"`
}

// GroupAttemptView 是一个分支在某次尝试上实际留下的终局事实。
// 同一尝试可能有多个事实（先失败/取消，迟到成功后到）；
// Stale 表示该事实到达时已不能影响分支裁决或本组决策。
type GroupAttemptView struct {
	Attempt int    `json:"attempt"`
	Status  string `json:"status"`
	Result  string `json:"result,omitempty"`
	Stale   bool   `json:"stale,omitempty"`
}

// GroupView 是等待组的可追溯快照：各分支结果、参与最终决策的分支、
// 尚未完成的分支，以及失败结果是否计入本组条件。
type GroupView struct {
	GroupID       string    `json:"group_id"`
	RootStep      int       `json:"root_step"`
	Mode          WaitMode  `json:"mode"`
	Quota         int       `json:"quota,omitempty"`
	FailuresCount bool      `json:"failures_count"`
	After         AfterMode `json:"after"`
	// Completed 表示根步骤已收尾；已完成的组仍然可查询，
	// 决策、结果归属、迟到结果与未核实效果都不会从视图中消失。
	Completed bool              `json:"completed"`
	Decided   bool              `json:"decided"`
	Satisfied bool              `json:"satisfied"`
	Selected  []string          `json:"selected_branches,omitempty"`
	Pending   []string          `json:"pending_branches,omitempty"`
	Branches  []GroupBranchView `json:"branches"`
}

// WaitGroups 返回当前所有等待组快照（含嵌套子组）。
// 已完成的组保留可查询状态：唯一决策、参与决策的结果、
// 仍在等待的分支与迟到结果都继续可见。
func (e *Engine) WaitGroups() []GroupView {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.waitGroupsLocked()
}

func (e *Engine) waitGroupsLocked() []GroupView {
	var out []GroupView
	steps := make([]int, 0, len(e.groups))
	for si := range e.groups {
		steps = append(steps, si)
	}
	sort.Ints(steps)
	for _, si := range steps {
		root := e.groups[si]
		collectGroupViews(&out, root, e.completed[root.stepIndex])
	}
	return out
}

func collectGroupViews(out *[]GroupView, g *groupState, completed bool) {
	v := GroupView{
		GroupID:       g.id,
		RootStep:      g.stepIndex,
		Mode:          g.spec.Mode,
		Quota:         g.spec.Quota,
		FailuresCount: g.spec.FailuresCount,
		After:         effectiveAfter(g.spec.After),
		Completed:     completed,
		Decided:       g.decided,
		Satisfied:     g.satisfied,
		Selected:      append([]string(nil), g.selected...),
	}
	selectedSet := map[string]bool{}
	for _, p := range g.selected {
		selectedSet[p] = true
	}
	leafSpecs := map[string]*BranchSpec{}
	collectLeafSpecs(g.root.spec, "", leafSpecs)
	for _, p := range g.subtree {
		leaf := g.root.flatLeaves[p]
		status := branchPending
		var result string
		stale := false
		if leaf.uncertain != "" {
			status = branchUncertain
		} else if leaf.outcome != nil {
			switch leaf.outcome.kind {
			case EvtGroupResult:
				status = branchSucceeded
			case EvtGroupFailure:
				status = branchFailed
			case EvtGroupCanceled:
				status = branchCanceled
			case EvtGroupTimedOut:
				status = branchTimedOut
			}
			result = leaf.outcome.result
			stale = leaf.outcome.stale
		} else if leaf.cancelReq {
			status = branchCancelSent
		}
		if status == branchPending || status == branchCancelSent || status == branchUncertain {
			v.Pending = append(v.Pending, p)
		}
		bv := GroupBranchView{
			Path:            p,
			Name:            leafSpecs[p].Name,
			Status:          status,
			Result:          result,
			Attempt:         currentAttempt(leaf),
			Selected:        selectedSet[p],
			Stale:           stale,
			CancelRequested: leaf.cancelReq,
			UncertainAction: leaf.uncertain,
		}
		for _, a := range leaf.attempts {
			bv.AttemptResults = append(bv.AttemptResults, a.result)
			st := ""
			switch a.kind {
			case EvtGroupResult:
				st = branchSucceeded
			case EvtGroupFailure:
				st = branchFailed
			case EvtGroupCanceled:
				st = branchCanceled
			case EvtGroupTimedOut:
				st = branchTimedOut
			}
			bv.Attempts = append(bv.Attempts, GroupAttemptView{
				Attempt: a.attempt, Status: st, Result: a.result, Stale: a.stale,
			})
		}
		v.Branches = append(v.Branches, bv)
	}
	sort.Slice(v.Branches, func(i, j int) bool { return v.Branches[i].Path < v.Branches[j].Path })
	sort.Strings(v.Pending)
	*out = append(*out, v)
	rels := make([]string, 0, len(g.subgroups))
	for rel := range g.subgroups {
		rels = append(rels, rel)
	}
	sort.Strings(rels)
	for _, rel := range rels {
		collectGroupViews(out, g.subgroups[rel], completed)
		_ = rel
	}
}

func currentAttempt(leaf *leafState) int {
	return leaf.liveAttempt
}

// GroupUncertainAction 描述等待组内一个已产生但未核实的外部效果。
type GroupUncertainAction struct {
	GroupID string
	Path    string
	Key     string
	Action  string
	Attempt int
}

// GroupUncertainActions 返回等待组内尚未核实的外部效果；
// 这些效果不会因为分支不再被需要（取消/组已决策）而消失。
func (e *Engine) GroupUncertainActions() []GroupUncertainAction {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.groupUncertainActionsLocked()
}

func (e *Engine) groupUncertainActionsLocked() []GroupUncertainAction {
	var out []GroupUncertainAction
	for _, leaf := range e.groupUncertain {
		out = append(out, GroupUncertainAction{
			GroupID: e.leafGroupIDLocked(leaf),
			Path:    leaf.path,
			Key:     leaf.uncertain,
			Action:  leaf.spec.Action,
			Attempt: currentAttempt(leaf),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}

func (e *Engine) leafGroupIDLocked(leaf *leafState) string {
	for _, root := range e.groups {
		if _, ok := root.flatLeaves[leaf.path]; ok {
			return root.id
		}
	}
	return ""
}

// runGroupStep 调度并等待一个 parallel_wait 步骤。
func (e *Engine) runGroupStep(stepIndex int, st Step, actions map[string]Action) error {
	e.mu.Lock()
	root, exists := e.groups[stepIndex]
	if !exists {
		root = buildGroup(e.runID, stepIndex, st.Group)
		e.groups[stepIndex] = root
	}
	e.running = true
	if !root.scheduled {
		root.scheduled = true
		if err := e.appendLocked(Event{Type: EvtStepStarted, StepIndex: stepIndex, Detail: st.Name}); err != nil {
			e.mu.Unlock()
			return err
		}
		deadline := time.Time{}
		if st.Group.TimeoutMS > 0 {
			deadline = e.clock.Now().Add(time.Duration(st.Group.TimeoutMS) * time.Millisecond)
		}
		root.deadline = deadline
		if err := e.appendLocked(Event{
			Type:      EvtGroupScheduled,
			StepIndex: stepIndex,
			Token:     root.id,
			Deadline:  deadline.UnixMilli(),
			Extra: map[string]interface{}{
				"mode": string(st.Group.Mode), ExtraQuota: st.Group.Quota,
				ExtraFailuresCount: st.Group.FailuresCount, ExtraAfter: string(effectiveAfter(st.Group.After)),
			},
		}); err != nil {
			e.mu.Unlock()
			return err
		}
	}
	ch := root.waitCh
	if ch == nil {
		ch = make(chan struct{}, 1)
		root.waitCh = ch
	}
	e.armGroupLocked(root, actions)
	alreadyDone := root.completedStep
	e.mu.Unlock()

	if alreadyDone {
		return nil
	}
	select {
	case <-ch:
	case <-e.stopCh:
	}
	e.mu.Lock()
	e.running = false
	e.mu.Unlock()
	return nil
}

// armGroupLocked 对恢复/新调度的根组武装：未开始的动作分支、
// 分支级等待定时器、整组超时；并在决策已落盘时补发步骤收尾。
func (e *Engine) armGroupLocked(root *groupState, actions map[string]Action) {
	if root.completedStep {
		return
	}
	if root.decided {
		if root.waitCh != nil {
			root.completedStep = true
			e.completed[root.stepIndex] = true
			e.step = root.stepIndex + 1
			close(root.waitCh)
			root.waitCh = nil
		}
		return
	}
	if !root.timerArmed && !root.deadline.IsZero() {
		root.timerArmed = true
		d := time.Until(root.deadline)
		if d < 0 {
			d = 0
		}
		g := root
		root.timer = time.AfterFunc(d, func() { e.groupTimeoutFired(g) })
	}
	for _, leaf := range root.flatLeaves {
		e.armLeafLocked(root, leaf, actions)
	}
}

func (e *Engine) armLeafLocked(root *groupState, leaf *leafState, actions map[string]Action) {
	if leaf.outcome != nil || leaf.uncertain != "" {
		return
	}
	if leaf.spec.Action != "" {
		attempt := currentAttempt(leaf)
		if leaf.invokedAttempt == attempt && leaf.invoked {
			return // 崩溃前已调用、结果未知：保持未决，等待投递/核实
		}
		leaf.invokedAttempt = attempt
		leaf.invoked = true
		tok := leaf.token
		key := leaf.key
		act := actions[leaf.spec.Action]
		if act == nil {
			leaf.invoked = false
			leaf.invokedAttempt = -1
			_ = e.appendLocked(Event{Type: EvtGroupUncertain, StepIndex: root.stepIndex, Token: tok, Key: key, Detail: leaf.spec.Action, Extra: map[string]interface{}{ExtraGroupID: root.id, ExtraBranch: leaf.path, ExtraAttempt: attempt, ExtraReason: "action not registered"}})
			leaf.uncertain = key
			e.groupUncertain[key] = leaf
			return
		}
		spec := *leaf.spec
		if err := e.appendLocked(Event{Type: EvtGroupActionInvoked, StepIndex: root.stepIndex, Token: tok, Key: key, Detail: spec.Action, Extra: map[string]interface{}{ExtraGroupID: root.id, ExtraBranch: leaf.path, ExtraAttempt: attempt}}); err != nil {
			return
		}
		go func() {
			res, err := act.Invoke(e.runID, -1, spec.Params)
			e.mu.Lock()
			defer e.mu.Unlock()
			if e.closed || e.gen == 0 {
				return
			}
			if err != nil {
				leaf.uncertain = key
				e.groupUncertain[key] = leaf
				_ = e.appendLocked(Event{Type: EvtGroupUncertain, StepIndex: root.stepIndex, Token: tok, Key: key, Detail: spec.Action, Extra: map[string]interface{}{ExtraGroupID: root.id, ExtraBranch: leaf.path, ExtraAttempt: attempt, ExtraReason: err.Error()}})
				e.reevaluateLeafLocked(root, leaf)
				return
			}
			_ = e.appendLocked(Event{Type: EvtGroupActionAcked, StepIndex: root.stepIndex, Token: tok, Key: key, Result: res, Extra: map[string]interface{}{ExtraGroupID: root.id, ExtraBranch: leaf.path, ExtraAttempt: attempt}})
			e.recordLeafOutcomeLocked(root, leaf, attempt, EvtGroupResult, res, false)
		}()
		return
	}
	if leaf.spec.Wait && !leaf.timerArmed && leaf.spec.TimeoutMS > 0 {
		leaf.timerArmed = true
		lf := leaf
		d := time.Duration(lf.spec.TimeoutMS) * time.Millisecond
		lf.timer = time.AfterFunc(d, func() { e.branchTimeoutFired(root, lf) })
	}
}

// recordLeafOutcomeLocked 记录一个叶子终局。每个 attempt 只接受第一个终局；
// 组决策后到达的结果仍落盘但标记 stale，不改变已选定的结果集合。
func (e *Engine) recordLeafOutcomeLocked(root *groupState, leaf *leafState, attempt int, kind EventType, result string, canceled bool) error {
	owner := owningGroupLocked(root, leaf.path)
	sameKind := false
	for _, a := range leaf.attempts {
		if a.attempt == attempt {
			if a.kind == kind {
				sameKind = true
			}
		}
	}
	if sameKind {
		// 同一尝试的同类迟到通知只可能重复：补记 duplicate，不改任何事实。
		return e.appendLocked(Event{Type: EvtExternalDuplicate, StepIndex: root.stepIndex, Token: leaf.token, Result: result, Detail: string(kind), Extra: map[string]interface{}{ExtraGroupID: root.id, ExtraBranch: leaf.path, ExtraAttempt: attempt}})
	}
	// 不同类型的迟到信号（如已失败/已取消尝试的迟到成功）是真实发生的事实：
	// 作为该尝试自己的终局事实追加保留，但不参与本组决策，也不改写分支终局。
	oc := &attemptOutcome{attempt: attempt, kind: kind, result: result}
	leaf.attempts = append(leaf.attempts, oc)
	ev := Event{Type: kind, StepIndex: root.stepIndex, Token: leaf.token, Result: result,
		Extra: map[string]interface{}{ExtraGroupID: root.id, ExtraBranch: leaf.path, ExtraAttempt: attempt}}
	_ = canceled
	seq, err := e.appendSeqLocked(ev)
	if err != nil {
		return err
	}
	oc.seq = seq
	// 下列终局事实只属于历史，不改变分支裁决与已确认的选定集合：
	// 组已决策、属于旧尝试（已重试）、已请求取消、或该分支已有在先终局。
	if owner.decided || attempt != leaf.liveAttempt || leaf.cancelReq || leaf.outcome != nil {
		oc.stale = true
	}
	// 分支终局只由当前在途尝试占据；旧尝试的迟到事实不得覆盖新尝试。
	if leaf.outcome == nil && attempt == leaf.liveAttempt {
		leaf.outcome = oc
	}
	e.reevaluateLeafLocked(root, leaf)
	return nil
}

// appendSeqLocked 落盘事件并返回其序号。
func (e *Engine) appendSeqLocked(ev Event) (int64, error) {
	if err := e.appendLocked(ev); err != nil {
		return 0, err
	}
	return e.seq, nil
}

// owningGroupLocked 找到直接拥有该叶子路径的最小嵌套组。
func owningGroupLocked(g *groupState, leafPath string) *groupState {
	var owner *groupState
	var walk func(x *groupState)
	walk = func(x *groupState) {
		for _, c := range x.subgroups {
			if strings.HasPrefix(leafPath, c.path+".") {
				walk(c)
				return
			}
		}
		owner = x
	}
	walk(g)
	return owner
}

// reevaluateLeafLocked 叶子发生变化后，自底向上重新评估各层组。
func (e *Engine) reevaluateLeafLocked(root *groupState, leaf *leafState) {
	g := owningGroupLocked(root, leaf.path)
	for g != nil {
		if g.decided {
			return // 已决策组的迟到结果不再向上传播，selected 已冻结
		}
		e.evaluateGroupLocked(g)
		g = g.parent
	}
}

// directChildForPath 判断路径是否经过某个直接子组，返回该子组。
func directChildForPath(g *groupState, path string) *groupState {
	for _, c := range g.subgroups {
		if strings.HasPrefix(path, c.path+".") {
			return c
		}
	}
	return nil
}

// evaluateGroupLocked 在单组上评估 all/any/quorum；每个组只决策一次。
func (e *Engine) evaluateGroupLocked(g *groupState) {
	if g.decided {
		return
	}
	type scored struct {
		path string
		seq  int64
	}
	var good []scored // 有效结果（成功；FailuresCount 时含失败）
	directTotal := 0
	seenChild := map[string]bool{}
	pending := 0
	for _, p := range g.subtree {
		leaf := g.root.flatLeaves[p]
		if c := directChildForPath(g, p); c != nil {
			if !seenChild[c.path] {
				seenChild[c.path] = true
				if c.decided {
					if c.satisfied {
						good = append(good, scored{path: c.path, seq: c.decidedSeq})
					}
				} else {
					pending++
				}
			}
			continue
		}
		directTotal++
		if leaf.outcome == nil && leaf.uncertain == "" {
			pending++
			continue
		}
		if leaf.outcome == nil || leaf.outcome.stale {
			continue // uncertain 占终局但不算有效结果
		}
		if leaf.outcome.kind == EvtGroupResult || (g.spec.FailuresCount && leaf.outcome.kind == EvtGroupFailure) {
			good = append(good, scored{path: p, seq: leaf.outcome.seq})
		}
	}
	totalUnits := directTotal + len(g.subgroups)
	need := 0
	switch g.spec.Mode {
	case WaitAll:
		need = totalUnits
	case WaitAny:
		need = 1
	case WaitQuorum:
		need = g.spec.Quota
	default:
		need = totalUnits
	}
	satisfied := need > 0 && len(good) >= need
	if !satisfied && pending > 0 {
		return // 仍有分支可能贡献结果，等待
	}
	if need <= 0 {
		return
	}
	selected := []string{}
	if satisfied {
		sort.Slice(good, func(i, j int) bool {
			if good[i].seq != good[j].seq {
				return good[i].seq < good[j].seq
			}
			return good[i].path < good[j].path
		})
		switch g.spec.Mode {
		case WaitAny, WaitQuorum:
			for i := 0; i < need && i < len(good); i++ {
				selected = append(selected, good[i].path)
			}
		default:
			for _, sc := range good {
				selected = append(selected, sc.path)
			}
		}
	}
	e.finalizeGroupLocked(g, satisfied, selected)
}

// finalizeGroupLocked 落盘组的唯一决策；满足时按 after 处置剩余分支，
// 根组决策后收尾步骤。迟到结果不再改变 selected。
func (e *Engine) finalizeGroupLocked(g *groupState, satisfied bool, selected []string) {
	if g.decided {
		return
	}
	g.decided = true
	g.satisfied = satisfied
	g.selected = selected
	sel := append([]string{}, selected...)
	if sel == nil {
		sel = []string{}
	}
	g.decidedSeq = e.seq + 1
	_ = e.appendLocked(Event{
		Type:      EvtGroupDecided,
		StepIndex: g.root.stepIndex,
		Token:     g.id,
		Detail:    map[bool]string{true: "satisfied", false: "unsatisfied"}[satisfied],
		Extra: map[string]interface{}{
			ExtraGroupID: g.id, ExtraSatisfied: satisfied, ExtraSelected: sel,
			"mode": string(g.spec.Mode), ExtraQuota: g.spec.Quota, ExtraFailuresCount: g.spec.FailuresCount,
			ExtraAfter: string(effectiveAfter(g.spec.After)),
		},
	})
	if satisfied && effectiveAfter(g.spec.After) == AfterCancel {
		for _, p := range g.subtree {
			leaf := g.root.flatLeaves[p]
			if leaf.outcome != nil || leaf.uncertain != "" || leaf.cancelReq {
				continue
			}
			leaf.cancelReq = true
			_ = e.appendLocked(Event{Type: EvtGroupCancelReq, StepIndex: g.root.stepIndex, Token: leaf.token,
				Extra: map[string]interface{}{ExtraGroupID: g.id, ExtraBranch: p, ExtraAttempt: currentAttempt(leaf)}})
			oc := &attemptOutcome{attempt: currentAttempt(leaf), kind: EvtGroupCanceled, result: "cancel_requested"}
			leaf.attempts = append(leaf.attempts, oc)
			if leaf.outcome == nil {
				leaf.outcome = oc
			}
		}
	}
	if g.parent != nil {
		e.evaluateGroupLocked(g.parent) // 子组决策向父组传播
		return
	}
	detail := "satisfied"
	if !satisfied {
		detail = "unsatisfied"
	}
	_ = e.appendLocked(Event{Type: EvtStepCompleted, StepIndex: g.stepIndex, Token: g.id, Detail: detail,
		Extra: map[string]interface{}{ExtraSelected: sel, ExtraSatisfied: satisfied}})
	g.completedStep = true
	e.completed[g.stepIndex] = true
	e.step = g.stepIndex + 1
	if g.waitCh != nil {
		close(g.waitCh)
		g.waitCh = nil
	}
}

// groupTimeoutFired 处理整组超时竞争：若组尚未决策，则以 unsatisfied 收尾。
func (e *Engine) groupTimeoutFired(g *groupState) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed || g.decided {
		return
	}
	for _, p := range g.subtree {
		leaf := g.root.flatLeaves[p]
		if leaf.outcome == nil && leaf.uncertain == "" && !leaf.cancelReq {
			leaf.cancelReq = true
			oc := &attemptOutcome{attempt: currentAttempt(leaf), kind: EvtGroupTimedOut, result: "group_timeout"}
			leaf.attempts = append(leaf.attempts, oc)
			leaf.outcome = oc
			_ = e.appendLocked(Event{Type: EvtGroupTimedOut, StepIndex: g.stepIndex, Token: leaf.token,
				Extra: map[string]interface{}{ExtraGroupID: g.id, ExtraBranch: p, ExtraAttempt: oc.attempt, ExtraReason: "group_timeout"}})
		}
	}
	_ = e.appendLocked(Event{Type: EvtGroupTimedOutAll, StepIndex: g.stepIndex, Token: g.id,
		Extra: map[string]interface{}{ExtraGroupID: g.id}})
	e.evaluateAllLocked(g)
}

// evaluateAllLocked 超时后所有在途分支已终止，强制各层重新评估并落盘唯一决策。
func (e *Engine) evaluateAllLocked(g *groupState) {
	for _, c := range g.subgroups {
		e.evaluateAllLocked(c)
	}
	e.evaluateGroupLocked(g)
}

// branchTimeoutFired 处理单个等待分支超时。
func (e *Engine) branchTimeoutFired(g *groupState, leaf *leafState) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return
	}
	if leaf.outcome != nil || leaf.uncertain != "" {
		return
	}
	attempt := currentAttempt(leaf)
	_ = e.recordLeafOutcomeLocked(g, leaf, attempt, EvtGroupTimedOut, "branch_timeout", false)
}

// DeliverGroupResult 向等待组分支投递成功结果。
func (e *Engine) DeliverGroupResult(token, result string) error {
	return e.deliverGroupExternal(token, result, EvtGroupResult, false)
}

// DeliverGroupFailure 投递分支失败结果。是否计入本组条件由 FailuresCount 决定，
// 默认明确不计入；失败照常展示。
func (e *Engine) DeliverGroupFailure(token, failure string) error {
	return e.deliverGroupExternal(token, failure, EvtGroupFailure, false)
}

// CancelGroupBranch 请求取消一个等待组分支（外部取消信号）。
func (e *Engine) CancelGroupBranch(token, reason string) error {
	return e.deliverGroupExternal(token, reason, EvtGroupCanceled, true)
}

func (e *Engine) deliverGroupExternal(token, value string, kind EventType, canceled bool) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	root, leaf, attempt, ok := e.resolveLeafTokenLocked(token)
	if !ok {
		return fmt.Errorf("engine: unknown wait-group branch token %q", token)
	}
	return e.recordLeafOutcomeLocked(root, leaf, attempt, kind, value, canceled)
}

func (e *Engine) resolveLeafTokenLocked(token string) (*groupState, *leafState, int, bool) {
	for _, root := range e.groups {
		for _, leaf := range root.flatLeaves {
			for _, a := range leaf.attempts {
				if leafToken(root.id, leaf.path, a.attempt) == token {
					return root, leaf, a.attempt, true
				}
			}
			if leafToken(root.id, leaf.path, currentAttempt(leaf)) == token {
				return root, leaf, currentAttempt(leaf), true
			}
		}
	}
	return nil, nil, 0, false
}

// RetryGroupBranch 对组决策前、已终局（失败/超时/取消）的等待分支发起部分重试；
// 组已决策或分支仍在途时拒绝。新 attempt 有独立 token，旧结果保留归属。
func (e *Engine) RetryGroupBranch(rootGroupID, branchPath string) (string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	root, leaf, ok := e.findBranchLocked(rootGroupID, branchPath)
	if !ok {
		return "", fmt.Errorf("engine: branch %s not found in group %s", branchPath, rootGroupID)
	}
	if root.decided || owningGroupLocked(root, branchPath).decided {
		return "", errors.New("engine: wait group already decided; retry cannot change the decision")
	}
	if leaf.outcome == nil && leaf.uncertain == "" {
		return "", errors.New("engine: branch has not terminated; nothing to retry")
	}
	if leaf.spec.Action != "" {
		return "", errors.New("engine: action branches are verified via ConfirmGroupAction, not retried")
	}
	attempt := nextAttemptNumber(leaf)
	newToken := leafToken(root.id, branchPath, attempt)
	leaf.token = newToken
	leaf.key = newToken + ":action"
	leaf.liveAttempt = attempt
	leaf.outcome = nil
	leaf.cancelReq = false
	leaf.timerArmed = false
	leaf.invoked = false
	leaf.invokedAttempt = -1
	if err := e.appendLocked(Event{Type: EvtGroupRetried, StepIndex: root.stepIndex, Token: newToken,
		Extra: map[string]interface{}{ExtraGroupID: root.id, ExtraBranch: branchPath, ExtraAttempt: attempt}}); err != nil {
		return "", err
	}
	e.armLeafLocked(root, leaf, map[string]Action{})
	return newToken, nil
}

// nextAttemptNumber 返回下一个 attempt 编号（终局/重试用）。
func nextAttemptNumber(leaf *leafState) int {
	max := -1
	for _, a := range leaf.attempts {
		if a != nil && a.attempt > max {
			max = a.attempt
		}
	}
	return max + 1
}

func (e *Engine) findBranchLocked(rootGroupID, branchPath string) (*groupState, *leafState, bool) {
	for _, root := range e.groups {
		if root.id != rootGroupID {
			continue
		}
		leaf, ok := root.flatLeaves[branchPath]
		return root, leaf, ok
	}
	return nil, nil, false
}

// ConfirmGroupAction 在人工核实组内动作外部效果后补记结果。
// 即便分支因超时/取消不再被需要，未核实的效果仍保留至此确认。
func (e *Engine) ConfirmGroupAction(key, result string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	leaf, ok := e.groupUncertain[key]
	if !ok {
		return fmt.Errorf("engine: key %q is not an unconfirmed group action", key)
	}
	var root *groupState
	for _, g := range e.groups {
		if _, has := g.flatLeaves[leaf.path]; has {
			root = g
			break
		}
	}
	delete(e.groupUncertain, key)
	leaf.uncertain = ""
	attempt := leaf.invokedAttempt
	if attempt < 0 {
		attempt = currentAttempt(leaf)
	}
	if err := e.appendLocked(Event{Type: EvtGroupActionAcked, StepIndex: root.stepIndex, Token: leaf.token, Key: key, Result: result,
		Extra: map[string]interface{}{ExtraGroupID: root.id, ExtraBranch: leaf.path, ExtraAttempt: attempt, ExtraReason: "confirmed"}}); err != nil {
		return err
	}
	return e.recordLeafOutcomeLocked(root, leaf, attempt, EvtGroupResult, result, false)
}

// replayGroupEventLocked 在 Recover 时按历史重放一条等待组事件。
// 恢复后沿用已经确认的决策与结果归属，不重新裁决。
func (e *Engine) replayGroupEventLocked(ev Event) {
	gid, _ := ev.Extra[ExtraGroupID].(string)
	path, _ := ev.Extra[ExtraBranch].(string)
	attempt := extraInt(ev.Extra, ExtraAttempt)
	switch ev.Type {
	case EvtGroupScheduled:
		si := ev.StepIndex
		root := buildGroup(e.runID, si, e.def.Steps[si].Group)
		root.scheduled = true
		root.deadline = time.UnixMilli(ev.Deadline)
		root.waitCh = make(chan struct{}, 1)
		e.groups[si] = root
	case EvtGroupActionInvoked:
		root := e.groups[ev.StepIndex]
		if root == nil {
			return
		}
		leaf := root.flatLeaves[path]
		leaf.invoked = true
		leaf.liveAttempt = attempt
		leaf.invokedAttempt = attempt
	case EvtGroupActionAcked:
		root := e.groups[ev.StepIndex]
		if root == nil {
			return
		}
		leaf := root.flatLeaves[path]
		if leaf == nil {
			return
		}
		delete(e.groupUncertain, ev.Key)
		leaf.uncertain = ""
		if reason, _ := ev.Extra[ExtraReason].(string); reason == "confirmed" {
			e.replayOutcomeLocked(root, leaf, ev.Seq, attempt, EvtGroupResult, ev.Result)
		}
	case EvtGroupUncertain:
		root := e.groups[ev.StepIndex]
		if root == nil {
			return
		}
		leaf := root.flatLeaves[path]
		leaf.uncertain = ev.Key
		e.groupUncertain[ev.Key] = leaf
	case EvtGroupResult, EvtGroupFailure, EvtGroupCanceled, EvtGroupTimedOut:
		root := e.groups[ev.StepIndex]
		if root == nil || path == "" {
			return
		}
		leaf := root.flatLeaves[path]
		if leaf == nil {
			return
		}
		e.replayOutcomeLocked(root, leaf, ev.Seq, attempt, ev.Type, ev.Result)
	case EvtGroupRetried:
		root := e.groups[ev.StepIndex]
		leaf := root.flatLeaves[path]
		leaf.liveAttempt = attempt
		leaf.token = ev.Token
		leaf.key = ev.Token + ":action"
		leaf.outcome = nil
		leaf.cancelReq = false
		leaf.invoked = false
		leaf.invokedAttempt = -1
	case EvtGroupCancelReq:
		root := e.groups[ev.StepIndex]
		if root == nil || path == "" {
			return
		}
		leaf := root.flatLeaves[path]
		if leaf == nil {
			return
		}
		leaf.cancelReq = true
		// 与运行期 finalizeGroupLocked 完全一致：取消请求一经发出，
		// 当前在途尝试即以 canceled 终局（不另落终局事件）；
		// 之后到达的迟到结果只能作为该尝试的 stale 事实，重开后不得翻盘成成功。
		oc := &attemptOutcome{attempt: attempt, kind: EvtGroupCanceled, result: "cancel_requested"}
		leaf.attempts = append(leaf.attempts, oc)
		if leaf.outcome == nil && attempt == leaf.liveAttempt {
			leaf.outcome = oc
		}
	case EvtGroupDecided:
		root := findGroupByIDLocked(e, gid)
		if root == nil {
			return
		}
		g := root
		if gid != root.id {
			g = findSubgroupLocked(root, gid)
		}
		if g == nil {
			return
		}
		g.decided = true
		g.satisfied, _ = ev.Extra[ExtraSatisfied].(bool)
		g.decidedSeq = ev.Seq
		g.selected = nil
		if raw, ok := ev.Extra[ExtraSelected].([]interface{}); ok {
			for _, v := range raw {
				if s, ok := v.(string); ok {
					g.selected = append(g.selected, s)
				}
			}
		}
	}
}

// replayOutcomeLocked 重放一个叶子终局事实。同一尝试可能先后留下多个
// 不同类型的事实（先失败/取消，迟到成功随后到达）：它们都按实际所属尝试
// 保留；决策后、旧尝试或迟到的事实标 stale，不占据分支终局，
// 不覆盖新尝试结果，也不改变已确认的选定集合。
func (e *Engine) replayOutcomeLocked(root *groupState, leaf *leafState, seq int64, attempt int, kind EventType, result string) {
	sameKind := false
	for _, a := range leaf.attempts {
		if a.attempt == attempt {
			if a.kind == kind {
				sameKind = true
			}
		}
	}
	if sameKind {
		// 同类重复事件（重放本身不会重复，但防止历史中重复行改变事实）。
		return
	}
	oc := &attemptOutcome{attempt: attempt, kind: kind, result: result, seq: seq}
	leaf.attempts = append(leaf.attempts, oc)
	owner := owningGroupLocked(root, leaf.path)
	if owner.decided || attempt != leaf.liveAttempt || leaf.cancelReq || leaf.outcome != nil {
		oc.stale = true
	}
	if leaf.outcome == nil && attempt == leaf.liveAttempt {
		leaf.outcome = oc
	}
}

func extraInt(m map[string]interface{}, key string) int {
	switch v := m[key].(type) {
	case int:
		return v
	case int64:
		return int(v)
	case float64:
		return int(v)
	}
	return 0
}

func findGroupByIDLocked(e *Engine, id string) *groupState {
	for _, root := range e.groups {
		if root.id == id {
			return root
		}
		if g := findSubgroupLocked(root, id); g != nil {
			return root
		}
	}
	return nil
}

func findSubgroupLocked(g *groupState, id string) *groupState {
	if g.id == id {
		return g
	}
	for _, c := range g.subgroups {
		if hit := findSubgroupLocked(c, id); hit != nil {
			return hit
		}
	}
	return nil
}

// isGroupEvent 判断事件是否属于并行等待组。
func isGroupEvent(t EventType) bool {
	switch t {
	case EvtGroupScheduled, EvtGroupResult, EvtGroupFailure, EvtGroupCanceled,
		EvtGroupTimedOut, EvtGroupRetried, EvtGroupCancelReq, EvtGroupTimedOutAll,
		EvtGroupDecided, EvtGroupActionInvoked, EvtGroupActionAcked, EvtGroupUncertain:
		return true
	}
	return false
}
