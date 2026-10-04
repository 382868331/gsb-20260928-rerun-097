package engine

import (
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func waitBranchSpec(wait bool, name string) BranchSpec {
	return BranchSpec{Name: name, Wait: true}
}

func startAsync(t *testing.T, eng *Engine, def Definition, runID string, actions map[string]Action) chan error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- eng.Start(def, runID, actions) }()
	return done
}

func waitForEvent(t *testing.T, store *Store, runID string, ty EventType) {
	t.Helper()
	for i := 0; i < 2000; i++ {
		ev, _ := store.Load(runID)
		for _, e := range ev {
			if e.Type == ty {
				return
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("event %s never recorded", ty)
}

func branchTok(runID string, step int, path string) string {
	return groupRootID(runID, step) + ":b:" + path + ":a:0"
}

func lastEvent(ev []Event, ty EventType) Event {
	var out Event
	for _, e := range ev {
		if e.Type == ty {
			out = e
		}
	}
	return out
}

func mustLoad(t *testing.T, store *Store, runID string) []Event {
	t.Helper()
	ev, err := store.Load(runID)
	if err != nil {
		t.Fatal(err)
	}
	return ev
}

// all 模式：所有分支都成功才继续；视图展示结果/选中/未完成。
func TestWaitGroupAllSatisfied(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewStore(dir)
	def := Definition{ID: 10, Steps: []Step{{Kind: "parallel_wait", Name: "wg", Group: &WaitGroupSpec{
		Mode: WaitAll, After: AfterCollect,
		Branches: []BranchSpec{waitBranchSpec(true, "a"), waitBranchSpec(true, "b"), waitBranchSpec(true, "c")},
	}}}}
	eng := NewEngine(store, systemClock{}, Hooks{})
	done := startAsync(t, eng, def, "g1", nil)
	waitForEvent(t, store, "g1", EvtGroupScheduled)
	views := eng.WaitGroups()
	if len(views) != 1 || len(views[0].Pending) != 3 || views[0].Decided {
		t.Fatalf("unexpected initial view: %+v", views)
	}
	if err := eng.DeliverGroupResult(branchTok("g1", 0, "0"), "r0"); err != nil {
		t.Fatal(err)
	}
	if err := eng.DeliverGroupResult(branchTok("g1", 0, "1"), "r1"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	select {
	case err := <-done:
		t.Fatalf("group must not finish early: %v", err)
	default:
	}
	if v := eng.WaitGroups()[0]; len(v.Pending) != 1 || v.Pending[0] != "2" {
		t.Fatalf("expected one pending branch, got %+v", v.Pending)
	}
	if err := eng.DeliverGroupResult(branchTok("g1", 0, "2"), "r2"); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	views2 := eng.WaitGroups()
	if len(views2) != 1 || !views2[0].Decided || !views2[0].Satisfied || len(views2[0].Selected) != 3 {
		t.Fatalf("finished group stays queryable with its decision: %+v", views2)
	}
	ev, _ := store.Load("g1")
	if countType(ev, EvtGroupDecided) != 1 || countType(ev, EvtRunCompleted) != 1 {
		t.Fatalf("exactly one decision and completion: %v", typesOf(ev))
	}
}

// any 模式：首个成功结果决定；迟到结果记录为 stale，不改变 selected。
func TestWaitGroupAnyAndLateResults(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewStore(dir)
	def := Definition{ID: 11, Steps: []Step{{Kind: "parallel_wait", Name: "wg", Group: &WaitGroupSpec{
		Mode: WaitAny, After: AfterCollect,
		Branches: []BranchSpec{waitBranchSpec(true, "a"), waitBranchSpec(true, "b")},
	}}}}
	eng := NewEngine(store, systemClock{}, Hooks{})
	done := startAsync(t, eng, def, "g2", nil)
	waitForEvent(t, store, "g2", EvtGroupScheduled)
	if err := eng.DeliverGroupResult(branchTok("g2", 0, "1"), "win"); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := eng.DeliverGroupResult(branchTok("g2", 0, "0"), "late"); err != nil {
		t.Fatal(err)
	}
	if err := eng.DeliverGroupResult(branchTok("g2", 0, "1"), "dup"); err != nil {
		t.Fatal(err)
	}
	ev, _ := store.Load("g2")
	dec := lastEvent(ev, EvtGroupDecided)
	sel, _ := dec.Extra[ExtraSelected].([]interface{})
	if len(sel) != 1 || sel[0] != "1" {
		t.Fatalf("selected must stay branch 1, got %v", sel)
	}
	if countType(ev, EvtGroupDecided) != 1 || countType(ev, EvtExternalDuplicate) != 1 {
		t.Fatalf("one decision, one duplicate: %v", typesOf(ev))
	}
}

// FailuresCount=true 时失败计入 all 的有效结果。
func TestWaitGroupFailureCounting(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewStore(dir)
	def := Definition{ID: 12, Steps: []Step{{Kind: "parallel_wait", Name: "wg", Group: &WaitGroupSpec{
		Mode: WaitAll, FailuresCount: true,
		Branches: []BranchSpec{waitBranchSpec(true, "a"), waitBranchSpec(true, "b")},
	}}}}
	eng := NewEngine(store, systemClock{}, Hooks{})
	done := startAsync(t, eng, def, "g3", nil)
	waitForEvent(t, store, "g3", EvtGroupScheduled)
	if err := eng.DeliverGroupFailure(branchTok("g3", 0, "0"), "boom"); err != nil {
		t.Fatal(err)
	}
	if err := eng.DeliverGroupResult(branchTok("g3", 0, "1"), "ok"); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	dec := lastEvent(mustLoad(t, store, "g3"), EvtGroupDecided)
	if ok, _ := dec.Extra[ExtraSatisfied].(bool); !ok {
		t.Fatal("failures_count=true must make failures satisfy the group")
	}
}

// 失败默认不满足条件：all 永不满足时以 unsatisfied 唯一决策收尾。
func TestWaitGroupFailureDoesNotCount(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewStore(dir)
	def := Definition{ID: 13, Steps: []Step{{Kind: "parallel_wait", Name: "wg", Group: &WaitGroupSpec{
		Mode:     WaitAll,
		Branches: []BranchSpec{waitBranchSpec(true, "a"), waitBranchSpec(true, "b")},
	}}}}
	eng := NewEngine(store, systemClock{}, Hooks{})
	done := startAsync(t, eng, def, "g4", nil)
	waitForEvent(t, store, "g4", EvtGroupScheduled)
	if err := eng.DeliverGroupFailure(branchTok("g4", 0, "0"), "boom"); err != nil {
		t.Fatal(err)
	}
	if err := eng.DeliverGroupResult(branchTok("g4", 0, "1"), "ok"); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	ev := mustLoad(t, store, "g4")
	dec := lastEvent(ev, EvtGroupDecided)
	if ok, _ := dec.Extra[ExtraSatisfied].(bool); ok {
		t.Fatal("failure must not satisfy the group by default")
	}
	if countType(ev, EvtGroupDecided) != 1 {
		t.Fatal("still exactly one traceable decision")
	}
}

// quorum：达到指定数量有效结果即继续，selected 为最早到达的指定数量分支。
func TestWaitGroupQuorum(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewStore(dir)
	def := Definition{ID: 14, Steps: []Step{{Kind: "parallel_wait", Name: "wg", Group: &WaitGroupSpec{
		Mode: WaitQuorum, Quota: 2, After: AfterCollect,
		Branches: []BranchSpec{waitBranchSpec(true, "a"), waitBranchSpec(true, "b"), waitBranchSpec(true, "c"), waitBranchSpec(true, "d")},
	}}}}
	eng := NewEngine(store, systemClock{}, Hooks{})
	done := startAsync(t, eng, def, "g5", nil)
	waitForEvent(t, store, "g5", EvtGroupScheduled)
	for _, p := range []string{"3", "1"} {
		if err := eng.DeliverGroupResult(branchTok("g5", 0, p), "r-"+p); err != nil {
			t.Fatal(err)
		}
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	dec := lastEvent(mustLoad(t, store, "g5"), EvtGroupDecided)
	sel, _ := dec.Extra[ExtraSelected].([]interface{})
	if len(sel) != 2 || sel[0] != "3" || sel[1] != "1" {
		t.Fatalf("quorum picks earliest 2 in order, got %v", sel)
	}
	if err := eng.DeliverGroupResult(branchTok("g5", 0, "0"), "late"); err != nil {
		t.Fatal(err)
	}
	if n := countType(mustLoad(t, store, "g5"), EvtGroupResult); n != 3 {
		t.Fatalf("late result still recorded, got %d", n)
	}
}

// 崩溃恢复：组内动作已 Invoke、结果未确认前崩溃，恢复后效果仍单独保留。
func TestWaitGroupUncertainActionRecovery(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewStore(dir)
	var calls int32
	actions := map[string]Action{"pay": &countingAction{name: "pay", calls: &calls, res: "charged"}}
	def := Definition{ID: 22, Steps: []Step{{Kind: "parallel_wait", Name: "wg", Group: &WaitGroupSpec{
		Mode: WaitAll,
		Branches: []BranchSpec{
			{Name: "do-pay", Action: "pay"},
			{Name: "wait-thing", Wait: true},
		},
	}}}}
	runID := "g13"
	tok := groupRootID(runID, 0) + ":b:0:a:0"
	now := time.Now().UnixMilli()
	for i, ev := range []Event{
		{Type: EvtRunStarted},
		{Type: EvtStepStarted, StepIndex: 0, Detail: "wg"},
		{Type: EvtGroupScheduled, StepIndex: 0, Token: groupRootID(runID, 0)},
		{Type: EvtGroupActionInvoked, StepIndex: 0, Token: tok, Key: tok + ":action", Detail: "pay",
			Extra: map[string]interface{}{ExtraGroupID: groupRootID(runID, 0), ExtraBranch: "0", ExtraAttempt: 0}},
	} {
		ev.Seq = int64(i + 1)
		ev.RunID = runID
		ev.DefID = def.ID
		ev.At = now
		if err := store.Append(ev); err != nil {
			t.Fatal(err)
		}
	}
	eng, err := Recover(store, systemClock{}, def, runID, Hooks{})
	if err != nil {
		t.Fatal(err)
	}
	uas := eng.GroupUncertainActions()
	if len(uas) != 1 || uas[0].Path != "0" {
		t.Fatalf("invoked-but-unconfirmed effect must survive recovery: %+v", uas)
	}
	done := make(chan error, 1)
	go func() { done <- eng.Run(actions) }()
	if err := eng.ConfirmGroupAction(uas[0].Key, "verified-after-recovery"); err != nil {
		t.Fatal(err)
	}
	if err := eng.DeliverGroupResult(groupRootID(runID, 0)+":b:1:a:0", "thing"); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if atomic.LoadInt32(&calls) != 0 {
		t.Fatal("recovered engine must not re-invoke the action whose effect is unconfirmed")
	}
	ev := mustLoad(t, store, runID)
	if countType(ev, EvtGroupDecided) != 1 || countType(ev, EvtGroupActionInvoked) != 1 {
		t.Fatalf("decision reused, no re-invoke: %v", typesOf(ev))
	}
}

// after=cancel：达到条件后对剩余分支发出取消请求；迟到真实结果仍记录为 stale。
func TestWaitGroupAfterCancel(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewStore(dir)
	def := Definition{ID: 15, Steps: []Step{{Kind: "parallel_wait", Name: "wg", Group: &WaitGroupSpec{
		Mode: WaitAny, After: AfterCancel,
		Branches: []BranchSpec{waitBranchSpec(true, "a"), waitBranchSpec(true, "b")},
	}}}}
	eng := NewEngine(store, systemClock{}, Hooks{})
	done := startAsync(t, eng, def, "g6", nil)
	waitForEvent(t, store, "g6", EvtGroupScheduled)
	if err := eng.DeliverGroupResult(branchTok("g6", 0, "0"), "win"); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	ev := mustLoad(t, store, "g6")
	if countType(ev, EvtGroupCancelReq) != 1 {
		t.Fatalf("remaining branch must get a cancel request, got %v", typesOf(ev))
	}
	// 外部效果在取消请求后才真正回来：记录，但不能改变 selected。
	before := countType(mustLoad(t, store, "g6"), EvtGroupDecided)
	if err := eng.DeliverGroupResult(branchTok("g6", 0, "1"), "late-real"); err != nil {
		t.Fatal(err)
	}
	ev = mustLoad(t, store, "g6")
	if countType(ev, EvtGroupResult) != 2 || countType(ev, EvtGroupDecided) != before {
		t.Fatalf("late result recorded, decision unchanged: %v", typesOf(ev))
	}
}

// 整组超时与结果竞争：只有一个决策；超时后迟到结果不改变集合。
func TestWaitGroupTimeoutRace(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewStore(dir)
	def := Definition{ID: 16, Steps: []Step{{Kind: "parallel_wait", Name: "wg", Group: &WaitGroupSpec{
		Mode: WaitAll, TimeoutMS: 40,
		Branches: []BranchSpec{waitBranchSpec(true, "a"), waitBranchSpec(true, "b")},
	}}}}
	eng := NewEngine(store, systemClock{}, Hooks{})
	done := startAsync(t, eng, def, "g7", nil)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := eng.DeliverGroupResult(branchTok("g7", 0, "0"), "late"); err != nil {
		t.Fatal(err)
	}
	if err := eng.DeliverGroupResult(branchTok("g7", 0, "1"), "late2"); err != nil {
		t.Fatal(err)
	}
	ev := mustLoad(t, store, "g7")
	if countType(ev, EvtGroupDecided) != 1 {
		t.Fatalf("exactly one group decision in a race, got %v", typesOf(ev))
	}
	dec := lastEvent(ev, EvtGroupDecided)
	if ok, _ := dec.Extra[ExtraSatisfied].(bool); ok {
		t.Fatal("timeout cannot leave a satisfied all-group")
	}
	if countType(ev, EvtGroupTimedOutAll) != 1 {
		t.Fatalf("one group timeout marker, got %v", typesOf(ev))
	}
}

// 动作分支失败：外部效果不确定时单独保留；分支不再被需要也不消失，核实后确认。
func TestWaitGroupActionUncertainRetained(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewStore(dir)
	var calls int32
	actions := map[string]Action{"pay": &countingAction{name: "pay", calls: &calls, err: errors.New("delivery timeout")}}
	def := Definition{ID: 17, Steps: []Step{{Kind: "parallel_wait", Name: "wg", Group: &WaitGroupSpec{
		Mode: WaitAny, After: AfterCancel,
		Branches: []BranchSpec{
			{Name: "wait-pay", Wait: true},
			{Name: "do-pay", Action: "pay"},
		},
	}}}}
	eng := NewEngine(store, systemClock{}, Hooks{})
	done := startAsync(t, eng, def, "g8", actions)
	waitForEvent(t, store, "g8", EvtGroupScheduled)
	// 外部等待分支先成功 -> any 满足，动作分支被取消请求。
	if err := eng.DeliverGroupResult(branchTok("g8", 0, "0"), "paid-externally"); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	// 等待动作分支进入不确定状态。
	deadline := time.Now().Add(2 * time.Second)
	for {
		if len(eng.GroupUncertainActions()) == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("uncertain group action must be retained, got %+v", eng.GroupUncertainActions())
		}
		time.Sleep(time.Millisecond)
	}
	if atomic.LoadInt32(&calls) != 1 {
		t.Fatal("action invoked exactly once")
	}
	pending := strings.Join(eng.Pending(), ";")
	if !strings.Contains(pending, "unconfirmed") {
		t.Fatalf("pending must surface retained effect: %v", eng.Pending())
	}
	ua := eng.GroupUncertainActions()[0]
	if err := eng.ConfirmGroupAction(ua.Key, "payment-verified"); err != nil {
		t.Fatal(err)
	}
	if len(eng.GroupUncertainActions()) != 0 {
		t.Fatal("confirmation clears the retained effect")
	}
	ev := mustLoad(t, store, "g8")
	if countType(ev, EvtGroupDecided) != 1 {
		t.Fatal("decision remains unique")
	}
	if n := countType(ev, EvtGroupActionInvoked); n != 1 {
		t.Fatalf("no automatic retry of the action, got %d invokes", n)
	}
}

// 嵌套等待组：内层 any 先满足，向外层 all 贡献一个成功单位。
func TestWaitGroupNested(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewStore(dir)
	inner := &WaitGroupSpec{Mode: WaitAny, Branches: []BranchSpec{waitBranchSpec(true, "i-a"), waitBranchSpec(true, "i-b")}}
	def := Definition{ID: 18, Steps: []Step{{Kind: "parallel_wait", Name: "wg", Group: &WaitGroupSpec{
		Mode: WaitAll,
		Branches: []BranchSpec{
			{Name: "outer-wait", Wait: true},
			{Name: "inner", Group: inner},
		},
	}}}}
	eng := NewEngine(store, systemClock{}, Hooks{})
	done := startAsync(t, eng, def, "g9", nil)
	waitForEvent(t, store, "g9", EvtGroupScheduled)
	// 只完成内层的一个分支 + 外层一个分支。
	if err := eng.DeliverGroupResult(branchTok("g9", 0, "1.0"), "inner-win"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	select {
	case err := <-done:
		t.Fatalf("outer all must still wait: %v", err)
	default:
	}
	if err := eng.DeliverGroupResult(branchTok("g9", 0, "0"), "outer-win"); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	ev := mustLoad(t, store, "g9")
	if countType(ev, EvtGroupDecided) != 2 {
		t.Fatalf("inner and root each decide once, got %d", countType(ev, EvtGroupDecided))
	}
	views := eng.WaitGroups()
	if len(views) != 2 {
		t.Fatalf("decided groups stay queryable, got %d views", len(views))
	}
	for _, gv := range views {
		if !gv.Decided || !gv.Satisfied {
			t.Fatalf("each layer keeps its own decision: %+v", gv)
		}
	}
}

// 部分分支重试：失败分支可在组决策前重试，旧结果保留、新 attempt 独立归属。
func TestWaitGroupBranchRetry(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewStore(dir)
	def := Definition{ID: 19, Steps: []Step{{Kind: "parallel_wait", Name: "wg", Group: &WaitGroupSpec{
		Mode:     WaitAll,
		Branches: []BranchSpec{waitBranchSpec(true, "a"), waitBranchSpec(true, "b")},
	}}}}
	eng := NewEngine(store, systemClock{}, Hooks{})
	done := startAsync(t, eng, def, "g10", nil)
	waitForEvent(t, store, "g10", EvtGroupScheduled)
	if err := eng.DeliverGroupFailure(branchTok("g10", 0, "0"), "transient"); err != nil {
		t.Fatal(err)
	}
	newToken, err := eng.RetryGroupBranch(groupRootID("g10", 0), "0")
	if err != nil {
		t.Fatal(err)
	}
	if newToken == branchTok("g10", 0, "0") {
		t.Fatal("retry must mint a new attempt token")
	}
	v := eng.WaitGroups()[0]
	var bv *GroupBranchView
	for i := range v.Branches {
		if v.Branches[i].Path == "0" {
			bv = &v.Branches[i]
		}
	}
	if bv == nil || bv.Attempt != 1 || bv.Status != branchPending || len(bv.AttemptResults) != 1 || bv.AttemptResults[0] != "transient" {
		t.Fatalf("old failure retained, new attempt pending: %+v", bv)
	}
	if err := eng.DeliverGroupResult(newToken, "recovered"); err != nil {
		t.Fatal(err)
	}
	if err := eng.DeliverGroupResult(branchTok("g10", 0, "1"), "ok"); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	ev := mustLoad(t, store, "g10")
	dec := lastEvent(ev, EvtGroupDecided)
	sel, _ := dec.Extra[ExtraSelected].([]interface{})
	if len(sel) != 2 {
		t.Fatalf("recovered branch participates with new result, selected=%v", sel)
	}
	if countType(ev, EvtGroupRetried) != 1 {
		t.Fatal("retry recorded once")
	}
	// 组决策后禁止重试。
	if _, err := eng.RetryGroupBranch(groupRootID("g10", 0), "0"); err == nil {
		t.Fatal("retry after decision must be rejected")
	}
}

// 决策前后保存/恢复：恢复后沿用决策与结果归属；决策前恢复可继续等待并完成。
func TestWaitGroupSavepointRecovery(t *testing.T) {
	t.Run("decided before crash", func(t *testing.T) {
		dir := t.TempDir()
		store, _ := NewStore(dir)
		def := Definition{ID: 20, Steps: []Step{
			{Kind: "parallel_wait", Name: "wg", Group: &WaitGroupSpec{Mode: WaitAny, After: AfterCollect,
				Branches: []BranchSpec{waitBranchSpec(true, "a"), waitBranchSpec(true, "b")}}},
			{Kind: "action", Name: "after", Action: "after"},
		}}
		var calls int32
		actions := map[string]Action{"after": &countingAction{name: "after", calls: &calls, res: "done"}}
		eng1 := NewEngine(store, systemClock{}, Hooks{})
		done := startAsync(t, eng1, def, "g11", actions)
		waitForEvent(t, store, "g11", EvtGroupScheduled)
		if err := eng1.DeliverGroupResult(branchTok("g11", 0, "0"), "win"); err != nil {
			t.Fatal(err)
		}
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		eng2, err := Recover(store, systemClock{}, def, "g11", Hooks{})
		if err != nil {
			t.Fatal(err)
		}
		if err := eng2.Run(actions); err != nil {
			t.Fatal(err)
		}
		ev := mustLoad(t, store, "g11")
		if countType(ev, EvtGroupDecided) != 1 || atomic.LoadInt32(&calls) != 1 {
			t.Fatalf("decision reused, flow advances once: %v", typesOf(ev))
		}
	})

	t.Run("open wait before crash resumes with same tokens", func(t *testing.T) {
		dir := t.TempDir()
		store, _ := NewStore(dir)
		def := Definition{ID: 21, Steps: []Step{{Kind: "parallel_wait", Name: "wg", Group: &WaitGroupSpec{
			Mode:     WaitAll,
			Branches: []BranchSpec{waitBranchSpec(true, "a"), waitBranchSpec(true, "b")},
		}}}}
		eng1 := NewEngine(store, systemClock{}, Hooks{})
		done := startAsync(t, eng1, def, "g12", nil)
		waitForEvent(t, store, "g12", EvtGroupScheduled)
		if err := eng1.DeliverGroupResult(branchTok("g12", 0, "0"), "first"); err != nil {
			t.Fatal(err)
		}
		time.Sleep(20 * time.Millisecond)
		eng1.Close()
		<-done
		eng2, err := Recover(store, systemClock{}, def, "g12", Hooks{})
		if err != nil {
			t.Fatal(err)
		}
		v := eng2.WaitGroups()[0]
		if len(v.Pending) != 1 || v.Pending[0] != "1" || v.Branches[0].Result != "first" {
			t.Fatalf("recovery must reuse arrivals and ownership: %+v", v)
		}
		done2 := make(chan error, 1)
		go func() { done2 <- eng2.Run(nil) }()
		if err := eng2.DeliverGroupResult(branchTok("g12", 0, "1"), "second"); err != nil {
			t.Fatal(err)
		}
		if err := <-done2; err != nil {
			t.Fatal(err)
		}
		if countType(mustLoad(t, store, "g12"), EvtGroupDecided) != 1 {
			t.Fatal("single decision across savepoint")
		}
	})
}

// 取消信号与结果竞争：被取消分支不贡献有效结果，其余分支照常推进。
func TestWaitGroupCancelRace(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewStore(dir)
	def := Definition{ID: 23, Steps: []Step{{Kind: "parallel_wait", Name: "wg", Group: &WaitGroupSpec{
		Mode: WaitQuorum, Quota: 1, After: AfterCollect,
		Branches: []BranchSpec{waitBranchSpec(true, "a"), waitBranchSpec(true, "b")},
	}}}}
	eng := NewEngine(store, systemClock{}, Hooks{})
	done := startAsync(t, eng, def, "g14", nil)
	waitForEvent(t, store, "g14", EvtGroupScheduled)
	if err := eng.CancelGroupBranch(branchTok("g14", 0, "0"), "user-abort"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	select {
	case err := <-done:
		t.Fatalf("cancellation must not satisfy quorum: %v", err)
	default:
	}
	v := eng.WaitGroups()[0]
	var canceledBranch *GroupBranchView
	for i := range v.Branches {
		if v.Branches[i].Path == "0" {
			canceledBranch = &v.Branches[i]
		}
	}
	if canceledBranch == nil || canceledBranch.Status != branchCanceled || canceledBranch.Selected {
		t.Fatalf("canceled branch shown but not selected: %+v", canceledBranch)
	}
	if err := eng.DeliverGroupResult(branchTok("g14", 0, "1"), "real-win"); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	ev := mustLoad(t, store, "g14")
	if countType(ev, EvtGroupDecided) != 1 {
		t.Fatalf("exactly one decision despite the race: %v", typesOf(ev))
	}
	dec := lastEvent(ev, EvtGroupDecided)
	sel, _ := dec.Extra[ExtraSelected].([]interface{})
	if len(sel) != 1 || sel[0] != "1" {
		t.Fatalf("only the real result participates, got %v", sel)
	}
}

// 已完成的等待组保持可查询：决策、迟到结果、取消请求在视图与重开后都一致。
func TestWaitGroupDecidedViewRetained(t *testing.T) {
	newDef := func() Definition {
		return Definition{ID: 24, Steps: []Step{{Kind: "parallel_wait", Name: "wg", Group: &WaitGroupSpec{
			Mode: WaitAny, After: AfterCancel,
			Branches: []BranchSpec{waitBranchSpec(true, "a"), waitBranchSpec(true, "b"), waitBranchSpec(true, "c")},
		}}}}
	}
	check := func(t *testing.T, eng *Engine) {
		t.Helper()
		views := eng.WaitGroups()
		if len(views) != 1 {
			t.Fatalf("decided group stays queryable, got %d views", len(views))
		}
		v := views[0]
		if !v.Decided || !v.Satisfied || len(v.Selected) != 1 || v.Selected[0] != "0" {
			t.Fatalf("unique decision and selected set retained: %+v", v)
		}
		byPath := map[string]GroupBranchView{}
		for _, b := range v.Branches {
			byPath[b.Path] = b
		}
		late := byPath["1"]
		if !late.CancelRequested || late.Selected || len(late.AttemptResults) != 2 || late.AttemptResults[1] != "late" {
			t.Fatalf("late result kept in its own branch attempt record: %+v", late)
		}
		canceled := byPath["2"]
		if !canceled.CancelRequested || canceled.Status != branchCanceled {
			t.Fatalf("cancel-requested branch still reported: %+v", canceled)
		}
	}
	dir := t.TempDir()
	store, _ := NewStore(dir)
	eng1 := NewEngine(store, systemClock{}, Hooks{})
	done := startAsync(t, eng1, newDef(), "g15", nil)
	waitForEvent(t, store, "g15", EvtGroupScheduled)
	if err := eng1.DeliverGroupResult(branchTok("g15", 0, "0"), "win"); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	// 决策后的迟到结果：留在自己的分支上，标 stale，不改变 selected。
	if err := eng1.DeliverGroupResult(branchTok("g15", 0, "1"), "late"); err != nil {
		t.Fatal(err)
	}
	check(t, eng1)
	eng1.Close()
	// 保存并重开：决策、分支归属、迟到结果与取消请求与退出前一致。
	eng2, err := Recover(store, systemClock{}, newDef(), "g15", Hooks{})
	if err != nil {
		t.Fatal(err)
	}
	check(t, eng2)
	if err := eng2.Run(nil); err != nil {
		t.Fatal(err)
	}
	check(t, eng2)
	ev := mustLoad(t, store, "g15")
	if countType(ev, EvtGroupDecided) != 1 {
		t.Fatalf("decision stays unique across save/reopen: %v", typesOf(ev))
	}
}
