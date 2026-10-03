package engine

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func waitForGroupEvent(t *testing.T, store *Store, runID string, typ EventType) {
	t.Helper()
	for i := 0; i < 3000; i++ {
		evts, err := store.Load(runID)
		if err != nil {
			t.Fatal(err)
		}
		if countType(evts, typ) > 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("event %s never recorded", typ)
}

func quorumDef(mode WaitMode, quota int, countFailures, collect bool, branches []Branch) Definition {
	return Definition{ID: 10, Name: "wg", Steps: []Step{{
		Kind: "group", Name: "fanout",
		Group: WaitGroup{Mode: mode, Quota: quota, CountFailures: countFailures, CollectRemaining: collect, TimeoutMS: 5000, Branches: branches},
	}}}
}

// 模式与状态展示：all/any/quorum；展示各分支结果、参与决策的分支、未完成分支，
// 并明确失败是否计入条件。
func TestWaitGroupModesAndStatus(t *testing.T) {
	branches := []Branch{{Name: "a"}, {Name: "b"}, {Name: "c"}}
	cases := []struct {
		name          string
		mode          WaitMode
		quota         int
		countFailures bool
		deliver       func(eng *Engine, tok string)
		wantSatisfied bool
		wantSelected  int
		wantTerminals int
	}{
		{"any-first-success", ModeAny, 0, false, func(eng *Engine, tok string) {
			if err := eng.DeliverBranchResult(tok+":b1", "v"); err != nil {
				t.Fatal(err)
			}
		}, true, 1, 1},
		{"quorum-two-successes", ModeQuorum, 2, false, func(eng *Engine, tok string) {
			_ = eng.DeliverBranchFailure(tok+":b0", "boom")
			_ = eng.DeliverBranchResult(tok+":b1", "v1")
			_ = eng.DeliverBranchResult(tok+":b2", "v2")
		}, true, 2, 3},
		{"quorum-failures-not-counted", ModeQuorum, 2, false, func(eng *Engine, tok string) {
			_ = eng.DeliverBranchFailure(tok+":b0", "boom")
			_ = eng.CancelBranch(tok+":b1", "gone")
			_ = eng.DeliverBranchResult(tok+":b2", "only-one")
		}, false, 1, 3},
		{"all-count-failures", ModeAll, 0, true, func(eng *Engine, tok string) {
			_ = eng.DeliverBranchFailure(tok+":b0", "f")
			_ = eng.CancelBranch(tok+":b1", "c")
			_ = eng.DeliverBranchResult(tok+":b2", "ok")
		}, true, 3, 3},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			store, _ := NewStore(dir)
			eng := NewEngine(store, systemClock{}, Hooks{})
			runID := "wg-" + tc.name
			done := make(chan error, 1)
			go func() { done <- eng.Start(quorumDef(tc.mode, tc.quota, tc.countFailures, true, branches), runID, nil) }()
			tok := groupToken(runID, 0, "")
			waitForGroupEvent(t, store, runID, EvtGroupOpened)
			tc.deliver(eng, tok)
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			st := eng.GroupStatus(0)
			if st == nil || !st.Decided || st.Satisfied != tc.wantSatisfied {
				t.Fatalf("bad status: %+v", st)
			}
			if len(st.SelectedTokens) != tc.wantSelected {
				t.Fatalf("selected=%v want %d", st.SelectedTokens, tc.wantSelected)
			}
			if st.CountFailures != tc.countFailures {
				t.Fatal("CountFailures not surfaced")
			}
			totalTerm := 0
			for _, b := range st.Branches {
				if b.Terminal {
					totalTerm++
				}
			}
			if totalTerm != tc.wantTerminals {
				t.Fatalf("terminals=%d want %d: %+v", totalTerm, tc.wantTerminals, st.Branches)
			}
			pending, inDecision := 0, 0
			for _, b := range st.Branches {
				if !b.Terminal {
					pending++
				}
				if b.InDecision {
					inDecision++
				}
			}
			if inDecision != tc.wantSelected || pending != 3-tc.wantTerminals {
				t.Fatalf("pending=%d inDecision=%d branches=%+v", pending, inDecision, st.Branches)
			}
			evts, _ := store.Load(runID)
			if countType(evts, EvtGroupDecided) != 1 || countType(evts, EvtGroupCompleted) != 1 {
				t.Fatal("exactly one group decision")
			}
		})
	}
}

// 决策后取消剩余分支 vs 继续收集；迟到结果不改变已选定集合。
func TestGroupDecisionLateResultsAndCancelPolicy(t *testing.T) {
	t.Run("cancel remaining", func(t *testing.T) {
		dir := t.TempDir()
		store, _ := NewStore(dir)
		eng := NewEngine(store, systemClock{}, Hooks{})
		def := quorumDef(ModeAny, 0, false, false, []Branch{{Name: "a"}, {Name: "b"}})
		runID := "wg-cancel"
		done := make(chan error, 1)
		go func() { done <- eng.Start(def, runID, nil) }()
		waitForGroupEvent(t, store, runID, EvtGroupOpened)
		tok := groupToken(runID, 0, "")
		if err := eng.DeliverBranchResult(tok+":b0", "first"); err != nil {
			t.Fatal(err)
		}
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		evts, _ := store.Load(runID)
		if countType(evts, EvtGroupCancelRequested) != 1 {
			t.Fatal("remaining branch must get cancel request")
		}
		st := eng.GroupStatus(0)
		if len(st.SelectedTokens) != 1 || st.SelectedTokens[0] != tok+":b0" {
			t.Fatalf("selected snapshot frozen: %v", st.SelectedTokens)
		}
		// 迟到结果仍记录，但不改变选定集合。
		if err := eng.DeliverBranchResult(tok+":b1", "late-win"); err != nil {
			t.Fatal(err)
		}
		st2 := eng.GroupStatus(0)
		if len(st2.SelectedTokens) != 1 || st2.Branches[1].InDecision || st2.Branches[1].Result != "late-win" {
			t.Fatalf("late result recorded but excluded from selection: %+v", st2)
		}
		evts2, _ := store.Load(runID)
		if countType(evts2, EvtGroupDecided) != 1 {
			t.Fatal("decision must stay singular")
		}
	})
	t.Run("collect remaining", func(t *testing.T) {
		dir := t.TempDir()
		store, _ := NewStore(dir)
		eng := NewEngine(store, systemClock{}, Hooks{})
		def := quorumDef(ModeAny, 0, false, true, []Branch{{Name: "a"}, {Name: "b"}})
		runID := "wg-collect"
		done := make(chan error, 1)
		go func() { done <- eng.Start(def, runID, nil) }()
		waitForGroupEvent(t, store, runID, EvtGroupOpened)
		tok := groupToken(runID, 0, "")
		_ = eng.DeliverBranchResult(tok+":b0", "first")
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		evts, _ := store.Load(runID)
		if countType(evts, EvtGroupCancelRequested) != 0 {
			t.Fatal("collect mode must not request cancellation")
		}
		st := eng.GroupStatus(0)
		if st.Branches[1].Terminal || st.Branches[1].CancelAsked {
			t.Fatalf("unfinished branch must stay open and un-canceled: %+v", st.Branches[1])
		}
		if err := eng.DeliverBranchResult(tok+":b1", "later"); err != nil {
			t.Fatal(err)
		}
		if got := eng.GroupStatus(0); got.Branches[1].Outcome != "success" || len(got.SelectedTokens) != 1 {
			t.Fatalf("collected outcome recorded without changing decision: %+v", got.Branches[1])
		}
	})
}

// 结果、分支超时、整组超时、取消同时竞争：每组只能形成一个决策。
func TestGroupRaceSingleDecision(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewStore(dir)
	def := Definition{ID: 11, Steps: []Step{{Kind: "group", Name: "g", Group: WaitGroup{
		Mode: ModeAny, TimeoutMS: 40, CollectRemaining: true,
		Branches: []Branch{{Name: "a", TimeoutMS: 20}, {Name: "b", TimeoutMS: 20}},
	}}}}
	eng := NewEngine(store, systemClock{}, Hooks{})
	runID := "wg-race"
	done := make(chan error, 1)
	go func() { done <- eng.Start(def, runID, nil) }()
	tok := groupToken(runID, 0, "")
	waitForGroupEvent(t, store, runID, EvtGroupOpened)
	time.Sleep(30 * time.Millisecond)
	_ = eng.CancelBranch(tok+":b0", "cancel-race")
	_ = eng.DeliverBranchResult(tok+":b1", "result-race")
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	evts, _ := store.Load(runID)
	if countType(evts, EvtGroupDecided) != 1 || countType(evts, EvtGroupCompleted) != 1 {
		t.Fatalf("exactly one decision: %v", typesOf(evts))
	}
	eng2, err := Recover(store, systemClock{}, def, runID, Hooks{})
	if err != nil {
		t.Fatal(err)
	}
	if err := eng2.Run(nil); err != nil {
		t.Fatal(err)
	}
	evts2, _ := store.Load(runID)
	if countType(evts2, EvtGroupDecided) != 1 {
		t.Fatal("recover must not re-decide")
	}
}

// 嵌套等待组：子组先形成自己的决策，再作为父组的一个分支参与裁决。
func TestNestedWaitGroups(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewStore(dir)
	inner := &WaitGroup{Mode: ModeAny, TimeoutMS: 5000, Branches: []Branch{{Name: "i0"}, {Name: "i1"}}}
	def := Definition{ID: 12, Steps: []Step{{Kind: "group", Name: "outer", Group: WaitGroup{
		Mode: ModeAll, CountFailures: true, TimeoutMS: 5000,
		Branches: []Branch{{Name: "child", Subgroup: inner}, {Name: "top"}},
	}}}}
	eng := NewEngine(store, systemClock{}, Hooks{})
	runID := "wg-nested"
	done := make(chan error, 1)
	go func() { done <- eng.Start(def, runID, nil) }()
	waitForGroupEvent(t, store, runID, EvtGroupOpened)
	innerTok := groupToken(runID, 0, "0")
	outerTok := groupToken(runID, 0, "")
	if err := eng.DeliverBranchResult(innerTok+":b0", "inner-win"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	if st := eng.GroupStatus(0); st.Decided {
		t.Fatal("outer must wait for all branches")
	}
	if err := eng.DeliverBranchFailure(outerTok+":b1", "top-failed"); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	st := eng.GroupStatus(0)
	if !st.Satisfied || !st.Groups[innerTok].Satisfied {
		t.Fatalf("nested decision lost: %+v", st)
	}
	found := false
	for _, s := range st.SelectedTokens {
		if s == innerTok {
			found = true
		}
	}
	if !found {
		t.Fatalf("subgroup must participate in outer decision: %v", st.SelectedTokens)
	}
}

// 部分分支重试：超时后重试同一分支；决策后重试被拒绝，迟到结果仍记录。
func TestBranchPartialRetry(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewStore(dir)
	def := Definition{ID: 13, Steps: []Step{{Kind: "group", Name: "g", Group: WaitGroup{
		Mode: ModeQuorum, Quota: 2, TimeoutMS: 5000, CollectRemaining: true,
		Branches: []Branch{{Name: "a"}, {Name: "b", TimeoutMS: 20}, {Name: "c"}},
	}}}}
	eng := NewEngine(store, systemClock{}, Hooks{})
	runID := "wg-retry"
	doneCh := make(chan error, 1)
	go func() { doneCh <- eng.Start(def, runID, nil) }()
	tok := groupToken(runID, 0, "")
	waitForGroupEvent(t, store, runID, EvtGroupOpened)
	time.Sleep(35 * time.Millisecond)
	if err := eng.RetryBranch(tok+":b1", nil); err != nil {
		t.Fatal(err)
	}
	if st := eng.GroupStatus(0); st.Branches[1].Attempt != 2 {
		t.Fatal("attempt must advance")
	}
	if err := eng.DeliverBranchResult(tok+":b0", "v0"); err != nil {
		t.Fatal(err)
	}
	if err := eng.DeliverBranchResult(tok+":b1", "v1-retry"); err != nil {
		t.Fatal(err)
	}
	if err := <-doneCh; err != nil {
		t.Fatal(err)
	}
	if err := eng.RetryBranch(tok+":b2", nil); !errors.Is(err, ErrGroupDecided) {
		t.Fatalf("retry after decision must fail, got %v", err)
	}
	if err := eng.DeliverBranchResult(tok+":b2", "late-c"); err != nil {
		t.Fatal(err)
	}
	evts, _ := store.Load(runID)
	if countType(evts, EvtGroupDecided) != 1 || countType(evts, EvtBranchRetried) != 1 {
		t.Fatal("bad history")
	}
}

type failAction struct{ name string }

func (a *failAction) Name() string { return a.name }
func (a *failAction) Invoke(runID string, stepIndex int, params map[string]string) (string, error) {
	return "", errors.New("delivery timeout")
}

// 分支 action 失败：外部效果已产生但未核实，单独保留，不自动重试、不消失。
func TestBranchUnverifiedEffectRetained(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewStore(dir)
	def := Definition{ID: 14, Steps: []Step{{Kind: "group", Name: "g", Group: WaitGroup{
		Mode: ModeAny, TimeoutMS: 5000, CollectRemaining: false,
		Branches: []Branch{{Name: "risky", Action: "risky"}, {Name: "plain"}},
	}}}}
	actions := map[string]Action{"risky": &failAction{name: "risky"}}
	eng := NewEngine(store, systemClock{}, Hooks{})
	err := eng.Start(def, "wg-unverified", actions)
	if !errors.Is(err, ErrUncertain) {
		t.Fatalf("want uncertain, got %v", err)
	}
	u := eng.UncertainEffects()
	if len(u) != 1 || u[0].Action != "risky" || u[0].Attempt != 1 {
		t.Fatalf("unexpected uncertain: %+v", u)
	}
	tok := groupToken("wg-unverified", 0, "")
	if err := eng.DeliverBranchResult(tok+":b1", "plain-win"); err != nil {
		t.Fatal(err)
	}
	if u2 := eng.UncertainEffects(); len(u2) != 1 {
		t.Fatalf("effect must survive group satisfaction: %+v", u2)
	}
	st := eng.GroupStatus(0)
	if !st.Branches[0].Unverified || !st.Branches[0].CancelAsked {
		t.Fatalf("unverified+cancel-asked expected: %+v", st.Branches[0])
	}
	if err := eng.RetryBranch(tok+":b0", actions); err == nil {
		t.Fatal("retry must be blocked until confirmed")
	}
	ok, err := eng.ConfirmBranchAction(tok+":b0", 1, "risky-verified")
	if err != nil || !ok {
		t.Fatalf("confirm: %v %v", ok, err)
	}
	if len(eng.UncertainEffects()) != 0 {
		t.Fatal("confirmed effect should clear")
	}
	evts, _ := store.Load("wg-unverified")
	if countType(evts, EvtBranchActionInvoked) != 1 {
		t.Fatal("no auto re-invoke allowed")
	}
}

var _ = atomic.LoadInt32

// 决策前保存/恢复：恢复后续等同一批分支，截止时间沿用，最终结果归属不变。
func TestGroupRecoverBeforeDecision(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewStore(dir)
	def := Definition{ID: 15, Steps: []Step{{Kind: "group", Name: "g", Group: WaitGroup{
		Mode: ModeQuorum, Quota: 2, TimeoutMS: 3600000, CollectRemaining: true,
		Branches: []Branch{{Name: "a"}, {Name: "b"}, {Name: "c"}},
	}}}}
	eng := NewEngine(store, systemClock{}, Hooks{})
	runID := "wg-rec-before"
	done := make(chan error, 1)
	go func() { done <- eng.Start(def, runID, nil) }()
	tok := groupToken(runID, 0, "")
	waitForGroupEvent(t, store, runID, EvtGroupOpened)
	if err := eng.DeliverBranchResult(tok+":b0", "v0"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	eng.Close()
	<-done
	eng2, err := Recover(store, systemClock{}, def, runID, Hooks{})
	if err != nil {
		t.Fatal(err)
	}
	st := eng2.GroupStatus(0)
	if st.Decided || !st.Branches[0].Terminal || st.Branches[0].Result != "v0" {
		t.Fatalf("bad recovered status: %+v", st)
	}
	done2 := make(chan error, 1)
	go func() { done2 <- eng2.Run(nil) }()
	time.Sleep(20 * time.Millisecond)
	if err := eng2.DeliverBranchResult(tok+":b1", "v1"); err != nil {
		t.Fatal(err)
	}
	if err := <-done2; err != nil {
		t.Fatal(err)
	}
	if got := eng2.GroupStatus(0); !got.Satisfied || len(got.SelectedTokens) != 2 {
		t.Fatalf("wrong decision after recovery: %+v", got)
	}
	evts, _ := store.Load(runID)
	if countType(evts, EvtGroupDecided) != 1 {
		t.Fatal("single decision across recovery")
	}
}

// 决策后保存/恢复：沿用已确认决策与结果归属，不重新裁决、不重跑动作。
func TestGroupRecoverAfterDecision(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewStore(dir)
	var calls int32
	def := Definition{ID: 16, Steps: []Step{
		{Kind: "group", Name: "g", Group: WaitGroup{Mode: ModeAny, TimeoutMS: 5000, Branches: []Branch{{Name: "a"}, {Name: "b"}}}},
		{Kind: "action", Name: "after", Action: "after"},
	}}
	actions := map[string]Action{"after": &countingAction{name: "after", calls: &calls, res: "done"}}
	eng := NewEngine(store, systemClock{}, Hooks{})
	runID := "wg-rec-after"
	done := make(chan error, 1)
	go func() { done <- eng.Start(def, runID, actions) }()
	tok := groupToken(runID, 0, "")
	waitForGroupEvent(t, store, runID, EvtGroupOpened)
	if err := eng.DeliverBranchResult(tok+":b0", "win"); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if atomic.LoadInt32(&calls) != 1 {
		t.Fatal("after step should run once")
	}
	eng2, err := Recover(store, systemClock{}, def, runID, Hooks{})
	if err != nil {
		t.Fatal(err)
	}
	if err := eng2.Run(actions); err != nil {
		t.Fatal(err)
	}
	if atomic.LoadInt32(&calls) != 1 {
		t.Fatal("after step must not rerun")
	}
	evts, _ := store.Load(runID)
	if countType(evts, EvtGroupDecided) != 1 || countType(evts, EvtRunCompleted) != 1 {
		t.Fatal("history mismatch")
	}
}

// 组内崩溃：分支 action 已触发、ack 前崩溃；恢复后不重新触发，
// 未核实效果保留，人工确认后流程继续。
func TestGroupCrashUnverifiedThenConfirm(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewStore(dir)
	var calls int32
	def := Definition{ID: 17, Steps: []Step{{Kind: "group", Name: "g", Group: WaitGroup{
		Mode: ModeAll, TimeoutMS: 5000, Branches: []Branch{{Name: "charge", Action: "charge"}, {Name: "note"}},
	}}}}
	actions := map[string]Action{"charge": &countingAction{name: "charge", calls: &calls, res: "charged"}}
	eng1 := NewEngine(store, systemClock{}, Hooks{CrashAfterInvoke: func(runID string, stepIndex int) bool { return stepIndex == 0 }})
	err := eng1.Start(def, "wg-crash", actions)
	if !errors.Is(err, ErrCrashed) {
		t.Fatalf("want crash, got %v", err)
	}
	if atomic.LoadInt32(&calls) != 1 {
		t.Fatal("side effect happened once")
	}
	eng2, rerr := Recover(store, systemClock{}, def, "wg-crash", Hooks{})
	if rerr != nil {
		t.Fatal(rerr)
	}
	if err := eng2.Run(actions); !errors.Is(err, ErrUncertain) {
		t.Fatalf("want uncertain after recover, got %v", err)
	}
	if atomic.LoadInt32(&calls) != 1 {
		t.Fatal("must not re-invoke")
	}
	u := eng2.UncertainEffects()
	if len(u) != 1 {
		t.Fatalf("unverified retained: %+v", u)
	}
	tok := groupToken("wg-crash", 0, "")
	if ok, err := eng2.ConfirmBranchAction(tok+":b0", 1, "charged-verified"); err != nil || !ok {
		t.Fatalf("confirm %v %v", ok, err)
	}
	done := make(chan error, 1)
	go func() { done <- eng2.Run(actions) }()
	time.Sleep(20 * time.Millisecond)
	if err := eng2.DeliverBranchResult(tok+":b1", "note"); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if atomic.LoadInt32(&calls) != 1 {
		t.Fatal("charge still once")
	}
	evts, _ := store.Load("wg-crash")
	if countType(evts, EvtBranchActionInvoked) != 1 || countType(evts, EvtGroupDecided) != 1 {
		t.Fatal("bad history")
	}
}
