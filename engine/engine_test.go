package engine

import (
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// countingAction 记录真实副作用次数，用于证明"没有重复执行"。
type countingAction struct {
	name  string
	calls *int32
	res   string
	err   error
}

func (a *countingAction) Name() string { return a.name }
func (a *countingAction) Invoke(runID string, stepIndex int, params map[string]string) (string, error) {
	atomic.AddInt32(a.calls, 1)
	return a.res, a.err
}

func typesOf(events []Event) []EventType {
	var out []EventType
	for _, e := range events {
		out = append(out, e.Type)
	}
	return out
}

func countType(events []Event, t EventType) int {
	n := 0
	for _, e := range events {
		if e.Type == t {
			n++
		}
	}
	return n
}

// 场景一：执行到一半退出 —— 动作已 Invoke、确认落盘前崩溃。
// 重建执行器后：不得重试动作，报告不确定状态，核实确认后继续完成。
func TestCrashMidRunUncertainThenConfirm(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewStore(dir)
	def := Definition{ID: 1, Name: "order", Steps: []Step{
		{Kind: "action", Name: "charge", Action: "charge"},
		{Kind: "action", Name: "ship", Action: "ship"},
	}}
	var chargeCalls, shipCalls int32
	actions := map[string]Action{
		"charge": &countingAction{name: "charge", calls: &chargeCalls, res: "charged-100"},
		"ship":   &countingAction{name: "ship", calls: &shipCalls, res: "shipped"},
	}

	// 第一个"进程"：在 ship 确认前崩溃。
	eng1 := NewEngine(store, systemClock{}, Hooks{
		CrashAfterInvoke: func(runID string, stepIndex int) bool { return stepIndex == 1 },
	})
	err := eng1.Start(def, "r1", actions)
	if !errors.Is(err, ErrCrashed) {
		t.Fatalf("expected crash, got %v", err)
	}
	if atomic.LoadInt32(&shipCalls) != 1 {
		t.Fatalf("ship side effect should have happened once, got %d", shipCalls)
	}

	// 第二个"进程"：从记录恢复。
	eng2, err := Recover(store, systemClock{}, def, "r1", Hooks{})
	if err != nil {
		t.Fatal(err)
	}
	pending := eng2.Pending()
	if len(pending) == 0 || !strings.Contains(pending[0], "unconfirmed") {
		t.Fatalf("expected uncertain-action pending item, got %v", pending)
	}
	if err := eng2.Run(actions); !errors.Is(err, ErrUncertain) {
		t.Fatalf("recovered run must refuse to retry, got %v", err)
	}
	if atomic.LoadInt32(&shipCalls) != 1 {
		t.Fatalf("ship must NOT be retried, got %d calls", shipCalls)
	}

	// 人工核实：ship 确实已发货，补确认；重复确认只生效一次。
	ok, err := eng2.ConfirmAction(1, "shipped-verified")
	if err != nil || !ok {
		t.Fatalf("confirm failed: ok=%v err=%v", ok, err)
	}
	ok2, _ := eng2.ConfirmAction(1, "shipped-verified")
	if ok2 {
		t.Fatal("duplicate confirm must be a no-op")
	}
	if err := eng2.Run(actions); err != nil {
		t.Fatalf("run should complete after confirm, got %v", err)
	}

	events, _ := store.Load("r1")
	if n := countType(events, EvtActionInvoked); n != 2 {
		t.Fatalf("each action invoked exactly once, got %d invokes", n)
	}
	if n := countType(events, EvtActionAcked); n != 2 {
		t.Fatalf("expected 2 acks, got %d", n)
	}
	if n := countType(events, EvtRunCompleted); n != 1 {
		t.Fatalf("run must complete exactly once, got %d", n)
	}
	if got := eng2.Pending(); len(got) != 1 || !strings.Contains(got[0], "nothing pending") {
		t.Fatalf("unexpected pending after completion: %v", got)
	}
}

// 场景二：等待超时与结果几乎同时到达 —— 历史按实际发生顺序记录，
// 先到者决定步骤结果，迟到结果仍补记；重复投递记 duplicate。
func TestWaitTimeoutAndResultRace(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewStore(dir)
	def := Definition{ID: 2, Name: "await-payment", Steps: []Step{
		{Kind: "wait", Name: "wait-payment", TimeoutMS: 60},
		{Kind: "action", Name: "close", Action: "close"},
	}}
	var closeCalls int32
	actions := map[string]Action{"close": &countingAction{name: "close", calls: &closeCalls, res: "closed"}}

	eng := NewEngine(store, systemClock{}, Hooks{})
	done := make(chan error, 1)
	go func() { done <- eng.Start(def, "r2", actions) }()

	// 等 wait 调度出来后，在超时边界附近投递结果。
	deadline := time.Now().Add(60 * time.Millisecond)
	for {
		events, _ := store.Load("r2")
		if countType(events, EvtWaitScheduled) == 1 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	time.Sleep(time.Until(deadline)) // 贴着超时点投递
	token := "r2:wait:0"
	if err := eng.DeliverResult(token, "paid-999"); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	// 等待已结束后再重复投递同一结果：只记 duplicate，不影响流程。
	if err := eng.DeliverResult(token, "paid-999"); err != nil {
		t.Fatal(err)
	}

	events, _ := store.Load("r2")
	fired := countType(events, EvtWaitFired)
	timedOut := countType(events, EvtWaitTimedOut)
	if fired+timedOut != 1 {
		t.Fatalf("exactly one of fired/timed_out may decide the step, got fired=%d timedOut=%d", fired, timedOut)
	}
	if n := countType(events, EvtExternalResult); n != 1 {
		t.Fatalf("result recorded exactly once, got %d", n)
	}
	if n := countType(events, EvtExternalDuplicate); n != 1 {
		t.Fatalf("duplicate recorded exactly once, got %d", n)
	}
	// 序号必须严格递增 —— 历史顺序即实际发生顺序。
	for i := 1; i < len(events); i++ {
		if events[i].Seq <= events[i-1].Seq {
			t.Fatalf("history out of order at %d", i)
		}
	}
	if countType(events, EvtRunCompleted) != 1 {
		t.Fatal("run should complete")
	}

	// 重建执行器：已完成，无待处理。
	eng2, _ := Recover(store, systemClock{}, def, "r2", Hooks{})
	if got := eng2.Pending(); len(got) != 1 || !strings.Contains(got[0], "nothing pending") {
		t.Fatalf("unexpected pending: %v", got)
	}
}

// 场景三：确认丢失 —— 动作返回成功但 ack 未落盘（崩溃），
// 恢复后处于不确定状态；另测等待中途崩溃后按原截止时间续等。
func TestLostAckAndWaitRecovery(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewStore(dir)
	def := Definition{ID: 3, Name: "mixed", Steps: []Step{
		{Kind: "action", Name: "reserve", Action: "reserve"},
		{Kind: "wait", Name: "wait-confirm", TimeoutMS: 24 * 3600 * 1000}, // 长等待
		{Kind: "action", Name: "finish", Action: "finish"},
	}}
	var reserveCalls, finishCalls int32
	actions := map[string]Action{
		"reserve": &countingAction{name: "reserve", calls: &reserveCalls, res: "reserved"},
		"finish":  &countingAction{name: "finish", calls: &finishCalls, res: "done"},
	}

	// 进程一：reserve 成功返回，但 ack 落盘前崩溃（确认丢失）。
	eng1 := NewEngine(store, systemClock{}, Hooks{
		CrashAfterInvoke: func(runID string, stepIndex int) bool { return stepIndex == 0 },
	})
	if err := eng1.Start(def, "r3", actions); !errors.Is(err, ErrCrashed) {
		t.Fatalf("expected crash, got %v", err)
	}

	// 进程二：恢复 -> 不确定，核实后继续，进入长等待。
	eng2, _ := Recover(store, systemClock{}, def, "r3", Hooks{})
	if err := eng2.Run(actions); !errors.Is(err, ErrUncertain) {
		t.Fatalf("expected uncertain, got %v", err)
	}
	if atomic.LoadInt32(&reserveCalls) != 1 {
		t.Fatal("reserve must not be re-executed")
	}
	if ok, _ := eng2.ConfirmAction(0, "reserved-verified"); !ok {
		t.Fatal("confirm should apply once")
	}
	done := make(chan error, 1)
	go func() { done <- eng2.Run(actions) }()
	waitFor := func(pred func([]Event) bool, what string) {
		t.Helper()
		for i := 0; i < 2000; i++ {
			events, _ := store.Load("r3")
			if pred(events) {
				return
			}
			time.Sleep(time.Millisecond)
		}
		t.Fatalf("timed out waiting for %s", what)
	}
	waitFor(func(e []Event) bool { return countType(e, EvtWaitScheduled) == 1 }, "wait scheduled")

	// 进程二在等待中"退出"（丢弃 eng2，不投递结果）。
	// 进程三：恢复 -> 应识别出进行中的等待及其截止时间，继续等同一个 token。
	eng3, _ := Recover(store, systemClock{}, def, "r3", Hooks{})
	pending := eng3.Pending()
	joined := strings.Join(pending, "; ")
	if !strings.Contains(joined, "wait") {
		t.Fatalf("pending should mention the outstanding wait, got %v", pending)
	}
	done3 := make(chan error, 1)
	go func() { done3 <- eng3.Run(actions) }()
	// 结果到达（只投递一次），等待被满足，流程跑完。
	if err := eng3.DeliverResult("r3:wait:1", "confirmed"); err != nil {
		t.Fatal(err)
	}
	if err := <-done3; err != nil {
		t.Fatal(err)
	}

	events, _ := store.Load("r3")
	if n := countType(events, EvtActionInvoked); n != 2 {
		t.Fatalf("reserve+finish invoked once each, got %d invokes", n)
	}
	if n := countType(events, EvtWaitFired); n != 1 {
		t.Fatalf("wait fired once, got %d", n)
	}
	if countType(events, EvtWaitTimedOut) != 0 {
		t.Fatal("wait must not time out after recovery")
	}
	if countType(events, EvtRunCompleted) != 1 {
		t.Fatal("run must complete")
	}
	if atomic.LoadInt32(&finishCalls) != 1 {
		t.Fatal("finish executed once")
	}
	_ = typesOf // keep helper referenced if unused elsewhere
}

// startWaitRun 启动一个以 wait 开头的 run，并等到 wait_scheduled 落盘。
func startWaitRun(t *testing.T, store *Store, def Definition, runID string, actions map[string]Action, hooks Hooks) (*Engine, chan error) {
	t.Helper()
	eng := NewEngine(store, systemClock{}, hooks)
	done := make(chan error, 1)
	go func() { done <- eng.Start(def, runID, actions) }()
	for i := 0; i < 2000; i++ {
		events, _ := store.Load(runID)
		if countType(events, EvtWaitScheduled) == 1 {
			return eng, done
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("wait never scheduled")
	return nil, nil
}

func waitForEvent(t *testing.T, store *Store, runID string, pred func([]Event) bool, what string) {
	t.Helper()
	for i := 0; i < 2000; i++ {
		events, _ := store.Load(runID)
		if pred(events) {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func assertStrictlyOrdered(t *testing.T, events []Event) {
	t.Helper()
	for i := 1; i < len(events); i++ {
		if events[i].Seq <= events[i-1].Seq {
			t.Fatalf("history out of order at %d", i)
		}
	}
}

func assertOneDecision(t *testing.T, events []Event) {
	t.Helper()
	n := countType(events, EvtWaitFired) + countType(events, EvtWaitTimedOut) + countType(events, EvtWaitCancelled)
	if n != 1 {
		t.Fatalf("exactly one wait decision expected, got %d (fired=%d timedOut=%d cancelled=%d)",
			n, countType(events, EvtWaitFired), countType(events, EvtWaitTimedOut), countType(events, EvtWaitCancelled))
	}
	c := 0
	for _, ev := range events {
		if ev.Type == EvtStepCompleted && ev.StepIndex == 0 {
			c++
		}
	}
	if c != 1 {
		t.Fatalf("wait step completed exactly once, got %d", c)
	}
}

func waitDef(timeoutMS int64) Definition {
	return Definition{ID: 10, Name: "race", Steps: []Step{
		{Kind: "wait", Name: "w", TimeoutMS: timeoutMS},
		{Kind: "action", Name: "after", Action: "after"},
	}}
}

// 结果先到：取消与迟到的重复决定都不得生效。
func TestDecisionResultFirst(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewStore(dir)
	def := waitDef(60 * 1000)
	var calls int32
	actions := map[string]Action{"after": &countingAction{name: "after", calls: &calls, res: "ok"}}

	eng, done := startWaitRun(t, store, def, "rf", actions, Hooks{})
	if err := eng.DeliverResult("rf:wait:0", "v1"); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if ok, _ := eng.CancelWait(0, "too late"); ok {
		t.Fatal("cancel after result must not win")
	}

	events, _ := store.Load("rf")
	assertStrictlyOrdered(t, events)
	assertOneDecision(t, events)
	if countType(events, EvtWaitFired) != 1 {
		t.Fatal("result should decide the wait")
	}
	// 历史必须保留真实顺序：external_result 在 wait_fired 之前。
	fireIdx, resIdx := -1, -1
	for i, ev := range events {
		if ev.Type == EvtWaitFired {
			fireIdx = i
		}
		if ev.Type == EvtExternalResult {
			resIdx = i
		}
	}
	if resIdx < 0 || fireIdx < 0 || resIdx > fireIdx {
		t.Fatalf("external_result must precede wait_fired: res=%d fired=%d", resIdx, fireIdx)
	}
	if atomic.LoadInt32(&calls) != 1 {
		t.Fatal("next step advanced exactly once")
	}
}

// 超时先到：迟到的结果只补记为 late，不得把等待改记为正常完成。
func TestDecisionTimeoutFirst(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewStore(dir)
	def := waitDef(1)
	var calls int32
	actions := map[string]Action{"after": &countingAction{name: "after", calls: &calls, res: "ok"}}

	eng, done := startWaitRun(t, store, def, "tf", actions, Hooks{})
	waitForEvent(t, store, "tf", func(e []Event) bool { return countType(e, EvtWaitTimedOut) == 1 }, "timeout")
	if err := eng.DeliverResult("tf:wait:0", "late-result"); err != nil {
		t.Fatal(err)
	}
	if ok, _ := eng.CancelWait(0, "too late"); ok {
		t.Fatal("cancel after timeout must not win")
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}

	events, _ := store.Load("tf")
	assertStrictlyOrdered(t, events)
	assertOneDecision(t, events)
	if countType(events, EvtWaitFired) != 0 {
		t.Fatal("timed-out wait must never be recorded as fired")
	}
	var late *Event
	for i, ev := range events {
		if ev.Type == EvtExternalResult {
			late = &events[i]
		}
	}
	if late == nil || late.Detail != "late" {
		t.Fatalf("late result must be recorded as late, got %+v", late)
	}
	if atomic.LoadInt32(&calls) != 1 {
		t.Fatal("next step advanced exactly once")
	}
}

// 取消先到：结果变迟到，超时不再发生。
func TestDecisionCancelFirst(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewStore(dir)
	def := waitDef(60 * 1000)
	var calls int32
	actions := map[string]Action{"after": &countingAction{name: "after", calls: &calls, res: "ok"}}

	eng, done := startWaitRun(t, store, def, "cf", actions, Hooks{})
	if ok, err := eng.CancelWait(0, "user abort"); err != nil || !ok {
		t.Fatalf("cancel should win: ok=%v err=%v", ok, err)
	}
	if ok, _ := eng.CancelWait(0, "again"); ok {
		t.Fatal("second cancel must be a no-op")
	}
	if err := eng.DeliverResult("cf:wait:0", "too-late"); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}

	events, _ := store.Load("cf")
	assertStrictlyOrdered(t, events)
	assertOneDecision(t, events)
	if countType(events, EvtWaitCancelled) != 1 || countType(events, EvtWaitTimedOut) != 0 {
		t.Fatal("cancel should be the single decision, no timeout")
	}
	if atomic.LoadInt32(&calls) != 1 {
		t.Fatal("next step advanced exactly once after cancel")
	}
}

// 贴着边界的并发竞争：反复跑，三种竞争者同时压上，
// 每次都必须恰好一个决定、恰好一次推进、历史严格有序。
func TestDecisionRaceStress(t *testing.T) {
	for i := 0; i < 50; i++ {
		dir := t.TempDir()
		store, _ := NewStore(dir)
		runID := "stress"
		def := waitDef(2)
		var calls int32
		actions := map[string]Action{"after": &countingAction{name: "after", calls: &calls, res: "ok"}}

		eng, done := startWaitRun(t, store, def, runID, actions, Hooks{})
		go eng.DeliverResult(runID+":wait:0", "v")
		go eng.CancelWait(0, "race")
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		events, _ := store.Load(runID)
		assertStrictlyOrdered(t, events)
		assertOneDecision(t, events)
		if n := countType(events, EvtRunCompleted); n != 1 {
			t.Fatalf("iter %d: run completed once, got %d", i, n)
		}
		if atomic.LoadInt32(&calls) != 1 {
			t.Fatalf("iter %d: next step advanced exactly once", i)
		}
	}
}

// 保存点落在"超时决定已落盘、step_completed 未落盘"之间：
// 恢复后不得重新等待或重复推进，应与已有历史相容。
func TestRecoverBetweenTimeoutAndAdvance(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewStore(dir)
	def := waitDef(1000)
	deadline := time.Now().Add(time.Hour).UnixMilli()
	mk := func(seq int64, typ EventType, step int, token string) Event {
		return Event{Seq: seq, RunID: "sp1", DefID: def.ID, Type: typ, StepIndex: step, Token: token, At: 1}
	}
	store.Append(mk(1, EvtRunStarted, 0, ""))
	store.Append(mk(2, EvtStepStarted, 0, ""))
	ev := mk(3, EvtWaitScheduled, 0, "sp1:wait:0")
	ev.Deadline = deadline
	store.Append(ev)
	store.Append(mk(4, EvtWaitTimedOut, 0, "sp1:wait:0")) // 决定已落盘，step_completed 丢失

	eng, err := Recover(store, systemClock{}, def, "sp1", Hooks{})
	if err != nil {
		t.Fatal(err)
	}
	if eng.HasPendingWait() {
		t.Fatal("decided wait must not be re-armed after recovery")
	}
	var calls int32
	actions := map[string]Action{"after": &countingAction{name: "after", calls: &calls, res: "ok"}}
	if err := eng.Run(actions); err != nil {
		t.Fatal(err)
	}
	events, _ := store.Load("sp1")
	if n := countType(events, EvtWaitTimedOut); n != 1 {
		t.Fatalf("timeout recorded once, got %d", n)
	}
	if n := countType(events, EvtWaitScheduled); n != 1 {
		t.Fatalf("wait must not be re-scheduled, got %d", n)
	}
	if atomic.LoadInt32(&calls) != 1 {
		t.Fatal("next step advanced exactly once")
	}
	if countType(events, EvtRunCompleted) != 1 {
		t.Fatal("run must complete")
	}
}

// 保存点落在"结果已落盘、wait_fired 未落盘"之间：
// 恢复后应沿用原 token 续等并消费已到的结果，不重复投递。
func TestRecoverBetweenResultAndFire(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewStore(dir)
	def := waitDef(1000)
	deadline := time.Now().Add(time.Hour).UnixMilli()
	mk := func(seq int64, typ EventType, step int, token string) Event {
		return Event{Seq: seq, RunID: "sp2", DefID: def.ID, Type: typ, StepIndex: step, Token: token, At: 1}
	}
	store.Append(mk(1, EvtRunStarted, 0, ""))
	store.Append(mk(2, EvtStepStarted, 0, ""))
	ev := mk(3, EvtWaitScheduled, 0, "sp2:wait:0")
	ev.Deadline = deadline
	store.Append(ev)
	res := mk(4, EvtExternalResult, 0, "sp2:wait:0")
	res.Result = "paid"
	store.Append(res) // 结果已落盘，wait_fired 丢失

	eng, err := Recover(store, systemClock{}, def, "sp2", Hooks{})
	if err != nil {
		t.Fatal(err)
	}
	token, _, ok := eng.WaitToken()
	if !ok || token != "sp2:wait:0" {
		t.Fatalf("recovered wait should be pending on same token, got %q ok=%v", token, ok)
	}
	var calls int32
	actions := map[string]Action{"after": &countingAction{name: "after", calls: &calls, res: "ok"}}
	if err := eng.Run(actions); err != nil {
		t.Fatal(err)
	}
	events, _ := store.Load("sp2")
	if n := countType(events, EvtExternalResult); n != 1 {
		t.Fatalf("result recorded once, got %d", n)
	}
	if n := countType(events, EvtWaitFired); n != 1 {
		t.Fatalf("wait fired once after recovery, got %d", n)
	}
	if n := countType(events, EvtWaitScheduled); n != 1 {
		t.Fatalf("wait must not be re-scheduled, got %d", n)
	}
	if atomic.LoadInt32(&calls) != 1 {
		t.Fatal("next step advanced exactly once")
	}
}

// 保存点落在"ack 已落盘、step_completed 未落盘"之间：
// 恢复后动作不得重跑。
func TestRecoverBetweenAckAndComplete(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewStore(dir)
	def := Definition{ID: 11, Name: "ack-gap", Steps: []Step{
		{Kind: "action", Name: "charge", Action: "charge"},
		{Kind: "action", Name: "ship", Action: "ship"},
	}}
	mk := func(seq int64, typ EventType, step int) Event {
		return Event{Seq: seq, RunID: "sp3", DefID: def.ID, Type: typ, StepIndex: step, At: 1}
	}
	store.Append(mk(1, EvtRunStarted, 0))
	store.Append(mk(2, EvtStepStarted, 0))
	inv := mk(3, EvtActionInvoked, 0)
	inv.Key = "sp3:action:0"
	store.Append(inv)
	ack := mk(4, EvtActionAcked, 0)
	ack.Key = "sp3:action:0"
	ack.Result = "charged"
	store.Append(ack) // ack 已落盘，step_completed 丢失

	eng, err := Recover(store, systemClock{}, def, "sp3", Hooks{})
	if err != nil {
		t.Fatal(err)
	}
	if u := eng.Uncertain(); u != nil {
		t.Fatalf("acked action must not be uncertain, got %+v", u)
	}
	var chargeCalls, shipCalls int32
	actions := map[string]Action{
		"charge": &countingAction{name: "charge", calls: &chargeCalls, res: "charged"},
		"ship":   &countingAction{name: "ship", calls: &shipCalls, res: "shipped"},
	}
	if err := eng.Run(actions); err != nil {
		t.Fatal(err)
	}
	if atomic.LoadInt32(&chargeCalls) != 0 {
		t.Fatal("acked action must not be re-executed")
	}
	if atomic.LoadInt32(&shipCalls) != 1 {
		t.Fatal("ship executed once")
	}
}

// 不确定动作查询：恢复后、确认前可查询；确认后清空。
func TestUncertainQuery(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewStore(dir)
	def := Definition{ID: 12, Name: "q", Steps: []Step{
		{Kind: "action", Name: "pay", Action: "pay"},
	}}
	var calls int32
	actions := map[string]Action{"pay": &countingAction{name: "pay", calls: &calls, res: "paid"}}

	eng1 := NewEngine(store, systemClock{}, Hooks{
		CrashAfterInvoke: func(runID string, stepIndex int) bool { return true },
	})
	if err := eng1.Start(def, "q1", actions); !errors.Is(err, ErrCrashed) {
		t.Fatalf("expected crash, got %v", err)
	}
	eng2, _ := Recover(store, systemClock{}, def, "q1", Hooks{})
	u := eng2.Uncertain()
	if u == nil || u.StepIndex != 0 || u.Action != "pay" || u.Key != "q1:action:0" {
		t.Fatalf("uncertain action should be queryable, got %+v", u)
	}
	if err := eng2.Run(actions); !errors.Is(err, ErrUncertain) {
		t.Fatalf("must refuse to re-run side-effecting step, got %v", err)
	}
	if atomic.LoadInt32(&calls) != 1 {
		t.Fatal("no duplicate side effect while uncertain")
	}
	if ok, err := eng2.ConfirmAction(0, "paid-verified"); err != nil || !ok {
		t.Fatalf("confirm: ok=%v err=%v", ok, err)
	}
	if u := eng2.Uncertain(); u != nil {
		t.Fatalf("uncertain cleared after confirm, got %+v", u)
	}
	if err := eng2.Run(actions); err != nil {
		t.Fatal(err)
	}
}
