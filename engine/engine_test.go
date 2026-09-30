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
