package engine

import (
	"errors"
	"path/filepath"
	"testing"
	"time"
)

// fakeExternalService 模拟真实外部系统：
// Invoke 一旦执行，副作用在外部已经落账；即使调用方随后崩溃、确认丢失，
// 也可以用 Lookup 核实结果，且不会因为再次调用而重复扣款/重复发货。
type fakeExternalService struct {
	calls int
}

func (f *fakeExternalService) Name() string { return "payment" }

func (f *fakeExternalService) Invoke(runID string, stepIndex int, params map[string]string) (string, error) {
	f.calls++
	return "paid:" + runID, nil
}

func (f *fakeExternalService) Lookup(runID string) (string, bool) {
	return "paid:" + runID, f.calls > 0
}

type manualClock struct{ t time.Time }

func (c *manualClock) Now() time.Time { return c.t }

func (c *manualClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func sampleDef() Definition {
	return Definition{
		ID:   1,
		Name: "order-flow",
		Steps: []Step{
			{Kind: "action", Name: "charge", Action: "payment"},
			{Kind: "wait", Name: "approval", TimeoutMS: 1000},
			{Kind: "action", Name: "ship", Action: "payment"},
		},
	}
}

func newFileEngine(t *testing.T, def Definition, clk Clock) (*Engine, *FileStore) {
	t.Helper()
	store, err := OpenFileStore(filepath.Join(t.TempDir(), "history.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	eng, err := NewEngine(def, store, clk)
	if err != nil {
		t.Fatal(err)
	}
	return eng, store
}

func reopen(t *testing.T, def Definition, store Store, clk Clock) *Engine {
	t.Helper()
	eng, err := NewEngine(def, store, clk)
	if err != nil {
		t.Fatal(err)
	}
	return eng
}

// 场景一：执行到一半退出（外部动作已发出、确认前崩溃）。
// 恢复后必须报告不确定动作，核实后继续，外部副作用只发生一次。
func TestCrashBeforeAckThenVerifyAndResume(t *testing.T) {
	svc := &fakeExternalService{}
	actions := map[string]Action{"payment": svc}
	clk := &manualClock{t: time.UnixMilli(0)}
	def := sampleDef()
	eng, store := newFileEngine(t, def, clk)

	// 在 action_invoked 落盘后立刻“杀死进程”：外部调用已发生，ack 未写。
	eng.SetCrashHook(func(evt Event) bool { return evt.Type == EvtActionInvoked })
	func() {
		defer func() {
			if r := recover(); !errors.Is(r.(error), Crash) {
				t.Fatalf("expect crash panic, got %v", r)
			}
		}()
		_ = eng.Advance(actions)
	}()
	if svc.calls != 1 {
		t.Fatalf("external action should have happened once, calls=%d", svc.calls)
	}

	// 重新创建执行器（新进程），从文件历史恢复。
	eng2 := reopen(t, def, store, clk)
	st := eng2.Status()
	if st.CompletedSteps != 0 || len(st.UncertainActions) != 1 {
		t.Fatalf("unexpected status: %+v", st)
	}
	if st.UncertainActions[0].Name != "charge" {
		t.Fatalf("uncertain item should be charge: %+v", st.UncertainActions[0])
	}

	// 不能直接重复执行：Advance 返回 unknown 错误，且不会再次调用外部。
	if err := eng2.Advance(actions); !errors.Is(err, ErrActionUnknown) {
		t.Fatalf("expect ErrActionUnknown, got %v", err)
	}
	if svc.calls != 1 {
		t.Fatalf("action must not be retried before verification, calls=%d", svc.calls)
	}

	// 向外部系统核实：副作用确实发生过，用查到的结果补登记。
	token := st.UncertainActions[0].Token
	result, occurred := svc.Lookup(eng2.RunID())
	if !occurred {
		t.Fatal("external service should show the charge")
	}
	if err := eng2.VerifyAction(token, result, true); err != nil {
		t.Fatal(err)
	}

	// 继续推进到等待步骤。
	if err := eng2.Advance(actions); !errors.Is(err, ErrWaitPending) {
		t.Fatalf("expect ErrWaitPending, got %v", err)
	}
	waitToken, ok := eng2.WaitToken(1)
	if !ok {
		t.Fatal("wait token should exist")
	}

	// 再重建一次：进度仍一致（charge 完成、approval 在等）。
	eng3 := reopen(t, def, store, clk)
	st3 := eng3.Status()
	if st3.CompletedSteps != 1 || len(st3.PendingWaits) != 1 || len(st3.UncertainActions) != 0 {
		t.Fatalf("restored progress mismatch: %+v", st3)
	}
	if st3.PendingWaits[0].TimedOut {
		t.Fatal("wait should not be timed out yet")
	}

	// 结果到达，走完最后一步；外部 payment 总共只被调用两次（charge + ship）。
	if err := eng3.DeliverResult(waitToken, "approved"); err != nil {
		t.Fatal(err)
	}
	if err := eng3.Advance(actions); err != nil {
		t.Fatal(err)
	}
	if !eng3.Status().Done {
		t.Fatal("run should be completed")
	}
	if svc.calls != 2 {
		t.Fatalf("expect exactly 2 external calls, got %d", svc.calls)
	}

	// 历史按实际发生顺序落盘，且 run 完成后重建仍是完成态。
	eng4 := reopen(t, def, store, clk)
	if !eng4.Status().Done {
		t.Fatal("completed state must survive recreation")
	}
}

// 场景二：等待超时后结果仍然到达（迟到结果），以及超时与结果在同一刻到达。
func TestWaitTimeoutThenLateResultAndDuplicate(t *testing.T) {
	svc := &fakeExternalService{}
	actions := map[string]Action{"payment": svc}
	clk := &manualClock{t: time.UnixMilli(0)}
	def := Definition{
		ID: 9, Name: "w",
		Steps: []Step{{Kind: "wait", Name: "approval", TimeoutMS: 1000}},
	}
	eng, store := newFileEngine(t, def, clk)
	if err := eng.Advance(actions); !errors.Is(err, ErrWaitPending) {
		t.Fatal(err)
	}
	token, _ := eng.WaitToken(0)

	// 推进时钟直到超时：等待仍未决，只是标记 timed_out。
	clk.advance(time.Second)
	if err := eng.CheckTimeouts(); err != nil {
		t.Fatal(err)
	}
	st := eng.Status()
	if len(st.PendingWaits) != 1 || !st.PendingWaits[0].TimedOut {
		t.Fatalf("timeout should keep wait pending: %+v", st)
	}

	// 迟到结果仍然可以完成等待，而不是重新发起。
	if err := eng.DeliverResult(token, "approved-late"); err != nil {
		t.Fatal(err)
	}
	if err := eng.Advance(actions); err != nil {
		t.Fatal(err)
	}

	// 重复投递同一结果：只多一条 duplicate 审计，流程状态不变。
	eventsBefore, _ := store.Read()
	if err := eng.DeliverResult(token, "approved-again"); err != nil {
		t.Fatal(err)
	}
	eventsAfter, _ := store.Read()
	if len(eventsAfter) != len(eventsBefore)+1 {
		t.Fatalf("duplicate should append exactly one event")
	}
	if eventsAfter[len(eventsAfter)-1].Type != EvtExternalDuplicate {
		t.Fatalf("expected duplicate event, got %s", eventsAfter[len(eventsAfter)-1].Type)
	}
}

// 场景二补充：超时与结果“同时到达”。两种输入顺序都应收敛到
// 同一进度（等待完成），且实际结果生效、超时只作留痕或不再阻止。
func TestTimeoutAndResultArriveTogether(t *testing.T) {
	sameMoment := func(markTimeoutFirst bool) *Engine {
		svc := &fakeExternalService{}
		actions := map[string]Action{"payment": svc}
		clk := &manualClock{t: time.UnixMilli(0)}
		def := Definition{
			ID: 9, Name: "w",
			Steps: []Step{{Kind: "wait", Name: "approval", TimeoutMS: 1000}},
		}
		eng, _ := newFileEngine(t, def, clk)
		_ = eng.Advance(actions)
		token, _ := eng.WaitToken(0)
		clk.advance(time.Second)

		if markTimeoutFirst {
			if err := eng.CheckTimeouts(); err != nil {
				t.Fatal(err)
			}
			if err := eng.DeliverResult(token, "ok"); err != nil {
				t.Fatal(err)
			}
		} else {
			if err := eng.DeliverResult(token, "ok"); err != nil {
				t.Fatal(err)
			}
			// 结果已生效后，超时检查必须是无害的：不能复活已完成的等待。
			if err := eng.CheckTimeouts(); err != nil {
				t.Fatal(err)
			}
		}
		if err := eng.Advance(actions); err != nil {
			t.Fatal(err)
		}
		return eng
	}
	if !sameMoment(true).Status().Done || !sameMoment(false).Status().Done {
		t.Fatal("both arrival orders must converge to completed")
	}
}

// 场景三：确认丢失（ack 未写），重建后核实为“未发生”，才允许真正执行一次。
func TestVerifiedNotOccurredAllowsSingleRetry(t *testing.T) {
	svc := &fakeExternalService{}
	actions := map[string]Action{"payment": svc}
	clk := &manualClock{t: time.UnixMilli(0)}
	def := Definition{
		ID: 3, Name: "a", Steps: []Step{{Kind: "action", Name: "charge", Action: "payment"}},
	}
	eng, store := newFileEngine(t, def, clk)

	// 构造“调用记录存在但外部其实没执行”的历史（如网络请求未到达服务器）。
	eng.SetCrashHook(func(evt Event) bool { return evt.Type == EvtActionInvoked })
	func() {
		defer func() { _ = recover() }()
		_ = eng.Advance(actions)
	}()
	// 手工把外部服务状态还原为“未发生”（请求实际没到）。
	svc.calls = 0

	eng2 := reopen(t, def, store, clk)
	token := eng2.Status().UncertainActions[0].Token
	if _, occurred := svc.Lookup(eng2.RunID()); occurred {
		t.Fatal("fixture: external side effect should be absent")
	}
	if err := eng2.VerifyAction(token, "", false); err != nil {
		t.Fatal(err)
	}
	if err := eng2.Advance(actions); err != nil {
		t.Fatal(err)
	}
	if !eng2.Status().Done || svc.calls != 1 {
		t.Fatalf("after verified-not-occurred, exactly one real call expected, calls=%d done=%v", svc.calls, eng2.Status().Done)
	}
}

// 重复确认同一动作只生效一次。
func TestDuplicateActionConfirmation(t *testing.T) {
	svc := &fakeExternalService{}
	actions := map[string]Action{"payment": svc}
	def := Definition{
		ID: 4, Name: "a", Steps: []Step{{Kind: "action", Name: "charge", Action: "payment"}},
	}
	eng, store := newFileEngine(t, def, &manualClock{t: time.UnixMilli(0)})
	if err := eng.Advance(actions); err != nil {
		t.Fatal(err)
	}
	token, _ := eng.ActionToken(0)
	evts, _ := store.Read()
	if err := eng.DeliverResult(token, "again"); err != nil {
		t.Fatal(err)
	}
	evts2, _ := store.Read()
	if len(evts2) != len(evts)+1 || evts2[len(evts2)-1].Type != EvtExternalDuplicate {
		t.Fatalf("duplicate ack must append one duplicate event only")
	}
}
