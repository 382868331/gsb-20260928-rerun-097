package engine

import (
	"reflect"
	"testing"
)

// 部分分支重试成功后，补到旧 attempt 的迟到结果在退出前可见；
// 保存/重开后必须仍挂在旧 attempt 上，不覆盖新 attempt 的结果。
func TestWaitGroupRetryLateResultSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewStore(dir)
	def := Definition{ID: 25, Steps: []Step{{Kind: "parallel_wait", Name: "wg", Group: &WaitGroupSpec{
		Mode:     WaitAll,
		Branches: []BranchSpec{waitBranchSpec(true, "a"), waitBranchSpec(true, "b")},
	}}}}
	eng1 := NewEngine(store, systemClock{}, Hooks{})
	done := startAsync(t, eng1, def, "g16", nil)
	waitForEvent(t, store, "g16", EvtGroupScheduled)
	if err := eng1.DeliverGroupFailure(branchTok("g16", 0, "0"), "transient"); err != nil {
		t.Fatal(err)
	}
	newToken, err := eng1.RetryGroupBranch(groupRootID("g16", 0), "0")
	if err != nil {
		t.Fatal(err)
	}
	if err := eng1.DeliverGroupResult(newToken, "recovered"); err != nil {
		t.Fatal(err)
	}
	if err := eng1.DeliverGroupResult(branchTok("g16", 0, "1"), "ok"); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	// 旧 attempt 的迟到成功结果：退出前可见，标 stale，归属 attempt 0。
	if err := eng1.DeliverGroupResult(branchTok("g16", 0, "0"), "late-old-attempt"); err != nil {
		t.Fatal(err)
	}
	// 重复迟到通知：只记 duplicate，不新增归属。
	if err := eng1.DeliverGroupResult(branchTok("g16", 0, "0"), "late-old-attempt"); err != nil {
		t.Fatal(err)
	}
	before := eng1.WaitGroups()
	eng1.Close()

	eng2, err := Recover(store, systemClock{}, def, "g16", Hooks{})
	if err != nil {
		t.Fatal(err)
	}
	after := eng2.WaitGroups()
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("view must survive reopen\nbefore: %+v\nafter:  %+v", before, after)
	}
	var bv *GroupBranchView
	for i := range after[0].Branches {
		if after[0].Branches[i].Path == "0" {
			bv = &after[0].Branches[i]
		}
	}
	if bv == nil || bv.Result != "recovered" || bv.Attempt != 1 {
		t.Fatalf("new attempt result must not be overwritten: %+v", bv)
	}
	want := []string{"transient", "recovered", "late-old-attempt"}
	if !reflect.DeepEqual(bv.AttemptResults, want) {
		t.Fatalf("late result kept on its own attempt, got %v want %v", bv.AttemptResults, want)
	}
	// 多次重开：视图仍然一致。
	eng2.Close()
	eng3, err := Recover(store, systemClock{}, def, "g16", Hooks{})
	if err != nil {
		t.Fatal(err)
	}
	if got := eng3.WaitGroups(); !reflect.DeepEqual(before, got) {
		t.Fatalf("view must survive repeated reopens\nbefore: %+v\nafter:  %+v", before, got)
	}
	if n := countType(mustLoad(t, store, "g16"), EvtGroupDecided); n != 1 {
		t.Fatalf("exactly one decision, got %d", n)
	}
}

// 已请求取消的分支收到迟到结果：退出前仍标取消，重开后不得变成成功；
// 取消请求与迟到结果在恢复后分别可见。
func TestWaitGroupCancelThenLateResultSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewStore(dir)
	def := Definition{ID: 26, Steps: []Step{{Kind: "parallel_wait", Name: "wg", Group: &WaitGroupSpec{
		Mode: WaitAny, After: AfterCancel,
		Branches: []BranchSpec{waitBranchSpec(true, "a"), waitBranchSpec(true, "b")},
	}}}}
	eng1 := NewEngine(store, systemClock{}, Hooks{})
	done := startAsync(t, eng1, def, "g17", nil)
	waitForEvent(t, store, "g17", EvtGroupScheduled)
	if err := eng1.DeliverGroupResult(branchTok("g17", 0, "0"), "win"); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	// 取消请求之后，外部效果才真正回来。
	if err := eng1.DeliverGroupResult(branchTok("g17", 0, "1"), "late-real"); err != nil {
		t.Fatal(err)
	}
	before := eng1.WaitGroups()
	var canceled *GroupBranchView
	for i := range before[0].Branches {
		if before[0].Branches[i].Path == "1" {
			canceled = &before[0].Branches[i]
		}
	}
	if canceled == nil || canceled.Status != branchCanceled || !canceled.CancelRequested {
		t.Fatalf("branch stays canceled before close: %+v", canceled)
	}
	eng1.Close()

	eng2, err := Recover(store, systemClock{}, def, "g17", Hooks{})
	if err != nil {
		t.Fatal(err)
	}
	after := eng2.WaitGroups()
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("view must survive reopen\nbefore: %+v\nafter:  %+v", before, after)
	}
	var restored *GroupBranchView
	for i := range after[0].Branches {
		if after[0].Branches[i].Path == "1" {
			restored = &after[0].Branches[i]
		}
	}
	if restored == nil || restored.Status != branchCanceled || !restored.CancelRequested || restored.Selected {
		t.Fatalf("canceled branch must not flip to succeeded after reopen: %+v", restored)
	}
	want := []string{"cancel_requested", "late-real"}
	if !reflect.DeepEqual(restored.AttemptResults, want) {
		t.Fatalf("cancel request and late result both visible, got %v want %v", restored.AttemptResults, want)
	}
	if len(after[0].Selected) != 1 || after[0].Selected[0] != "0" {
		t.Fatalf("selected set unchanged after reopen: %v", after[0].Selected)
	}
	// 重开后重复迟到通知：仍是 duplicate，视图不变。
	if err := eng2.DeliverGroupResult(branchTok("g17", 0, "1"), "late-real"); err != nil {
		t.Fatal(err)
	}
	if got := eng2.WaitGroups(); !reflect.DeepEqual(before, got) {
		t.Fatalf("duplicate late notification must not change the view\nbefore: %+v\nafter:  %+v", before, got)
	}
	eng2.Close()
	eng3, err := Recover(store, systemClock{}, def, "g17", Hooks{})
	if err != nil {
		t.Fatal(err)
	}
	if got := eng3.WaitGroups(); !reflect.DeepEqual(before, got) {
		t.Fatalf("view must survive repeated reopens\nbefore: %+v\nafter:  %+v", before, got)
	}
}

// 整组超时后迟到结果与取消请求交织：恢复后视图与退出前一致。
func TestWaitGroupTimeoutLateResultSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewStore(dir)
	def := Definition{ID: 27, Steps: []Step{{Kind: "parallel_wait", Name: "wg", Group: &WaitGroupSpec{
		Mode: WaitAll, TimeoutMS: 30,
		Branches: []BranchSpec{waitBranchSpec(true, "a"), waitBranchSpec(true, "b")},
	}}}}
	eng1 := NewEngine(store, systemClock{}, Hooks{})
	done := startAsync(t, eng1, def, "g18", nil)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := eng1.DeliverGroupResult(branchTok("g18", 0, "0"), "late"); err != nil {
		t.Fatal(err)
	}
	before := eng1.WaitGroups()
	eng1.Close()

	eng2, err := Recover(store, systemClock{}, def, "g18", Hooks{})
	if err != nil {
		t.Fatal(err)
	}
	after := eng2.WaitGroups()
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("view must survive reopen\nbefore: %+v\nafter:  %+v", before, after)
	}
	var late *GroupBranchView
	for i := range after[0].Branches {
		if after[0].Branches[i].Path == "0" {
			late = &after[0].Branches[i]
		}
	}
	if late == nil || late.Status != branchTimedOut || !late.CancelRequested || late.Selected {
		t.Fatalf("timed-out branch keeps its cancel request after reopen: %+v", late)
	}
	want := []string{"group_timeout", "late"}
	if !reflect.DeepEqual(late.AttemptResults, want) {
		t.Fatalf("timeout and late result both visible, got %v want %v", late.AttemptResults, want)
	}
	if len(after[0].Selected) != 0 || after[0].Satisfied {
		t.Fatalf("unsatisfied decision unchanged: %+v", after[0])
	}
}
