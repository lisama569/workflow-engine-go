package workflow

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// waitForStale 等待协调器发出 STALE_RESULT 事件（迟到结果已被识别并丢弃），
// 这是测试与协调器之间的确定性同步点。
func waitForStale(t *testing.T, sink *testSink, taskID string) {
	t.Helper()
	waitFor(t, "STALE_RESULT event for "+taskID, func() bool {
		for _, ev := range sink.events() {
			if ev.TaskID == taskID && ev.Reason == ReasonStaleResult {
				return true
			}
		}
		return false
	})
}

func staleEvents(sink *testSink, taskID string) []Event {
	var out []Event
	for _, ev := range sink.events() {
		if ev.TaskID == taskID && ev.Reason == ReasonStaleResult {
			out = append(out, ev)
		}
	}
	return out
}

// TestLateResult_LateSuccessDoesNotHijackRetry 覆盖：任务超时后，卡住的首次
// 尝试在“重试已在运行”时才迟到返回成功。迟到的成功不得记到重试头上、不得
// 把工作流提前推进到终态；重试自己的结果才决定成败。
func TestLateResult_LateSuccessDoesNotHijackRetry(t *testing.T) {
	t.Log("COVERAGE: 超时尝试迟到返回成功（到达时重试在跑）-> 丢弃，不提前终态，重试自身结果定成败")
	sink := newTestSink(t)
	e := NewEngine(NewMemoryStore(), WithEventSink(sink))

	releaseLate := make(chan struct{})  // 放行第 1 次尝试的迟到返回
	retryStarted := make(chan struct{}) // 第 2 次尝试（重试）已开始
	releaseRetry := make(chan struct{}) // 放行第 2 次尝试返回
	var runs int32
	specs := []TaskSpec{{
		ID:      "late-success",
		Timeout: 150 * time.Millisecond,
		Fn: func(ctx context.Context) error {
			n := atomic.AddInt32(&runs, 1)
			if n == 1 {
				<-ctx.Done()  // 卡在不受取消影响的外部调用里，直到超时被取消
				<-releaseLate // 返回还要再拖一会儿：迟到的成功
				return nil
			}
			close(retryStarted)
			<-releaseRetry
			return nil
		},
		Retry: RetryPolicy{MaxRetries: 1, Backoff: NoBackoff{}}, // 无退避，结论也必须一致
	}}
	if err := e.Submit("wf", specs); err != nil {
		t.Fatal(err)
	}
	triggerDone := make(chan struct{})
	var wf *WorkflowState
	var trigErr error
	go func() {
		defer close(triggerDone)
		wf, trigErr = e.Trigger(context.Background(), "wf")
	}()

	<-retryStarted
	close(releaseLate) // 第 1 次尝试此刻才迟到返回成功
	waitForStale(t, sink, "late-success")

	// 关键断言：迟到成功被丢弃后，重试仍在运行，工作流不得提前终态。
	mid, err := e.GetState(context.Background(), "wf")
	if err != nil {
		t.Fatal(err)
	}
	if mid.Status.IsTerminal() {
		t.Fatalf("late success must not push workflow to terminal early, got %s", mid.Status)
	}
	if ts := mid.Tasks["late-success"]; ts.Status != StatusRunning || ts.Attempt != 2 {
		t.Fatalf("retry must still be running as attempt 2, got %s attempt=%d", ts.Status, ts.Attempt)
	}

	close(releaseRetry) // 重试自己返回成功
	<-triggerDone
	if trigErr != nil {
		t.Fatal(trigErr)
	}
	if wf.Status != WfSucceeded {
		t.Fatalf("want SUCCEEDED decided by the retry itself, got %s", wf.Status)
	}
	ts := wf.Tasks["late-success"]
	if ts.Status != StatusSucceeded || ts.Attempt != 2 {
		t.Fatalf("want SUCCEEDED attempt=2, got %s attempt=%d", ts.Status, ts.Attempt)
	}
	if got := atomic.LoadInt32(&runs); got != 2 {
		t.Fatalf("fn must run once per attempt, got %d", got)
	}
	stales := staleEvents(sink, "late-success")
	if len(stales) != 1 || !strings.Contains(stales[0].Detail, "attempt 1") {
		t.Fatalf("want exactly 1 STALE_RESULT event naming attempt 1, got %+v", stales)
	}
	var timeoutEv int
	for _, ev := range sink.events() {
		if ev.TaskID == "late-success" && ev.Reason == ReasonAttemptTimeout {
			timeoutEv++
		}
	}
	if timeoutEv != 1 {
		t.Fatalf("want exactly 1 ATTEMPT_TIMEOUT event, got %d", timeoutEv)
	}
	t.Log("RESULT: PASS 迟到成功被丢弃，重试在跑不被提前完成，最终由重试自身成功收尾")
}

// TestLateResult_LateFailureDoesNotFailRetry 覆盖：任务超时后，卡住的首次
// 尝试在重试运行期间迟到返回失败。迟到的失败不得算到重试头上：重试成功
// 就是成功，最终状态、尝试次数与事件序列一一对应。
func TestLateResult_LateFailureDoesNotFailRetry(t *testing.T) {
	t.Log("COVERAGE: 超时尝试迟到返回失败（到达时重试在跑）-> 丢弃，重试成功即成功，状态/次数/日志对齐")
	sink := newTestSink(t)
	e := NewEngine(NewMemoryStore(), WithEventSink(sink))

	releaseLate := make(chan struct{})
	retryStarted := make(chan struct{})
	releaseRetry := make(chan struct{})
	var runs int32
	specs := []TaskSpec{{
		ID:      "late-failure",
		Timeout: 150 * time.Millisecond,
		Fn: func(ctx context.Context) error {
			n := atomic.AddInt32(&runs, 1)
			if n == 1 {
				<-ctx.Done()
				<-releaseLate
				return errors.New("late failure from timed-out attempt") // 迟到的失败
			}
			close(retryStarted)
			<-releaseRetry
			return nil // 重试自己成功
		},
		Retry: RetryPolicy{MaxRetries: 1, Backoff: ConstantBackoff{Interval: time.Millisecond}},
	}}
	if err := e.Submit("wf", specs); err != nil {
		t.Fatal(err)
	}
	triggerDone := make(chan struct{})
	var wf *WorkflowState
	var trigErr error
	go func() {
		defer close(triggerDone)
		wf, trigErr = e.Trigger(context.Background(), "wf")
	}()

	<-retryStarted
	close(releaseLate) // 迟到的失败在重试运行期间到达
	waitForStale(t, sink, "late-failure")
	close(releaseRetry)
	<-triggerDone
	if trigErr != nil {
		t.Fatal(trigErr)
	}
	if wf.Status != WfSucceeded {
		t.Fatalf("late failure must not fail the in-flight retry, got %s", wf.Status)
	}
	ts := wf.Tasks["late-failure"]
	if ts.Status != StatusSucceeded || ts.Attempt != 2 {
		t.Fatalf("want SUCCEEDED attempt=2, got %s attempt=%d", ts.Status, ts.Attempt)
	}
	if ts.LastError != "" {
		t.Fatalf("success must clear LastError, got %q", ts.LastError)
	}
	if got := atomic.LoadInt32(&runs); got != 2 {
		t.Fatalf("fn must run once per attempt, got %d", got)
	}
	// 事件序列：恰好 1 次超时、1 次迟到丢弃、1 次重试成功，不得出现
	// 把迟到失败记成业务失败的 ATTEMPT_FAILED。
	var timeoutEv, successEv int
	for _, ev := range sink.events() {
		if ev.TaskID != "late-failure" {
			continue
		}
		switch ev.Reason {
		case ReasonAttemptTimeout:
			timeoutEv++
		case ReasonSucceeded:
			successEv++
		case ReasonAttemptFailed:
			t.Fatal("late failure must not be recorded as a business failure of the retry")
		}
	}
	if timeoutEv != 1 || successEv != 1 {
		t.Fatalf("want 1 ATTEMPT_TIMEOUT + 1 SUCCEEDED, got %d/%d", timeoutEv, successEv)
	}
	if got := len(staleEvents(sink, "late-failure")); got != 1 {
		t.Fatalf("want exactly 1 STALE_RESULT event, got %d", got)
	}
	t.Log("RESULT: PASS 迟到失败被丢弃，重试成功即成功，事件序列与终态一一对应")
}

// TestLateResult_LateSuccessCannotReverseTerminalFailure 覆盖：重试额度用尽后
// 任务进入终态 FAILED，此时首次尝试迟到的成功才返回。已确定的终态不得被
// 迟到结果翻回，工作流保持 FAILED。
func TestLateResult_LateSuccessCannotReverseTerminalFailure(t *testing.T) {
	t.Log("COVERAGE: 末次尝试也超时 -> 重试耗尽落 FAILED -> 首次尝试迟到成功不得翻回终态")
	sink := newTestSink(t)
	e := NewEngine(NewMemoryStore(), WithEventSink(sink))

	releaseLate := make(chan struct{})   // 放行第 1 次尝试的迟到成功
	retryStarted := make(chan struct{})  // 第 2 次（末次）尝试已开始
	releaseRetry := make(chan struct{})  // 测试收尾时放行第 2 次尝试的协程
	var runs int32
	specs := []TaskSpec{{
		ID:      "late-after-exhausted",
		Timeout: 60 * time.Millisecond,
		Fn: func(ctx context.Context) error {
			n := atomic.AddInt32(&runs, 1)
			if n == 1 {
				<-ctx.Done()
				<-releaseLate
				return nil // 迟到的成功
			}
			close(retryStarted)
			<-releaseRetry // 卡住直到也超时（不受取消影响）
			return nil
		},
		Retry: RetryPolicy{MaxRetries: 1, Backoff: ConstantBackoff{Interval: time.Millisecond}},
	}}
	if err := e.Submit("wf", specs); err != nil {
		t.Fatal(err)
	}
	triggerDone := make(chan struct{})
	var wf *WorkflowState
	var trigErr error
	go func() {
		defer close(triggerDone)
		wf, trigErr = e.Trigger(context.Background(), "wf")
	}()

	<-retryStarted
	close(releaseLate) // 第 1 次尝试的迟到成功在末次尝试运行期间到达
	waitForStale(t, sink, "late-after-exhausted")
	<-triggerDone      // 末次尝试也超时 -> 重试耗尽 -> FAILED
	close(releaseRetry)
	if trigErr != nil {
		t.Fatal(trigErr)
	}
	if wf.Status != WfFailed {
		t.Fatalf("late success must not reverse terminal failure, got %s", wf.Status)
	}
	ts := wf.Tasks["late-after-exhausted"]
	if ts.Status != StatusFailed || ts.Attempt != 2 {
		t.Fatalf("want FAILED attempt=2, got %s attempt=%d", ts.Status, ts.Attempt)
	}
	if !strings.Contains(ts.LastError, ErrTaskTimeout.Error()) {
		t.Fatalf("LastError must stay the timeout of the final attempt, got %q", ts.LastError)
	}
	for _, ev := range sink.events() {
		if ev.TaskID == "late-after-exhausted" && ev.To == string(StatusSucceeded) {
			t.Fatal("late success must never produce a SUCCEEDED transition")
		}
	}
	if got := len(staleEvents(sink, "late-after-exhausted")); got != 1 {
		t.Fatalf("want exactly 1 STALE_RESULT event, got %d", got)
	}
	// 终态后重复触发：幂等返回同一 FAILED，不重跑。
	again, err := e.Trigger(context.Background(), "wf")
	if err != nil {
		t.Fatal(err)
	}
	if again.Status != WfFailed {
		t.Fatalf("terminal state must not roll back on re-trigger, got %s", again.Status)
	}
	if got := atomic.LoadInt32(&runs); got != 2 {
		t.Fatalf("terminal re-trigger must not re-run tasks, runs=%d", got)
	}
	t.Log("RESULT: PASS 重试耗尽落 FAILED，迟到成功被丢弃，终态不被翻回")
}

// TestLateResult_ArrivesDuringBackoff 覆盖：迟到的失败在退避期间（任务处于
// RETRYING）到达，同样被丢弃；退避结束后重试成功。退避长短不改变结论。
func TestLateResult_ArrivesDuringBackoff(t *testing.T) {
	t.Log("COVERAGE: 超时尝试在退避期间迟到返回失败 -> 丢弃 -> 退避结束重试成功，结论与短退避一致")
	sink := newTestSink(t)
	e := NewEngine(NewMemoryStore(), WithEventSink(sink))

	releaseLate := make(chan struct{})
	var runs int32
	specs := []TaskSpec{{
		ID:      "late-in-backoff",
		Timeout: 50 * time.Millisecond,
		Fn: func(ctx context.Context) error {
			n := atomic.AddInt32(&runs, 1)
			if n == 1 {
				<-ctx.Done()
				<-releaseLate
				return errors.New("late failure during backoff")
			}
			return nil
		},
		Retry: RetryPolicy{MaxRetries: 1, Backoff: ConstantBackoff{Interval: 200 * time.Millisecond}},
	}}
	if err := e.Submit("wf", specs); err != nil {
		t.Fatal(err)
	}
	triggerDone := make(chan struct{})
	var wf *WorkflowState
	var trigErr error
	go func() {
		defer close(triggerDone)
		wf, trigErr = e.Trigger(context.Background(), "wf")
	}()

	// 等任务进入退避，再放出迟到的失败。
	waitFor(t, "task enters RETRYING", func() bool {
		wf, err := e.GetState(context.Background(), "wf")
		return err == nil && wf.Tasks["late-in-backoff"].Status == StatusRetrying
	})
	close(releaseLate)
	waitForStale(t, sink, "late-in-backoff")
	<-triggerDone
	if trigErr != nil {
		t.Fatal(trigErr)
	}
	if wf.Status != WfSucceeded || wf.Tasks["late-in-backoff"].Status != StatusSucceeded {
		t.Fatalf("want SUCCEEDED, got wf=%s task=%s", wf.Status, wf.Tasks["late-in-backoff"].Status)
	}
	if wf.Tasks["late-in-backoff"].Attempt != 2 {
		t.Fatalf("want attempt=2, got %d", wf.Tasks["late-in-backoff"].Attempt)
	}
	if got := atomic.LoadInt32(&runs); got != 2 {
		t.Fatalf("fn must run once per attempt, got %d", got)
	}
	t.Log("RESULT: PASS 退避期间到达的迟到失败被丢弃，重试成功，结论与短退避一致")
}

// TestLateResult_TimeoutInterruptResume 覆盖：超时尝试完全不响应取消（卡住
// 不动），任务在退避期被中断，此时卡住的协程才迟到返回；续跑后重试成功，
// 尝试计数不重复、终态不受迟到结果影响。
func TestLateResult_TimeoutInterruptResume(t *testing.T) {
	t.Log("COVERAGE: 超时+卡住不响应取消 -> 退避期中断 -> 迟到返回落在两次运行之间 -> 续跑重试成功")
	sink := newTestSink(t)
	e := NewEngine(NewMemoryStore(), WithEventSink(sink))

	releaseLate := make(chan struct{})
	lateReturned := make(chan struct{})
	var runs int32
	specs := []TaskSpec{{
		ID:      "stuck-forever",
		Timeout: 50 * time.Millisecond,
		Fn: func(ctx context.Context) error {
			n := atomic.AddInt32(&runs, 1)
			if n == 1 {
				<-releaseLate // 完全不响应取消，直到测试放行
				close(lateReturned)
				return nil // 迟到的成功
			}
			return nil
		},
		Retry: RetryPolicy{MaxRetries: 2, Backoff: ConstantBackoff{Interval: 500 * time.Millisecond}},
	}}
	if err := e.Submit("wf", specs); err != nil {
		t.Fatal(err)
	}
	triggerDone := make(chan struct{})
	go func() {
		defer close(triggerDone)
		if _, err := e.Trigger(context.Background(), "wf"); err != nil {
			t.Errorf("trigger: %v", err)
		}
	}()
	// 超时发生、任务进入退避后中断；卡住的协程仍握着第 1 次尝试。
	waitFor(t, "task enters RETRYING after timeout", func() bool {
		wf, err := e.GetState(context.Background(), "wf")
		return err == nil && wf.Tasks["stuck-forever"].Status == StatusRetrying
	})
	if !e.Cancel("wf") {
		t.Fatal("cancel returned false for in-flight run")
	}
	<-triggerDone
	// 中断后才放出迟到返回：它落在已结束的运行上，不得影响续跑。
	close(releaseLate)
	<-lateReturned

	wf, err := e.Resume(context.Background(), "wf")
	if err != nil {
		t.Fatal(err)
	}
	if wf.Status != WfSucceeded || wf.Tasks["stuck-forever"].Status != StatusSucceeded {
		t.Fatalf("resume want SUCCEEDED, got wf=%s task=%s", wf.Status, wf.Tasks["stuck-forever"].Status)
	}
	if wf.Tasks["stuck-forever"].Attempt != 2 {
		t.Fatalf("timeout consumed exactly one attempt, want attempt=2, got %d",
			wf.Tasks["stuck-forever"].Attempt)
	}
	if got := atomic.LoadInt32(&runs); got != 2 {
		t.Fatalf("fn must run once per attempt, got %d", got)
	}
	t.Log("RESULT: PASS 超时+中断+续跑叠加，迟到返回不影响续跑结论，尝试计数正确")
}

// TestLateResult_ConcurrentTriggersConclusionUnique 覆盖：超时+迟到失败场景下
// 5 路并发触发共享同一次执行，结论唯一（重试成功即成功），不多派任务。
func TestLateResult_ConcurrentTriggersConclusionUnique(t *testing.T) {
	t.Log("COVERAGE: 超时+迟到失败场景下 5 路并发触发 -> 共享同一次执行，结论唯一，任务不多跑")
	sink := newTestSink(t)
	e := NewEngine(NewMemoryStore(), WithEventSink(sink))

	releaseLate := make(chan struct{})
	retryStarted := make(chan struct{})
	releaseRetry := make(chan struct{})
	var runs int32
	specs := []TaskSpec{{
		ID:      "late-concurrent",
		Timeout: 150 * time.Millisecond,
		Fn: func(ctx context.Context) error {
			n := atomic.AddInt32(&runs, 1)
			if n == 1 {
				<-ctx.Done()
				<-releaseLate
				return errors.New("late failure")
			}
			close(retryStarted)
			<-releaseRetry
			return nil
		},
		Retry: RetryPolicy{MaxRetries: 1, Backoff: ConstantBackoff{Interval: time.Millisecond}},
	}}
	if err := e.Submit("wf", specs); err != nil {
		t.Fatal(err)
	}

	const N = 5
	var wg sync.WaitGroup
	states := make([]*WorkflowState, N)
	errs := make([]error, N)
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func(i int) {
			defer wg.Done()
			states[i], errs[i] = e.Trigger(context.Background(), "wf")
		}(i)
	}
	<-retryStarted
	close(releaseLate)
	waitForStale(t, sink, "late-concurrent")
	close(releaseRetry)
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("trigger %d: %v", i, err)
		}
		if states[i].Status != WfSucceeded {
			t.Fatalf("trigger %d: want SUCCEEDED, got %s", i, states[i].Status)
		}
		if states[i].Tasks["late-concurrent"].Attempt != 2 {
			t.Fatalf("trigger %d: want attempt=2, got %d", i, states[i].Tasks["late-concurrent"].Attempt)
		}
	}
	if got := atomic.LoadInt32(&runs); got != 2 {
		t.Fatalf("concurrent triggers must share one run; fn invocations=%d want 2", got)
	}
	t.Log("RESULT: PASS 并发触发下迟到结果不制造分歧，结论唯一且任务只跑一次")
}
