package workflow

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// waitFor 轮询直到条件满足或超时，用于与协调器状态同步。
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timeout waiting for: %s", what)
}

// TestTaskTimeout_RetryThenSuccess 覆盖：单任务声明超时后，卡住的首次尝试
// 被判为超时失败（任务本身能感知取消），走原有重试/退避后成功；
// 超时失败与普通业务失败在事件与日志中可区分。
func TestTaskTimeout_RetryThenSuccess(t *testing.T) {
	t.Log("COVERAGE: 单任务超时 -> 本次尝试按失败算 -> 任务感知取消 -> 重试后成功")
	sink := newTestSink(t)
	e := NewEngine(NewMemoryStore(), WithEventSink(sink))

	var attempts int32
	cancelSeen := make(chan struct{}, 1)
	specs := []TaskSpec{{
		ID:      "stuck-then-ok",
		Timeout: 20 * time.Millisecond,
		Fn: func(ctx context.Context) error {
			n := atomic.AddInt32(&attempts, 1)
			if n == 1 {
				<-ctx.Done() // 模拟卡住的外部调用，直到被取消
				select {
				case cancelSeen <- struct{}{}:
				default:
				}
				return ctx.Err()
			}
			return nil
		},
		Retry: RetryPolicy{MaxRetries: 2, Backoff: ConstantBackoff{Interval: time.Millisecond}},
	}}
	if err := e.Submit("wf", specs); err != nil {
		t.Fatal(err)
	}
	wf, err := e.Trigger(context.Background(), "wf")
	if err != nil {
		t.Fatal(err)
	}
	if wf.Status != WfSucceeded {
		t.Fatalf("want SUCCEEDED after retry, got %s", wf.Status)
	}
	ts := wf.Tasks["stuck-then-ok"]
	if ts.Status != StatusSucceeded || ts.Attempt != 2 {
		t.Fatalf("want SUCCEEDED attempt=2, got %s attempt=%d", ts.Status, ts.Attempt)
	}
	select {
	case <-cancelSeen:
	default:
		t.Fatal("stuck task must observe ctx cancellation on timeout")
	}
	var timeoutEvents int
	for _, ev := range sink.events() {
		if ev.TaskID == "stuck-then-ok" && ev.Reason == ReasonAttemptTimeout {
			timeoutEvents++
			if !strings.Contains(ev.Detail, "timed out") {
				t.Fatalf("timeout event detail must say timed out, got %q", ev.Detail)
			}
		}
		if ev.TaskID == "stuck-then-ok" && ev.Reason == ReasonAttemptFailed {
			t.Fatal("timeout must not be reported as plain business failure")
		}
	}
	if timeoutEvents != 1 {
		t.Fatalf("want exactly 1 ATTEMPT_TIMEOUT event, got %d", timeoutEvents)
	}
	t.Log("RESULT: PASS 超时尝试被区分记录，重试后成功")
}

// TestTaskTimeout_RetriesExhausted 覆盖：每次尝试都超时，重试额度耗尽后
// 任务进入终态 FAILED，工作流 FAILED；LastError 可用 ErrTaskTimeout 区分。
func TestTaskTimeout_RetriesExhausted(t *testing.T) {
	t.Log("COVERAGE: 单任务连续超时 -> 重试额度耗尽 -> 终态 FAILED，超时原因可区分")
	sink := newTestSink(t)
	e := NewEngine(NewMemoryStore(), WithEventSink(sink))

	var attempts int32
	specs := []TaskSpec{{
		ID:      "always-stuck",
		Timeout: 15 * time.Millisecond,
		Fn: func(ctx context.Context) error {
			atomic.AddInt32(&attempts, 1)
			<-ctx.Done()
			return ctx.Err()
		},
		Retry: RetryPolicy{MaxRetries: 1, Backoff: ConstantBackoff{Interval: time.Millisecond}},
	}}
	if err := e.Submit("wf", specs); err != nil {
		t.Fatal(err)
	}
	wf, err := e.Trigger(context.Background(), "wf")
	if err != nil {
		t.Fatal(err)
	}
	if wf.Status != WfFailed {
		t.Fatalf("want FAILED, got %s", wf.Status)
	}
	ts := wf.Tasks["always-stuck"]
	if ts.Status != StatusFailed || ts.Attempt != 2 {
		t.Fatalf("want FAILED attempt=2 (1+1 retry), got %s attempt=%d", ts.Status, ts.Attempt)
	}
	if atomic.LoadInt32(&attempts) != 2 {
		t.Fatalf("fn should run once per attempt, got %d", attempts)
	}
	if !strings.Contains(ts.LastError, ErrTaskTimeout.Error()) {
		t.Fatalf("LastError must be distinguishable as timeout, got %q", ts.LastError)
	}
	var timeoutEv, exhaustedEv int
	for _, ev := range sink.events() {
		if ev.TaskID != "always-stuck" {
			continue
		}
		switch ev.Reason {
		case ReasonAttemptTimeout:
			timeoutEv++
		case ReasonRetriesExhausted:
			exhaustedEv++
			if !strings.Contains(ev.Detail, "timed out") {
				t.Fatalf("exhaustion detail must mention timeout, got %q", ev.Detail)
			}
		}
	}
	if timeoutEv != 1 || exhaustedEv != 1 {
		t.Fatalf("want 1 ATTEMPT_TIMEOUT + 1 RETRIES_EXHAUSTED, got %d/%d", timeoutEv, exhaustedEv)
	}
	t.Log("RESULT: PASS 超时耗尽重试后落终态 FAILED，事件与 LastError 均可区分")
}

// TestWorkflowBudgetExhausted 覆盖：总预算到期后不再派发新任务，在跑任务
// 被取消并收敛到 CANCELLED，未跑任务也有终态，已完成任务结果保留，
// 工作流落到可区分的 DEADLINE_EXCEEDED。
func TestWorkflowBudgetExhausted(t *testing.T) {
	t.Log("COVERAGE: 工作流总预算耗尽 -> 立即取消在跑任务 -> 全部任务有终态 -> DEADLINE_EXCEEDED")
	sink := newTestSink(t)
	e := NewEngine(NewMemoryStore(), WithEventSink(sink))

	var neverRan int32
	slowCancelSeen := make(chan struct{}, 1)
	specs := []TaskSpec{
		{ID: "quick", Fn: okFn()},
		{ID: "slow", Fn: func(ctx context.Context) error {
			<-ctx.Done()
			select {
			case slowCancelSeen <- struct{}{}:
			default:
			}
			return ctx.Err()
		}},
		{ID: "never", Deps: []string{"slow"}, Fn: func(ctx context.Context) error {
			atomic.AddInt32(&neverRan, 1)
			return nil
		}},
	}
	if err := e.Submit("wf", specs, WithConcurrency(2), WithBudget(60*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	wf, err := e.Trigger(context.Background(), "wf")
	if err != nil {
		t.Fatal(err)
	}
	if wf.Status != WfDeadlineExceeded {
		t.Fatalf("want DEADLINE_EXCEEDED, got %s", wf.Status)
	}
	if wf.Tasks["quick"].Status != StatusSucceeded {
		t.Fatalf("completed task must keep SUCCEEDED, got %s", wf.Tasks["quick"].Status)
	}
	if wf.Tasks["slow"].Status != StatusCancelled {
		t.Fatalf("in-flight task must be CANCELLED, got %s", wf.Tasks["slow"].Status)
	}
	if wf.Tasks["never"].Status != StatusCancelled {
		t.Fatalf("never-dispatched task must be CANCELLED, got %s", wf.Tasks["never"].Status)
	}
	if atomic.LoadInt32(&neverRan) != 0 {
		t.Fatal("no new task may be dispatched after budget exhausted")
	}
	select {
	case <-slowCancelSeen:
	case <-time.After(2 * time.Second):
		t.Fatal("in-flight task must observe cancellation after budget exhausted")
	}
	var budgetEv int
	for _, ev := range sink.events() {
		if ev.Reason == ReasonBudgetExhausted {
			budgetEv++
		}
	}
	if budgetEv == 0 {
		t.Fatal("want BUDGET_EXHAUSTED events distinguishable from normal failure")
	}
	// 预算终态后重复触发：幂等返回同一终态，不再执行任何任务。
	again, err := e.Trigger(context.Background(), "wf")
	if err != nil {
		t.Fatal(err)
	}
	if again.Status != WfDeadlineExceeded {
		t.Fatalf("re-trigger must return same terminal state, got %s", again.Status)
	}
	t.Log("RESULT: PASS 预算到点收场，终态明确且与普通失败可区分")
}

// TestTimeoutWithInterruptAndResume 覆盖：任务超时进入退避时发生中断，
// 续跑后按保留的尝试计数重试并成功，不超扣重试额度、状态不回退。
func TestTimeoutWithInterruptAndResume(t *testing.T) {
	t.Log("COVERAGE: 任务超时 -> 退避期间中断 -> 续跑重试成功，尝试计数不重复")
	sink := newTestSink(t)
	e := NewEngine(NewMemoryStore(), WithEventSink(sink))

	var runs int32
	specs := []TaskSpec{{
		ID:      "flaky",
		Timeout: 20 * time.Millisecond,
		Fn: func(ctx context.Context) error {
			n := atomic.AddInt32(&runs, 1)
			if n == 1 {
				<-ctx.Done() // 第一次卡住，被超时取消
				return ctx.Err()
			}
			return nil
		},
		Retry: RetryPolicy{MaxRetries: 2, Backoff: ConstantBackoff{Interval: 300 * time.Millisecond}},
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
	// 等待超时发生、任务进入退避，然后中断。
	waitFor(t, "task enters RETRYING after timeout", func() bool {
		wf, err := e.GetState(context.Background(), "wf")
		return err == nil && wf.Tasks["flaky"].Status == StatusRetrying
	})
	if !e.Cancel("wf") {
		t.Fatal("cancel returned false for in-flight run")
	}
	<-triggerDone
	mid, err := e.GetState(context.Background(), "wf")
	if err != nil {
		t.Fatal(err)
	}
	if mid.Tasks["flaky"].Status != StatusReady {
		t.Fatalf("interrupted retrying task must be reclaimed to READY, got %s",
			mid.Tasks["flaky"].Status)
	}

	wf, err := e.Resume(context.Background(), "wf")
	if err != nil {
		t.Fatal(err)
	}
	if wf.Status != WfSucceeded || wf.Tasks["flaky"].Status != StatusSucceeded {
		t.Fatalf("resume want SUCCEEDED, got wf=%s task=%s", wf.Status, wf.Tasks["flaky"].Status)
	}
	if wf.Tasks["flaky"].Attempt != 2 {
		t.Fatalf("timeout consumed exactly one attempt, want attempt=2, got %d",
			wf.Tasks["flaky"].Attempt)
	}
	if atomic.LoadInt32(&runs) != 2 {
		t.Fatalf("fn must run once per attempt, got %d", runs)
	}
	t.Log("RESULT: PASS 超时+中断+续跑叠加，尝试计数与终态一致")
}

// TestBudgetExhaustedBeforeResume 覆盖：中断期间预算到期，续跑不再执行任何
// 任务，直接给出终态结论；已完成任务保留，未完成任务落 CANCELLED。
func TestBudgetExhaustedBeforeResume(t *testing.T) {
	t.Log("COVERAGE: 中断 + 预算在停机期间到期 -> 续跑直接落 DEADLINE_EXCEEDED，不重跑")
	sink := newTestSink(t)
	e := NewEngine(NewMemoryStore(), WithEventSink(sink))

	var aRuns, bRuns int32
	bStarted := make(chan struct{})
	specs := []TaskSpec{
		{ID: "a", Fn: func(ctx context.Context) error {
			atomic.AddInt32(&aRuns, 1)
			return nil
		}},
		{ID: "b", Deps: []string{"a"}, Fn: func(ctx context.Context) error {
			atomic.AddInt32(&bRuns, 1)
			close(bStarted)
			<-ctx.Done()
			return ctx.Err()
		}},
	}
	if err := e.Submit("wf", specs, WithConcurrency(2), WithBudget(120*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	triggerDone := make(chan struct{})
	go func() {
		defer close(triggerDone)
		if _, err := e.Trigger(context.Background(), "wf"); err != nil {
			t.Errorf("trigger: %v", err)
		}
	}()
	<-bStarted
	if !e.Cancel("wf") {
		t.Fatal("cancel returned false for in-flight run")
	}
	<-triggerDone

	time.Sleep(200 * time.Millisecond) // 预算在“停机”期间到期

	wf, err := e.Resume(context.Background(), "wf")
	if err != nil {
		t.Fatal(err)
	}
	if wf.Status != WfDeadlineExceeded {
		t.Fatalf("budget long spent: resume must conclude DEADLINE_EXCEEDED, got %s", wf.Status)
	}
	if wf.Tasks["a"].Status != StatusSucceeded {
		t.Fatalf("completed task must stay SUCCEEDED, got %s", wf.Tasks["a"].Status)
	}
	if wf.Tasks["b"].Status != StatusCancelled {
		t.Fatalf("unfinished task must be CANCELLED, got %s", wf.Tasks["b"].Status)
	}
	if atomic.LoadInt32(&aRuns) != 1 || atomic.LoadInt32(&bRuns) != 1 {
		t.Fatalf("resume after budget exhausted must not re-run tasks: a=%d b=%d", aRuns, bRuns)
	}
	// 再次触发/续跑：幂等返回同一终态。
	again, err := e.Trigger(context.Background(), "wf")
	if err != nil {
		t.Fatal(err)
	}
	if again.Status != WfDeadlineExceeded {
		t.Fatalf("terminal state must not roll back, got %s", again.Status)
	}
	t.Log("RESULT: PASS 预算先于续跑到期，直接终态且已完成任务不重跑")
}

// TestConcurrentTriggers_BudgetConclusionUnique 覆盖：预算场景下并发/重复
// 触发共享同一次执行，结论唯一，不因并发多派任务。
func TestConcurrentTriggers_BudgetConclusionUnique(t *testing.T) {
	t.Log("COVERAGE: 预算耗尽场景下 5 路并发触发 -> 结论唯一 DEADLINE_EXCEEDED，任务只跑一次")
	sink := newTestSink(t)
	e := NewEngine(NewMemoryStore(), WithEventSink(sink))

	var runs int32
	specs := []TaskSpec{
		{ID: "slow", Fn: func(ctx context.Context) error {
			atomic.AddInt32(&runs, 1)
			<-ctx.Done()
			return ctx.Err()
		}},
		{ID: "queued", Deps: []string{"slow"}, Fn: func(ctx context.Context) error {
			atomic.AddInt32(&runs, 1)
			return nil
		}},
	}
	if err := e.Submit("wf", specs, WithBudget(50*time.Millisecond)); err != nil {
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
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("trigger %d: %v", i, err)
		}
		if states[i].Status != WfDeadlineExceeded {
			t.Fatalf("trigger %d: want DEADLINE_EXCEEDED, got %s", i, states[i].Status)
		}
	}
	if got := atomic.LoadInt32(&runs); got != 1 {
		t.Fatalf("concurrent triggers must share one run; fn invocations=%d want 1", got)
	}
	t.Log("RESULT: PASS 并发触发结论唯一，未多派任务")
}

// TestTimeoutAndBudgetValidation 覆盖：非法超时/预算配置在提交期被拒绝。
func TestTimeoutAndBudgetValidation(t *testing.T) {
	t.Log("COVERAGE: 负数任务超时 / 负数工作流预算在提交期以可区分错误拒绝")
	e := NewEngine(NewMemoryStore(), WithEventSink(newTestSink(t)))
	err := e.Submit("wf-bad-timeout", []TaskSpec{{ID: "a", Fn: okFn(), Timeout: -time.Second}})
	if !errors.Is(err, ErrInvalidTimeout) {
		t.Fatalf("want ErrInvalidTimeout, got %v", err)
	}
	err = e.Submit("wf-bad-budget", []TaskSpec{{ID: "a", Fn: okFn()}}, WithBudget(-time.Second))
	if !errors.Is(err, ErrInvalidBudget) {
		t.Fatalf("want ErrInvalidBudget, got %v", err)
	}
	t.Log("RESULT: PASS 非法配置提交期拒绝")
}

// TestTimeoutResourcesReclaimed 覆盖：大量任务频繁超时后，超时相关的
// 定时器与协程被及时回收，不随运行次数累积。
func TestTimeoutResourcesReclaimed(t *testing.T) {
	t.Log("COVERAGE: 20 轮 x 多任务频繁超时 -> 协程数回落，定时器/协程不累积")
	base := runtime.NumGoroutine()
	for i := 0; i < 20; i++ {
		e := NewEngine(NewMemoryStore())
		var specs []TaskSpec
		for j := 0; j < 4; j++ {
			specs = append(specs, TaskSpec{
				ID:      "t" + string(rune('a'+j)),
				Timeout: 5 * time.Millisecond,
				Fn: func(ctx context.Context) error {
					<-ctx.Done()
					return ctx.Err()
				},
				Retry: RetryPolicy{MaxRetries: 2, Backoff: ConstantBackoff{Interval: time.Millisecond}},
			})
		}
		if err := e.Submit("wf", specs, WithConcurrency(4)); err != nil {
			t.Fatal(err)
		}
		wf, err := e.Trigger(context.Background(), "wf")
		if err != nil {
			t.Fatal(err)
		}
		if wf.Status != WfFailed {
			t.Fatalf("round %d: want FAILED, got %s", i, wf.Status)
		}
	}
	waitFor(t, "goroutines fall back to baseline", func() bool {
		return runtime.NumGoroutine() <= base+2
	})
	t.Logf("RESULT: PASS goroutines base=%d now=%d", base, runtime.NumGoroutine())
}
