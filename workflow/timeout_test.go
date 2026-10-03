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

// blockUntilDone 返回一个感知取消的阻塞任务：直到 ctx 被取消才返回。
// sawCancel 用于断言任务确实感知到了取消（而不是调度方单方面不等它）。
func blockUntilDone(sawCancel *int32) TaskFunc {
	return func(ctx context.Context) error {
		<-ctx.Done()
		atomic.AddInt32(sawCancel, 1)
		return ctx.Err()
	}
}

func hasReason(sink *testSink, task string, r EventReason) bool {
	for _, e := range sink.events() {
		if e.TaskID == task && e.Reason == r {
			return true
		}
	}
	return false
}

// waitFor 轮询等待条件成立，超时则失败。
func waitFor(t *testing.T, d time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal(msg)
}

func TestSubmit_InvalidTimeoutAndBudget(t *testing.T) {
	t.Log("COVERAGE: 非法的超时/预算配置在提交期被拒绝，原因可区分")
	e := NewEngine(NewMemoryStore(), WithEventSink(newTestSink(t)))

	err := e.Submit("wf-neg-timeout", []TaskSpec{
		{ID: "a", Fn: okFn(), Timeout: -time.Second},
	})
	if !errors.Is(err, ErrInvalidTimeout) {
		t.Fatalf("want ErrInvalidTimeout, got %v", err)
	}

	err = e.Submit("wf-neg-budget", []TaskSpec{
		{ID: "a", Fn: okFn()},
	}, WithBudget(-time.Second))
	if !errors.Is(err, ErrInvalidBudget) {
		t.Fatalf("want ErrInvalidBudget, got %v", err)
	}
	t.Log("RESULT: 非法配置均在提交期以可区分错误拒绝")
}

func TestTaskTimeout_RetryThenSuccess(t *testing.T) {
	t.Log("COVERAGE: 单任务超时 -> 本次尝试按失败算且任务感知取消 -> 走退避重试 -> 重试成功")
	sink := newTestSink(t)
	e := NewEngine(NewMemoryStore(), WithEventSink(sink))

	var attempts, sawCancel int32
	specs := []TaskSpec{{
		ID:      "flaky-rpc",
		Timeout: 30 * time.Millisecond,
		Fn: func(ctx context.Context) error {
			n := atomic.AddInt32(&attempts, 1)
			if n == 1 {
				// 第一次尝试模拟卡住的外部调用，直到被取消。
				<-ctx.Done()
				atomic.AddInt32(&sawCancel, 1)
				return ctx.Err()
			}
			return nil // 重试后成功
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
		t.Fatalf("want SUCCEEDED, got %s", wf.Status)
	}
	ts := wf.Tasks["flaky-rpc"]
	if ts.Status != StatusSucceeded || ts.Attempt != 2 {
		t.Fatalf("want SUCCEEDED at attempt 2, got %s attempt=%d", ts.Status, ts.Attempt)
	}
	if atomic.LoadInt32(&sawCancel) != 1 {
		t.Fatalf("timed-out attempt must observe cancellation, sawCancel=%d", sawCancel)
	}
	if !hasReason(sink, "flaky-rpc", ReasonAttemptTimeout) {
		t.Fatal("missing ATTEMPT_TIMEOUT event; timeout must be distinguishable from business failure")
	}
	t.Log("RESULT: 超时尝试按失败计入并重试成功，任务感知到取消，ATTEMPT_TIMEOUT 事件可见")
}

func TestTaskTimeout_ExhaustsRetries(t *testing.T) {
	t.Log("COVERAGE: 持续超时把重试额度用尽 -> 任务终态 FAILED，超时与普通失败可区分；" +
		"顽固任务（无视取消）迟到的结果被丢弃，不干扰终态")
	sink := newTestSink(t)
	e := NewEngine(NewMemoryStore(), WithEventSink(sink))

	var attempts int32
	specs := []TaskSpec{{
		ID:      "stuck",
		Timeout: 20 * time.Millisecond,
		Fn: func(ctx context.Context) error {
			atomic.AddInt32(&attempts, 1)
			// 顽固任务：无视取消，硬睡超过超时时间后返回“成功”。
			// 迟到结果必须被协调器丢弃，任务仍按超时失败定论。
			time.Sleep(60 * time.Millisecond)
			return nil
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
	ts := wf.Tasks["stuck"]
	if ts.Status != StatusFailed {
		t.Fatalf("want task FAILED, got %s", ts.Status)
	}
	if ts.Attempt != 2 {
		t.Fatalf("want 2 attempts (1 + 1 retry), got %d", ts.Attempt)
	}
	if !strings.Contains(ts.LastError, ErrTaskTimeout.Error()) {
		t.Fatalf("LastError must distinguish timeout, got %q", ts.LastError)
	}
	if !hasReason(sink, "stuck", ReasonAttemptTimeout) {
		t.Fatal("missing ATTEMPT_TIMEOUT event")
	}
	if !hasReason(sink, "stuck", ReasonRetriesExhausted) {
		t.Fatal("missing RETRIES_EXHAUSTED event")
	}
	// 等顽固任务的迟到结果送达后，状态不得被翻回。
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&attempts) >= 2 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(80 * time.Millisecond) // 让迟到的 nil 结果有机会送达并被丢弃
	again, err := e.GetState(context.Background(), "wf")
	if err != nil {
		t.Fatal(err)
	}
	if again.Tasks["stuck"].Status != StatusFailed || again.Status != WfFailed {
		t.Fatalf("late result must not roll back terminal state, got wf=%s task=%s",
			again.Status, again.Tasks["stuck"].Status)
	}
	t.Log("RESULT: 超时耗尽重试进入终态 FAILED，迟到结果被对账丢弃，状态无回退")
}

func TestWorkflowBudget_Exhausted(t *testing.T) {
	t.Log("COVERAGE: 工作流总预算耗尽 -> 不再派发新任务，在途任务被取消，" +
		"未跑任务落定终态，已完成结果保留，工作流落到明确终态 CANCELLED")
	sink := newTestSink(t)
	e := NewEngine(NewMemoryStore(), WithEventSink(sink))

	var sawCancel, laterRan int32
	specs := []TaskSpec{
		{ID: "fast", Fn: okFn()}, // 预算内完成，结果必须保留
		{ID: "slow", Fn: blockUntilDone(&sawCancel)},
		{ID: "later", Deps: []string{"slow"}, Fn: func(ctx context.Context) error {
			atomic.AddInt32(&laterRan, 1)
			return nil
		}},
	}
	if err := e.Submit("wf", specs, WithConcurrency(2), WithBudget(60*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	wf, err := e.Trigger(context.Background(), "wf")
	if err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(start)

	if wf.Status != WfCancelled {
		t.Fatalf("want CANCELLED, got %s", wf.Status)
	}
	if wf.Tasks["fast"].Status != StatusSucceeded {
		t.Fatalf("completed task must keep SUCCEEDED, got %s", wf.Tasks["fast"].Status)
	}
	if wf.Tasks["slow"].Status != StatusCancelled {
		t.Fatalf("in-flight task must be CANCELLED, got %s", wf.Tasks["slow"].Status)
	}
	if wf.Tasks["later"].Status != StatusCancelled {
		t.Fatalf("never-dispatched task must be CANCELLED, got %s", wf.Tasks["later"].Status)
	}
	if atomic.LoadInt32(&laterRan) != 0 {
		t.Fatal("no new dispatch after budget exhausted")
	}
	// 取消信号已发出，配合的任务应“尽快”退出（允许极短的调度延迟）。
	waitFor(t, time.Second, func() bool { return atomic.LoadInt32(&sawCancel) == 1 },
		"in-flight task must observe cancellation")
	if elapsed > 2*time.Second {
		t.Fatalf("budget must bound total wall time, took %v", elapsed)
	}
	if !hasReason(sink, "slow", ReasonBudgetExhausted) || !hasReason(sink, "", ReasonBudgetExhausted) {
		t.Fatal("missing BUDGET_EXHAUSTED events; budget cutoff must be distinguishable")
	}
	if wf.Deadline.IsZero() {
		t.Fatal("deadline must be persisted on first trigger")
	}
	t.Logf("RESULT: 预算到点收口为 CANCELLED（耗时 %v），完成/在途/未跑任务各有明确终态", elapsed)
}

func TestBudget_ResumeAfterExpiry(t *testing.T) {
	t.Log("COVERAGE: 预算+中断续跑叠加 -> 预算按第一次触发算起，续跑时预算早已用尽，" +
		"不执行任何任务直接给出终态结论，已完成任务不重跑")
	sink := newTestSink(t)
	e := NewEngine(NewMemoryStore(), WithEventSink(sink))

	var aRuns, bRuns, sawCancel int32
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
			atomic.AddInt32(&sawCancel, 1)
			return ctx.Err()
		}},
	}
	if err := e.Submit("wf", specs, WithBudget(80*time.Millisecond)); err != nil {
		t.Fatal(err)
	}

	triggerDone := make(chan *WorkflowState)
	go func() { wf, _ := e.Trigger(context.Background(), "wf"); triggerDone <- wf }()
	<-bStarted
	mid, _ := e.GetState(context.Background(), "wf")
	firstDeadline := mid.Deadline
	if firstDeadline.IsZero() {
		t.Fatal("deadline must be armed at first trigger")
	}
	if !e.Cancel("wf") {
		t.Fatal("cancel in-flight run")
	}
	<-triggerDone

	// 等预算彻底用尽后续跑：必须直接给出终态，不得重跑任何任务。
	time.Sleep(120 * time.Millisecond)
	wf, err := e.Resume(context.Background(), "wf")
	if err != nil {
		t.Fatal(err)
	}
	if wf.Status != WfCancelled {
		t.Fatalf("resume after budget expiry want CANCELLED, got %s", wf.Status)
	}
	if wf.Tasks["a"].Status != StatusSucceeded {
		t.Fatalf("completed task must be preserved, got %s", wf.Tasks["a"].Status)
	}
	if wf.Tasks["b"].Status != StatusCancelled {
		t.Fatalf("interrupted task must be finalized CANCELLED, got %s", wf.Tasks["b"].Status)
	}
	if !wf.Deadline.Equal(firstDeadline) {
		t.Fatalf("deadline must be anchored at first trigger: %v vs %v", firstDeadline, wf.Deadline)
	}
	if atomic.LoadInt32(&aRuns) != 1 || atomic.LoadInt32(&bRuns) != 1 {
		t.Fatalf("expired-budget resume must not re-run tasks: a=%d b=%d", aRuns, bRuns)
	}
	// 终态后再次触发：幂等返回同一终态。
	again, err := e.Trigger(context.Background(), "wf")
	if err != nil || again.Status != WfCancelled {
		t.Fatalf("terminal re-trigger want CANCELLED, got %v %v", again.Status, err)
	}
	t.Log("RESULT: 续跑遇预算已尽直接终态 CANCELLED，Deadline 锚定首次触发，零重跑")
}

func TestTimeout_InterruptResume(t *testing.T) {
	t.Log("COVERAGE: 任务超时+中断续跑叠加 -> 超时进入退避时被 Cancel 中断，" +
		"续跑沿用尝试计数重执行并成功，状态不回退")
	sink := newTestSink(t)
	e := NewEngine(NewMemoryStore(), WithEventSink(sink))

	var attempts, sawCancel int32
	retrying := make(chan struct{})
	specs := []TaskSpec{{
		ID:      "flaky",
		Timeout: 30 * time.Millisecond,
		Fn: func(ctx context.Context) error {
			n := atomic.AddInt32(&attempts, 1)
			if n == 1 {
				<-ctx.Done() // 第一次尝试卡住直到超时取消
				atomic.AddInt32(&sawCancel, 1)
				return ctx.Err()
			}
			return nil
		},
		Retry: RetryPolicy{MaxRetries: 2, Backoff: ConstantBackoff{Interval: 200 * time.Millisecond}},
	}}
	if err := e.Submit("wf", specs); err != nil {
		t.Fatal(err)
	}
	triggerDone := make(chan struct{})
	go func() { e.Trigger(context.Background(), "wf"); close(triggerDone) }()

	// 等到超时发生、任务进入 RETRYING（退避 200ms 期间）后中断。
	deadline := time.Now().Add(2 * time.Second)
	for {
		st, err := e.GetState(context.Background(), "wf")
		if err != nil {
			t.Fatal(err)
		}
		if st.Tasks["flaky"].Status == StatusRetrying {
			close(retrying)
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("task never entered RETRYING after timeout")
		}
		time.Sleep(2 * time.Millisecond)
	}
	<-retrying
	if !e.Cancel("wf") {
		t.Fatal("cancel in-flight run")
	}
	<-triggerDone

	wf, err := e.Resume(context.Background(), "wf")
	if err != nil {
		t.Fatal(err)
	}
	if wf.Status != WfSucceeded || wf.Tasks["flaky"].Status != StatusSucceeded {
		t.Fatalf("resume want SUCCEEDED, got wf=%s task=%s", wf.Status, wf.Tasks["flaky"].Status)
	}
	if got := wf.Tasks["flaky"].Attempt; got != 1 {
		t.Fatalf("interrupted retry must reuse attempt count, got %d", got)
	}
	if atomic.LoadInt32(&attempts) != 2 {
		t.Fatalf("fn should run twice (timed-out + resumed), got %d", attempts)
	}
	if !hasReason(sink, "flaky", ReasonAttemptTimeout) {
		t.Fatal("missing ATTEMPT_TIMEOUT event")
	}
	t.Log("RESULT: 超时->退避->中断->续跑成功，Attempt 不多计，无状态回退")
}

func TestTimeout_ConcurrentTriggerUnique(t *testing.T) {
	t.Log("COVERAGE: 超时场景下并发/重复触发结论唯一 -> 共享同一次执行，" +
		"不多派任务，所有等待者拿到同一份终态")
	sink := newTestSink(t)
	e := NewEngine(NewMemoryStore(), WithEventSink(sink))

	var runs int32
	specs := []TaskSpec{{
		ID:      "always-stuck",
		Timeout: 20 * time.Millisecond,
		Fn: func(ctx context.Context) error {
			atomic.AddInt32(&runs, 1)
			<-ctx.Done()
			return ctx.Err()
		},
		Retry: RetryPolicy{MaxRetries: 1, Backoff: ConstantBackoff{Interval: time.Millisecond}},
	}}
	if err := e.Submit("wf", specs); err != nil {
		t.Fatal(err)
	}

	const N = 5
	var wg sync.WaitGroup
	states := make([]*WorkflowState, N)
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func(i int) {
			defer wg.Done()
			st, err := e.Trigger(context.Background(), "wf")
			if err != nil {
				t.Errorf("trigger %d: %v", i, err)
			}
			states[i] = st
		}(i)
	}
	wg.Wait()
	for i, st := range states {
		if st == nil || st.Status != WfFailed {
			t.Fatalf("trigger %d want FAILED, got %+v", i, st)
		}
	}
	if got := atomic.LoadInt32(&runs); got != 2 {
		t.Fatalf("concurrent triggers must share one run: fn invocations=%d want 2", got)
	}
	t.Log("RESULT: 5 路并发触发共享一次执行，超时失败结论唯一（2 次尝试后 FAILED）")
}

func TestTimeout_NoTimerOrGoroutineLeak(t *testing.T) {
	t.Log("COVERAGE: 任务多、超时频繁时定时器与协程及时回收 -> 反复运行后协程数回落基线")
	// 预热，排除首次运行的常驻协程干扰。
	warm := NewEngine(NewMemoryStore())
	_ = warm.Submit("warm", []TaskSpec{{ID: "t", Fn: okFn(), Timeout: time.Millisecond}})
	_, _ = warm.Trigger(context.Background(), "warm")
	time.Sleep(20 * time.Millisecond)
	baseline := runtime.NumGoroutine()

	e := NewEngine(NewMemoryStore())
	const batches = 30
	for i := 0; i < batches; i++ {
		id := "wf-" + strings.Repeat("x", 1) + string(rune('a'+i%26)) + strings.Repeat("y", i/26)
		var specs []TaskSpec
		for j := 0; j < 4; j++ {
			tid := id + "-t" + string(rune('0'+j))
			specs = append(specs, TaskSpec{
				ID:      tid,
				Timeout: 5 * time.Millisecond,
				Fn: func(ctx context.Context) error {
					<-ctx.Done()
					return ctx.Err()
				},
				Retry: RetryPolicy{MaxRetries: 2, Backoff: ConstantBackoff{Interval: time.Millisecond}},
			})
		}
		if err := e.Submit(id, specs, WithConcurrency(4), WithBudget(200*time.Millisecond)); err != nil {
			t.Fatal(err)
		}
		wf, err := e.Trigger(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		if wf.Status != WfFailed {
			t.Fatalf("batch %d want FAILED, got %s", i, wf.Status)
		}
	}

	// 所有工作流终态后，超时定时器与工作协程应全部回收。
	deadline := time.Now().Add(3 * time.Second)
	for {
		n := runtime.NumGoroutine()
		if n <= baseline+2 {
			t.Logf("RESULT: %d 批超时工作流后协程数 %d 回落到基线 %d 附近", batches, n, baseline)
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("goroutines not reclaimed: baseline=%d now=%d", baseline, n)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
