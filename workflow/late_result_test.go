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

// 本文件覆盖“任务超时后结果迟到返回”的语义：
// 一次尝试被判超时后，其迟到回报（无论成功还是失败）不得影响后续重试的
// 判定；重试的结论只由重试自己决定；终态不被迟到结果翻回；并发触发结论唯一。
//
// 时序用 channel 精确编排而非 sleep 余量：尝试 1 卡在不受取消影响的外部
// 调用里（阻塞在测试控制的 channel 上），超时判定后由测试放行其迟到返回，
// 保证迟到回报确定地落在“重试仍在执行”的窗口内。

// triggerInBackground 在后台触发工作流，返回结果通道。
func triggerInBackground(e *Engine, id string) <-chan triggerOutcome {
	ch := make(chan triggerOutcome, 1)
	go func() {
		wf, err := e.Trigger(context.Background(), id)
		ch <- triggerOutcome{wf: wf, err: err}
	}()
	return ch
}

type triggerOutcome struct {
	wf  *WorkflowState
	err error
}

// TestLateFailureAfterTimeout_RetrySuccessDecides 覆盖：尝试 1 超时后迟到返回
// 失败，重试自身成功 -> 终态 SUCCEEDED、attempt=2；迟到的失败不能算到重试头上。
func TestLateFailureAfterTimeout_RetrySuccessDecides(t *testing.T) {
	t.Log("COVERAGE: 尝试1超时 -> 迟到返回失败 -> 重试自身成功 => SUCCEEDED attempt=2，迟到失败被丢弃")
	sink := newTestSink(t)
	e := NewEngine(NewMemoryStore(), WithEventSink(sink))

	var runs int32
	releaseLate := make(chan struct{})     // 放行尝试 1 的迟到返回
	attempt2Started := make(chan struct{}) // 重试已开始执行
	releaseRetry := make(chan struct{})    // 放行重试自己的返回
	specs := []TaskSpec{{
		ID:      "late-failure",
		Timeout: 150 * time.Millisecond,
		Fn: func(ctx context.Context) error {
			n := atomic.AddInt32(&runs, 1)
			if n == 1 {
				<-releaseLate // 卡在不受取消影响的外部调用里
				return errors.New("late failure from attempt 1")
			}
			close(attempt2Started)
			<-releaseRetry
			return nil // 重试自身成功
		},
		Retry: RetryPolicy{MaxRetries: 1, Backoff: ConstantBackoff{Interval: 5 * time.Millisecond}},
	}}
	if err := e.Submit("wf", specs); err != nil {
		t.Fatal(err)
	}
	out := triggerInBackground(e, "wf")

	<-attempt2Started // 重试在跑：此时放行尝试 1 的迟到失败
	close(releaseLate)
	// 给协调器处理迟到回报的时间：重试仍阻塞在自己的返回上（未超其超时额度），
	// 迟到结果确定地落在“重试仍在执行”的窗口内。
	time.Sleep(50 * time.Millisecond)
	close(releaseRetry) // 重试自身随后返回成功

	res := <-out
	if res.err != nil {
		t.Fatal(res.err)
	}
	wf := res.wf
	if wf.Status != WfSucceeded {
		t.Fatalf("retry succeeded on its own: want SUCCEEDED, got %s", wf.Status)
	}
	ts := wf.Tasks["late-failure"]
	if ts.Status != StatusSucceeded || ts.Attempt != 2 {
		t.Fatalf("want SUCCEEDED attempt=2, got %s attempt=%d", ts.Status, ts.Attempt)
	}
	if got := atomic.LoadInt32(&runs); got != 2 {
		t.Fatalf("fn must run exactly once per attempt, got %d", got)
	}
	if ts.LastError != "" {
		t.Fatalf("success must clear LastError, got %q", ts.LastError)
	}
	// 事件一一对上：1 次超时 + 1 次成功（attempt 2），不得出现业务失败事件
	// （迟到的失败若被算到重试头上，会产生 ATTEMPT_FAILED 并多扣一次重试）。
	var timeoutEv, successEv, failedEv int
	for _, ev := range sink.events() {
		if ev.TaskID != "late-failure" {
			continue
		}
		switch ev.Reason {
		case ReasonAttemptTimeout:
			timeoutEv++
		case ReasonSucceeded:
			successEv++
			if !strings.Contains(ev.Detail, "attempt 2") {
				t.Fatalf("success must belong to attempt 2, got %q", ev.Detail)
			}
		case ReasonAttemptFailed:
			failedEv++
		}
	}
	if timeoutEv != 1 || successEv != 1 || failedEv != 0 {
		t.Fatalf("want 1 ATTEMPT_TIMEOUT + 1 SUCCEEDED + 0 ATTEMPT_FAILED, got %d/%d/%d",
			timeoutEv, successEv, failedEv)
	}
	t.Log("RESULT: PASS 迟到的失败未污染重试，状态/尝试次数/事件一一对上")
}

// TestLateSuccessAfterTimeout_RetryFailureDecides 覆盖：尝试 1 超时后迟到返回
// 成功，重试自身失败且额度用尽 -> 终态 FAILED；迟到的成功不能翻盘、不能把
// 工作流提前推进到终态。
func TestLateSuccessAfterTimeout_RetryFailureDecides(t *testing.T) {
	t.Log("COVERAGE: 尝试1超时 -> 迟到返回成功 -> 重试自身失败且额度用尽 => FAILED，迟到成功不翻盘")
	sink := newTestSink(t)
	e := NewEngine(NewMemoryStore(), WithEventSink(sink))

	var runs int32
	releaseLate := make(chan struct{})
	attempt2Started := make(chan struct{})
	releaseRetry := make(chan struct{})
	specs := []TaskSpec{{
		ID:      "late-success",
		Timeout: 150 * time.Millisecond,
		Fn: func(ctx context.Context) error {
			n := atomic.AddInt32(&runs, 1)
			if n == 1 {
				<-releaseLate
				return nil // 迟到的成功
			}
			close(attempt2Started)
			<-releaseRetry
			return errors.New("retry failed on its own")
		},
		Retry: RetryPolicy{MaxRetries: 1, Backoff: ConstantBackoff{Interval: 5 * time.Millisecond}},
	}}
	if err := e.Submit("wf", specs); err != nil {
		t.Fatal(err)
	}
	out := triggerInBackground(e, "wf")

	<-attempt2Started
	close(releaseLate) // 迟到的成功在重试仍在跑时回到协调器
	time.Sleep(50 * time.Millisecond)
	close(releaseRetry) // 重试自身失败

	res := <-out
	if res.err != nil {
		t.Fatal(res.err)
	}
	wf := res.wf
	if wf.Status != WfFailed {
		t.Fatalf("retry failed on its own: want FAILED, got %s", wf.Status)
	}
	ts := wf.Tasks["late-success"]
	if ts.Status != StatusFailed || ts.Attempt != 2 {
		t.Fatalf("want FAILED attempt=2, got %s attempt=%d", ts.Status, ts.Attempt)
	}
	if got := atomic.LoadInt32(&runs); got != 2 {
		t.Fatalf("fn must run exactly once per attempt, got %d", got)
	}
	if !strings.Contains(ts.LastError, "retry failed on its own") {
		t.Fatalf("LastError must come from the retry itself, got %q", ts.LastError)
	}
	for _, ev := range sink.events() {
		if ev.TaskID == "late-success" && ev.Reason == ReasonSucceeded {
			t.Fatal("late success from timed-out attempt must not be recorded")
		}
	}
	t.Log("RESULT: PASS 迟到的成功未翻盘，终态由重试自身结果决定")
}

// TestLateSuccessAfterTimeout_RetrySucceeds 覆盖：尝试 1 超时后迟到返回成功，
// 重试自身也成功 -> 终态 SUCCEEDED 必须来自重试自己的回报，工作流不得在
// 重试返回前被迟到的成功提前推进到终态。
func TestLateSuccessAfterTimeout_RetrySucceeds(t *testing.T) {
	t.Log("COVERAGE: 尝试1超时 -> 迟到返回成功 -> 重试自身也成功 => 终态来自重试自己的回报，不提前收场")
	sink := newTestSink(t)
	e := NewEngine(NewMemoryStore(), WithEventSink(sink))

	var runs int32
	var retryReturnedAt int64            // 重试自身返回的时刻（unix nano）
	retryReturned := make(chan struct{}) // 重试自身已返回（同步断言时序用）
	releaseLate := make(chan struct{})
	attempt2Started := make(chan struct{})
	releaseRetry := make(chan struct{})
	specs := []TaskSpec{{
		ID:      "late-success-2",
		Timeout: 150 * time.Millisecond,
		Fn: func(ctx context.Context) error {
			n := atomic.AddInt32(&runs, 1)
			if n == 1 {
				<-releaseLate
				return nil // 迟到的成功
			}
			close(attempt2Started)
			<-releaseRetry
			atomic.StoreInt64(&retryReturnedAt, time.Now().UnixNano())
			close(retryReturned)
			return nil
		},
		Retry: RetryPolicy{MaxRetries: 2, Backoff: ConstantBackoff{Interval: 5 * time.Millisecond}},
	}}
	if err := e.Submit("wf", specs); err != nil {
		t.Fatal(err)
	}
	out := triggerInBackground(e, "wf")

	<-attempt2Started
	close(releaseLate)
	time.Sleep(50 * time.Millisecond)
	close(releaseRetry)

	res := <-out
	if res.err != nil {
		t.Fatal(res.err)
	}
	<-retryReturned // 确保重试自身已返回，再比对终态记录时刻
	wf := res.wf
	if wf.Status != WfSucceeded || wf.Tasks["late-success-2"].Status != StatusSucceeded {
		t.Fatalf("want SUCCEEDED, got wf=%s task=%s", wf.Status, wf.Tasks["late-success-2"].Status)
	}
	if wf.Tasks["late-success-2"].Attempt != 2 {
		t.Fatalf("want attempt=2, got %d", wf.Tasks["late-success-2"].Attempt)
	}
	if got := atomic.LoadInt32(&runs); got != 2 {
		t.Fatalf("fn must run exactly once per attempt, got %d", got)
	}
	var successEv, wfDoneEv int
	for _, ev := range sink.events() {
		if ev.TaskID == "late-success-2" && ev.Reason == ReasonSucceeded {
			successEv++
			// 终态必须记录在重试自身返回之后：迟到成功不得提前收场。
			if ev.Time.UnixNano() < atomic.LoadInt64(&retryReturnedAt) {
				t.Fatalf("SUCCEEDED recorded before the retry itself returned: %v < %v",
					ev.Time, time.Unix(0, atomic.LoadInt64(&retryReturnedAt)))
			}
		}
		if ev.Reason == ReasonWfDone {
			wfDoneEv++
		}
	}
	if successEv != 1 || wfDoneEv != 1 {
		t.Fatalf("want exactly 1 SUCCEEDED + 1 WF_DONE, got %d/%d", successEv, wfDoneEv)
	}
	t.Log("RESULT: PASS 终态来自重试自身回报，未被迟到成功提前推进")
}

// TestLateSuccessAfterTimeout_RetriesExhausted 覆盖：每次尝试都超时且都迟到
// 返回成功，重试额度耗尽 -> 终态 FAILED；迟到的成功不能把已定终态翻回去。
func TestLateSuccessAfterTimeout_RetriesExhausted(t *testing.T) {
	t.Log("COVERAGE: 每次尝试都超时且迟到返回成功 -> 额度用尽 => FAILED，迟到成功不翻终态")
	sink := newTestSink(t)
	e := NewEngine(NewMemoryStore(), WithEventSink(sink))

	var runs int32
	releaseLate := make(chan struct{}) // 放行所有迟到返回
	specs := []TaskSpec{{
		ID:      "always-late",
		Timeout: 50 * time.Millisecond,
		Fn: func(ctx context.Context) error {
			atomic.AddInt32(&runs, 1)
			<-releaseLate // 每次尝试都卡住，超时后才返回成功
			return nil
		},
		Retry: RetryPolicy{MaxRetries: 1, Backoff: ConstantBackoff{Interval: 5 * time.Millisecond}},
	}}
	if err := e.Submit("wf", specs); err != nil {
		t.Fatal(err)
	}
	out := triggerInBackground(e, "wf")

	// 等两次尝试都被判超时、任务落终态后，再放行全部迟到返回。
	waitFor(t, "both attempts started", func() bool { return atomic.LoadInt32(&runs) == 2 })
	res := <-out // 两次尝试都不会自己返回，终态只能由超时判定推出
	if res.err != nil {
		t.Fatal(res.err)
	}
	wf := res.wf
	if wf.Status != WfFailed {
		t.Fatalf("all attempts timed out: want FAILED, got %s", wf.Status)
	}
	ts := wf.Tasks["always-late"]
	if ts.Status != StatusFailed || ts.Attempt != 2 {
		t.Fatalf("want FAILED attempt=2, got %s attempt=%d", ts.Status, ts.Attempt)
	}
	if !strings.Contains(ts.LastError, ErrTaskTimeout.Error()) {
		t.Fatalf("LastError must be the timeout, got %q", ts.LastError)
	}

	close(releaseLate) // 迟到的成功现在才回来
	time.Sleep(100 * time.Millisecond)
	after, err := e.GetState(context.Background(), "wf")
	if err != nil {
		t.Fatal(err)
	}
	if after.Status != WfFailed || after.Tasks["always-late"].Status != StatusFailed {
		t.Fatalf("terminal state must not roll back, got wf=%s task=%s",
			after.Status, after.Tasks["always-late"].Status)
	}
	for _, ev := range sink.events() {
		if ev.TaskID == "always-late" && ev.Reason == ReasonSucceeded {
			t.Fatal("late success must never be recorded after timeout adjudication")
		}
	}
	t.Log("RESULT: PASS 重试耗尽落 FAILED，迟到成功未翻回终态")
}

// TestLateResultWithInterruptAndResume 覆盖：任务不响应取消、超时后迟到返回，
// 退避期间中断 -> 续跑重试成功；旧运行的迟到回报不得泄漏进续跑，尝试计数
// 不重复、状态不回退。
func TestLateResultWithInterruptAndResume(t *testing.T) {
	t.Log("COVERAGE: 超时+迟到返回+退避期间中断 -> 续跑重试成功 => SUCCEEDED attempt=2，旧回报不泄漏")
	sink := newTestSink(t)
	e := NewEngine(NewMemoryStore(), WithEventSink(sink))

	var runs int32
	releaseLate := make(chan struct{})
	specs := []TaskSpec{{
		ID:      "flaky-late",
		Timeout: 50 * time.Millisecond,
		Fn: func(ctx context.Context) error {
			n := atomic.AddInt32(&runs, 1)
			if n == 1 {
				<-releaseLate // 不响应取消：超时被判后仍卡住，由测试放行
				return errors.New("late failure from interrupted attempt")
			}
			return nil
		},
		Retry: RetryPolicy{MaxRetries: 2, Backoff: ConstantBackoff{Interval: 400 * time.Millisecond}},
	}}
	if err := e.Submit("wf", specs); err != nil {
		t.Fatal(err)
	}
	out := triggerInBackground(e, "wf")

	waitFor(t, "task enters RETRYING after timeout", func() bool {
		wf, err := e.GetState(context.Background(), "wf")
		return err == nil && wf.Tasks["flaky-late"].Status == StatusRetrying
	})
	if !e.Cancel("wf") {
		t.Fatal("cancel returned false for in-flight run")
	}
	<-out // 第一次触发随中断收尾返回
	mid, err := e.GetState(context.Background(), "wf")
	if err != nil {
		t.Fatal(err)
	}
	if mid.Tasks["flaky-late"].Status != StatusReady {
		t.Fatalf("interrupted retrying task must be reclaimed to READY, got %s",
			mid.Tasks["flaky-late"].Status)
	}

	// 续跑与尝试 1 的迟到返回并发发生：两种交错下结论都必须一致。
	resumeOut := make(chan triggerOutcome, 1)
	go func() {
		wf, err := e.Resume(context.Background(), "wf")
		resumeOut <- triggerOutcome{wf: wf, err: err}
	}()
	close(releaseLate) // 旧运行的迟到回报此刻才发出
	res := <-resumeOut
	if res.err != nil {
		t.Fatal(res.err)
	}
	wf := res.wf
	if wf.Status != WfSucceeded || wf.Tasks["flaky-late"].Status != StatusSucceeded {
		t.Fatalf("resume want SUCCEEDED, got wf=%s task=%s", wf.Status, wf.Tasks["flaky-late"].Status)
	}
	if wf.Tasks["flaky-late"].Attempt != 2 {
		t.Fatalf("timeout consumed exactly one attempt, want attempt=2, got %d",
			wf.Tasks["flaky-late"].Attempt)
	}
	// 等迟到回报确定发出后再次确认：不多跑、不回退。
	time.Sleep(100 * time.Millisecond)
	if got := atomic.LoadInt32(&runs); got != 2 {
		t.Fatalf("late result must not cause extra dispatch, runs=%d want 2", got)
	}
	after, err := e.GetState(context.Background(), "wf")
	if err != nil {
		t.Fatal(err)
	}
	if after.Status != WfSucceeded || after.Tasks["flaky-late"].Status != StatusSucceeded {
		t.Fatalf("terminal state must not roll back, got wf=%s task=%s",
			after.Status, after.Tasks["flaky-late"].Status)
	}
	t.Log("RESULT: PASS 超时+中断+续跑+迟到返回叠加，结论一致且不多派任务")
}

// TestLateResult_ConcurrentTriggersUniqueConclusion 覆盖：任务超时且迟到返回
// 的场景下并发/重复触发共享同一次执行，结论唯一，不因并发多派任务。
func TestLateResult_ConcurrentTriggersUniqueConclusion(t *testing.T) {
	t.Log("COVERAGE: 超时+迟到返回场景下 5 路并发触发 -> 结论唯一 SUCCEEDED，任务只跑一轮")
	sink := newTestSink(t)
	e := NewEngine(NewMemoryStore(), WithEventSink(sink))

	var runs int32
	releaseLate := make(chan struct{})
	attempt2Started := make(chan struct{})
	var closeOnce sync.Once
	specs := []TaskSpec{{
		ID:      "late-concurrent",
		Timeout: 50 * time.Millisecond,
		Fn: func(ctx context.Context) error {
			n := atomic.AddInt32(&runs, 1)
			if n == 1 {
				<-releaseLate
				return nil // 迟到的成功
			}
			closeOnce.Do(func() { close(attempt2Started) })
			return nil
		},
		Retry: RetryPolicy{MaxRetries: 2, Backoff: ConstantBackoff{Interval: 5 * time.Millisecond}},
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
	<-attempt2Started // 重试在跑时放行迟到返回，制造最严苛的交错
	close(releaseLate)
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("trigger %d: %v", i, err)
		}
		if states[i].Status != WfSucceeded {
			t.Fatalf("trigger %d: want SUCCEEDED, got %s", i, states[i].Status)
		}
		if states[i].Tasks["late-concurrent"].Attempt != 2 {
			t.Fatalf("trigger %d: want attempt=2, got %d",
				i, states[i].Tasks["late-concurrent"].Attempt)
		}
	}
	// 等迟到回报确定发出后确认：没有额外派发，终态不回退。
	time.Sleep(100 * time.Millisecond)
	if got := atomic.LoadInt32(&runs); got != 2 {
		t.Fatalf("concurrent triggers share one run; fn invocations=%d want 2", got)
	}
	after, err := e.GetState(context.Background(), "wf")
	if err != nil {
		t.Fatal(err)
	}
	if after.Status != WfSucceeded {
		t.Fatalf("terminal state must not roll back, got %s", after.Status)
	}
	t.Log("RESULT: PASS 并发触发结论唯一，迟到结果未造成多派或回退")
}
