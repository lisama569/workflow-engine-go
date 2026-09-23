package workflow

import (
	"context"
	"sync"
	"testing"
	"time"
)

// flakyTask 前 failTimes 次返回失败，之后成功；记录每次尝试看到的 attempt 序号。
func flakyTask(id string, p RetryPolicy, failTimes int, attempts *[]int, mu *sync.Mutex) *Task {
	return &Task{ID: id, Retry: p, Run: func(_ context.Context, attempt int) error {
		mu.Lock()
		*attempts = append(*attempts, attempt)
		n := len(*attempts)
		mu.Unlock()
		if n <= failTimes {
			return errFlaky
		}
		return nil
	}}
}

type sentinelErr string

func (e sentinelErr) Error() string { return string(e) }

const errFlaky sentinelErr = "simulated transient failure"

// TestRetry_SucceedsWithinBudget 验证失败后按退避重试，成功即止且尝试计数正确。
func TestRetry_SucceedsWithinBudget(t *testing.T) {
	e := newTestEngine(t)
	var mu sync.Mutex
	var attempts []int
	tasks := []*Task{
		flakyTask("a", RetryPolicy{MaxAttempts: 3, InitialBackoff: 5 * time.Millisecond, Multiplier: 2}, 2, &attempts, &mu),
	}
	if err := e.Submit("retry-ok", tasks); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	res, err := e.Trigger(context.Background(), "retry-ok", 1)
	if err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(start)
	if res.Status != StatusSucceeded {
		t.Fatalf("status = %s, want SUCCEEDED", res.Status)
	}
	requireAttempts(t, e, "retry-ok", "a", 3)
	mu.Lock()
	got := append([]int(nil), attempts...)
	mu.Unlock()
	if len(got) != 3 {
		t.Fatalf("task ran %d times, want 3", len(got))
	}
	// attempt 序号必须是 1,2,3（重试不回退、不重复计数）。
	for i, v := range got {
		if v != i+1 {
			t.Fatalf("attempt sequence = %v, want [1 2 3]", got)
		}
	}
	// 指数退避：5ms + 10ms = 至少 15ms。
	if elapsed < 15*time.Millisecond {
		t.Fatalf("elapsed %s too short, backoff not applied", elapsed)
	}
	t.Logf("retry succeeded after %d attempts in %s", len(got), elapsed)
}

// TestRetry_ExhaustedLeadsToTerminalFailure 验证重试耗尽进入终态失败，
// 且下游被确定性 SKIPPED，工作流整体 FAILED。
func TestRetry_ExhaustedLeadsToTerminalFailure(t *testing.T) {
	e := newTestEngine(t)
	var mu sync.Mutex
	var attemptsA, attemptsC []int
	tasks := []*Task{
		flakyTask("a", RetryPolicy{MaxAttempts: 3, InitialBackoff: time.Millisecond, Multiplier: 1}, 10, &attemptsA, &mu),
		okTask("b", "a"), // a 失败 -> b 必跳过
		okTask("c"),      // 独立任务，应照常成功
		flakyTask("d", RetryPolicy{MaxAttempts: 2, InitialBackoff: time.Millisecond}, 10, &attemptsC, &mu),
	}
	if err := e.Submit("retry-exhaust", tasks); err != nil {
		t.Fatal(err)
	}
	res, err := e.Trigger(context.Background(), "retry-exhaust", 0)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != StatusFailed {
		t.Fatalf("workflow status = %s, want FAILED", res.Status)
	}
	requireStatus(t, e, "retry-exhaust", "a", StatusFailed)
	requireStatus(t, e, "retry-exhaust", "b", StatusSkipped)
	requireStatus(t, e, "retry-exhaust", "c", StatusSucceeded)
	requireStatus(t, e, "retry-exhaust", "d", StatusFailed)
	requireAttempts(t, e, "retry-exhaust", "a", 3) // 恰好耗尽 3 次，不多跑
	mu.Lock()
	nD := len(attemptsC)
	mu.Unlock()
	if nD != 2 {
		t.Fatalf("d ran %d times, want exactly 2", nD)
	}
	// SKIPPED 的任务绝不能执行：attempts 列表为空（b 没有记录器，间接由 b 从未 READY 保证）。
	seq := eventSequenceFor(e, "retry-exhaust")
	t.Logf("event sequence: %s", sequenceString(seq))
	for _, s := range seq {
		if s == "b:PENDING->RUNNING" {
			t.Fatalf("skipped task b must never run, saw %s", s)
		}
	}
}

// TestFailurePropagation_TransitiveSkip 验证失败沿传递依赖链跳过。
func TestFailurePropagation_TransitiveSkip(t *testing.T) {
	e := newTestEngine(t)
	var mu sync.Mutex
	var attempts []int
	tasks := []*Task{
		flakyTask("a", RetryPolicy{MaxAttempts: 1}, 1, &attempts, &mu),
		okTask("b", "a"),
		okTask("c", "b"),
		okTask("d", "c"),
		okTask("z"), // 独立任务成功
	}
	if err := e.Submit("chain-fail", tasks); err != nil {
		t.Fatal(err)
	}
	res, err := e.Trigger(context.Background(), "chain-fail", 0)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != StatusFailed {
		t.Fatalf("status = %s want FAILED", res.Status)
	}
	for _, id := range []string{"a"} {
		requireStatus(t, e, "chain-fail", id, StatusFailed)
	}
	for _, id := range []string{"b", "c", "d"} {
		requireStatus(t, e, "chain-fail", id, StatusSkipped)
	}
	requireStatus(t, e, "chain-fail", "z", StatusSucceeded)
}
