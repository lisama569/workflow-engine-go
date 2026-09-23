package workflow

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// testSink 既把每条状态迁移打印到测试日志（go test -v 可见），
// 又留存事件序列供断言触发依据与迁移自洽性。
type testSink struct {
	t  *testing.T
	mu sync.Mutex
	ev []Event
}

func newTestSink(t *testing.T) *testSink {
	t.Helper()
	return &testSink{t: t}
}

func (s *testSink) OnEvent(e Event) {
	s.mu.Lock()
	s.ev = append(s.ev, e)
	s.mu.Unlock()
	target := e.Workflow
	if e.TaskID != "" {
		target += "/" + e.TaskID
	}
	from := e.From
	if from == "" {
		from = "-"
	}
	s.t.Logf("[EVENT] %-14s %-22s %-9s -> %-9s | %s",
		e.Reason, target, from, e.To, e.Detail)
}

func (s *testSink) events() []Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Event, len(s.ev))
	copy(out, s.ev)
	return out
}

func (s *testSink) reasonsFor(task string) []EventReason {
	var rs []EventReason
	for _, e := range s.events() {
		if e.TaskID == task {
			rs = append(rs, e.Reason)
		}
	}
	return rs
}

// okFn 恒成功任务。
func okFn() TaskFunc {
	return func(ctx context.Context) error { return nil }
}

func TestSubmit_ValidationErrors(t *testing.T) {
	cases := []struct {
		name  string
		specs []TaskSpec
		want  error
	}{
		{
			name:  "empty workflow",
			specs: nil,
			want:  ErrEmptyWorkflow,
		},
		{
			name: "duplicate task id",
			specs: []TaskSpec{
				{ID: "a", Fn: okFn()},
				{ID: "a", Fn: okFn()},
			},
			want: ErrDuplicateTask,
		},
		{
			name: "missing dependency",
			specs: []TaskSpec{
				{ID: "a", Deps: []string{"ghost"}, Fn: okFn()},
			},
			want: ErrDependencyMissing,
		},
		{
			name: "self cycle",
			specs: []TaskSpec{
				{ID: "a", Deps: []string{"a"}, Fn: okFn()},
			},
			want: ErrCycleDetected,
		},
		{
			name: "multi node cycle",
			specs: []TaskSpec{
				{ID: "a", Deps: []string{"c"}, Fn: okFn()},
				{ID: "b", Deps: []string{"a"}, Fn: okFn()},
				{ID: "c", Deps: []string{"b"}, Fn: okFn()},
			},
			want: ErrCycleDetected,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := NewEngine(NewMemoryStore(), WithEventSink(newTestSink(t)))
			err := e.Submit("wf", tc.specs)
			if !errors.Is(err, tc.want) {
				t.Fatalf("want error %v, got %v", tc.want, err)
			}
			var ve *ValidationError
			if !errors.As(err, &ve) {
				t.Fatalf("want *ValidationError for distinguishability, got %T: %v", err, err)
			}
			t.Logf("rejected at submit as expected: %v", ve)
		})
	}
}

func TestSubmit_InvalidConcurrency(t *testing.T) {
	e := NewEngine(NewMemoryStore(), WithEventSink(newTestSink(t)))
	err := e.Submit("wf", []TaskSpec{{ID: "a", Fn: okFn()}}, WithConcurrency(0))
	if !errors.Is(err, ErrInvalidConcurrency) {
		t.Fatalf("want ErrInvalidConcurrency, got %v", err)
	}
}

func TestHappyPath_DependencyOrder(t *testing.T) {
	sink := newTestSink(t)
	e := NewEngine(NewMemoryStore(), WithEventSink(sink))

	var mu sync.Mutex
	var order []string
	rec := func(id string, deps ...string) TaskSpec {
		return TaskSpec{ID: id, Deps: deps, Fn: func(ctx context.Context) error {
			time.Sleep(2 * time.Millisecond)
			mu.Lock()
			order = append(order, id)
			mu.Unlock()
			return nil
		}}
	}
	specs := []TaskSpec{
		rec("a"),
		rec("b", "a"),
		rec("c", "a"),
		rec("d", "b", "c"),
	}
	if err := e.Submit("wf", specs, WithConcurrency(4)); err != nil {
		t.Fatal(err)
	}
	wf, err := e.Trigger(context.Background(), "wf")
	if err != nil {
		t.Fatal(err)
	}
	if wf.Status != WfSucceeded {
		t.Fatalf("want SUCCEEDED, got %s", wf.Status)
	}
	pos := map[string]int{}
	for i, id := range order {
		pos[id] = i
	}
	if !(pos["a"] < pos["b"] && pos["a"] < pos["c"] && pos["b"] < pos["d"] && pos["c"] < pos["d"]) {
		t.Fatalf("dependency order violated: %v", order)
	}
	for _, id := range []string{"a", "b", "c", "d"} {
		if wf.Tasks[id].Status != StatusSucceeded || wf.Tasks[id].Attempt != 1 {
			t.Fatalf("task %s unexpected state: %+v", id, wf.Tasks[id])
		}
	}
}

func TestConcurrencyLimit(t *testing.T) {
	for _, limit := range []int{1, 2, 3} {
		t.Run(fmt.Sprintf("limit=%d", limit), func(t *testing.T) {
			sink := newTestSink(t)
			e := NewEngine(NewMemoryStore(), WithEventSink(sink))

			var inFlight, maxFlight int32
			var specs []TaskSpec
			for i := 0; i < 6; i++ {
				id := fmt.Sprintf("t%d", i)
				specs = append(specs, TaskSpec{ID: id, Fn: func(ctx context.Context) error {
					cur := atomic.AddInt32(&inFlight, 1)
					for {
						old := atomic.LoadInt32(&maxFlight)
						if cur <= old || atomic.CompareAndSwapInt32(&maxFlight, old, cur) {
							break
						}
					}
					time.Sleep(5 * time.Millisecond)
					atomic.AddInt32(&inFlight, -1)
					return nil
				}})
			}
			if err := e.Submit("wf", specs, WithConcurrency(limit)); err != nil {
				t.Fatal(err)
			}
			wf, err := e.Trigger(context.Background(), "wf")
			if err != nil {
				t.Fatal(err)
			}
			if wf.Status != WfSucceeded {
				t.Fatalf("want SUCCEEDED, got %s", wf.Status)
			}
			if got := atomic.LoadInt32(&maxFlight); got != int32(limit) {
				t.Fatalf("concurrency limit violated: max in-flight=%d, want %d", got, limit)
			}
		})
	}
}

func TestRetryExhausted(t *testing.T) {
	sink := newTestSink(t)
	e := NewEngine(NewMemoryStore(), WithEventSink(sink))

	var attempts int32
	specs := []TaskSpec{{
		ID: "flaky",
		Fn: func(ctx context.Context) error {
			atomic.AddInt32(&attempts, 1)
			return errors.New("boom")
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
	if wf.Status != WfFailed {
		t.Fatalf("want FAILED workflow, got %s", wf.Status)
	}
	ts := wf.Tasks["flaky"]
	if ts.Status != StatusFailed {
		t.Fatalf("want task FAILED, got %s", ts.Status)
	}
	if ts.Attempt != 3 {
		t.Fatalf("want 3 attempts (1 + 2 retries), got %d", ts.Attempt)
	}
	if atomic.LoadInt32(&attempts) != 3 {
		t.Fatalf("want fn invoked 3 times, got %d", attempts)
	}
	if !strings.Contains(ts.LastError, "boom") {
		t.Fatalf("want distinguishable last error, got %q", ts.LastError)
	}
	reasons := sink.reasonsFor("flaky")
	var failReasons int
	for _, r := range reasons {
		if r == ReasonRetriesExhausted {
			failReasons++
		}
	}
	if failReasons != 1 {
		t.Fatalf("want exactly one RETRIES_EXHAUSTED event, got %v", reasons)
	}
}

func TestRetryThenSuccess(t *testing.T) {
	sink := newTestSink(t)
	e := NewEngine(NewMemoryStore(), WithEventSink(sink))

	var attempts int32
	specs := []TaskSpec{{
		ID: "recovering",
		Fn: func(ctx context.Context) error {
			n := atomic.AddInt32(&attempts, 1)
			if n < 3 {
				return fmt.Errorf("transient failure #%d", n)
			}
			return nil
		},
		Retry: RetryPolicy{MaxRetries: 3, Backoff: LinearBackoff{Interval: time.Millisecond}},
	}}
	if err := e.Submit("wf", specs); err != nil {
		t.Fatal(err)
	}
	wf, err := e.Trigger(context.Background(), "wf")
	if err != nil {
		t.Fatal(err)
	}
	if wf.Status != WfSucceeded || wf.Tasks["recovering"].Status != StatusSucceeded {
		t.Fatalf("want success after retries, got wf=%s task=%s",
			wf.Status, wf.Tasks["recovering"].Status)
	}
	if wf.Tasks["recovering"].Attempt != 3 {
		t.Fatalf("want attempt=3, got %d", wf.Tasks["recovering"].Attempt)
	}
}

func TestDependencyFailure_SkipsDependents(t *testing.T) {
	sink := newTestSink(t)
	e := NewEngine(NewMemoryStore(), WithEventSink(sink))

	var downstreamRan int32
	specs := []TaskSpec{
		{ID: "a", Fn: func(ctx context.Context) error { return errors.New("a broken") }},
		{ID: "b", Deps: []string{"a"}, Fn: func(ctx context.Context) error {
			atomic.AddInt32(&downstreamRan, 1)
			return nil
		}},
		{ID: "c", Fn: okFn()}, // 与失败链无关，应正常成功
	}
	if err := e.Submit("wf", specs, WithConcurrency(2)); err != nil {
		t.Fatal(err)
	}
	wf, err := e.Trigger(context.Background(), "wf")
	if err != nil {
		t.Fatal(err)
	}
	if wf.Status != WfFailed {
		t.Fatalf("want FAILED, got %s", wf.Status)
	}
	if wf.Tasks["a"].Status != StatusFailed {
		t.Fatalf("a want FAILED, got %s", wf.Tasks["a"].Status)
	}
	if wf.Tasks["b"].Status != StatusSkipped {
		t.Fatalf("b want SKIPPED, got %s", wf.Tasks["b"].Status)
	}
	if wf.Tasks["c"].Status != StatusSucceeded {
		t.Fatalf("c want SUCCEEDED, got %s", wf.Tasks["c"].Status)
	}
	if atomic.LoadInt32(&downstreamRan) != 0 {
		t.Fatal("skipped dependent must not run")
	}
	var sawDepFailed bool
	for _, ev := range sink.events() {
		if ev.TaskID == "b" && ev.Reason == ReasonDepFailed {
			sawDepFailed = true
		}
	}
	if !sawDepFailed {
		t.Fatal("missing DEP_FAILED event for skipped task")
	}
}

func TestInterruptAndResume(t *testing.T) {
	sink := newTestSink(t)
	e := NewEngine(NewMemoryStore(), WithEventSink(sink))

	var aRuns, bRuns int32
	aStarted := make(chan struct{})
	bStarted := make(chan struct{})

	specs := []TaskSpec{
		{ID: "a", Fn: func(ctx context.Context) error {
			atomic.AddInt32(&aRuns, 1)
			close(aStarted)
			return nil
		}},
		{ID: "b", Deps: []string{"a"}, Fn: func(ctx context.Context) error {
			n := atomic.AddInt32(&bRuns, 1)
			select {
			case <-bStarted:
			default:
				close(bStarted)
			}
			if n == 1 {
				// 第一次被调度：阻塞至被中断，模拟执行中宕机。
				<-ctx.Done()
				return ctx.Err()
			}
			return nil // 续跑后的第二次执行成功
		}},
		{ID: "c", Deps: []string{"b"}, Fn: okFn()},
	}
	if err := e.Submit("wf", specs, WithConcurrency(2)); err != nil {
		t.Fatal(err)
	}

	triggerDone := make(chan struct{})
	var mid *WorkflowState
	go func() {
		defer close(triggerDone)
		wf, err := e.Trigger(context.Background(), "wf")
		if err != nil {
			t.Errorf("trigger: %v", err)
		}
		mid = wf
	}()

	<-bStarted // 等待 b 真正进入 RUNNING
	if !e.Cancel("wf") {
		t.Fatal("cancel returned false for in-flight run")
	}
	<-triggerDone

	if mid.Status != WfRunning {
		t.Fatalf("interrupted snapshot want RUNNING, got %s", mid.Status)
	}
	if mid.Tasks["a"].Status != StatusSucceeded {
		t.Fatalf("a should be done before interrupt, got %s", mid.Tasks["a"].Status)
	}
	if mid.Tasks["b"].Status != StatusReady {
		t.Fatalf("b should be reclaimed to READY, got %s", mid.Tasks["b"].Status)
	}
	if mid.Tasks["c"].Status != StatusPending {
		t.Fatalf("c should stay PENDING, got %s", mid.Tasks["c"].Status)
	}

	// 断点续跑：已完成的 a 不得重复执行。
	wf, err := e.Resume(context.Background(), "wf")
	if err != nil {
		t.Fatal(err)
	}
	if wf.Status != WfSucceeded {
		t.Fatalf("resume want SUCCEEDED, got %s", wf.Status)
	}
	if atomic.LoadInt32(&aRuns) != 1 {
		t.Fatalf("completed task must not re-run after resume: aRuns=%d", aRuns)
	}
	if atomic.LoadInt32(&bRuns) != 2 {
		t.Fatalf("b should run twice (interrupted + resumed), got %d", bRuns)
	}
	if wf.Tasks["b"].Attempt != 1 {
		t.Fatalf("interrupted attempt must not be re-counted, got attempt=%d", wf.Tasks["b"].Attempt)
	}
	if wf.Tasks["a"].Attempt != 1 {
		t.Fatalf("a attempt must stay 1, got %d", wf.Tasks["a"].Attempt)
	}
}

func TestDuplicateAndConcurrentTriggers(t *testing.T) {
	sink := newTestSink(t)
	e := NewEngine(NewMemoryStore(), WithEventSink(sink))

	var runs int32
	specs := []TaskSpec{
		{ID: "a", Fn: func(ctx context.Context) error {
			atomic.AddInt32(&runs, 1)
			time.Sleep(10 * time.Millisecond)
			return nil
		}},
		{ID: "b", Deps: []string{"a"}, Fn: func(ctx context.Context) error {
			atomic.AddInt32(&runs, 1)
			time.Sleep(5 * time.Millisecond)
			return nil
		}},
	}
	if err := e.Submit("wf", specs, WithConcurrency(1)); err != nil {
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
		if states[i].Status != WfSucceeded {
			t.Fatalf("trigger %d status %s", i, states[i].Status)
		}
	}
	if atomic.LoadInt32(&runs) != 2 {
		t.Fatalf("concurrent triggers must share one run; fn invocations=%d want 2", runs)
	}

	// 终态后重复触发：直接返回同一终态，不执行任何任务。
	again, err := e.Trigger(context.Background(), "wf")
	if err != nil {
		t.Fatal(err)
	}
	if again.Status != WfSucceeded {
		t.Fatalf("re-trigger want SUCCEEDED, got %s", again.Status)
	}
	if atomic.LoadInt32(&runs) != 2 {
		t.Fatalf("terminal re-trigger must not execute tasks: runs=%d", runs)
	}
	var dedup int
	for _, ev := range sink.events() {
		if ev.Reason == ReasonDeduplicated {
			dedup++
		}
	}
	if dedup == 0 {
		t.Fatal("expected at least one DEDUPLICATED event")
	}
}

func TestDeterministicTerminalState(t *testing.T) {
	// 同样的规格重复构建运行，终态与尝试次数必须可复现。
	snapshots := make([]string, 0, 5)
	for i := 0; i < 5; i++ {
		e := NewEngine(NewMemoryStore(), WithEventSink(newTestSink(t)))
		var calls int32
		specs := []TaskSpec{
			{ID: "x", Fn: func(ctx context.Context) error {
				if atomic.AddInt32(&calls, 1) < 2 {
					return errors.New("nope")
				}
				return nil
			}, Retry: RetryPolicy{MaxRetries: 2, Backoff: NoBackoff{}}},
			{ID: "y", Deps: []string{"x"}, Fn: okFn()},
		}
		if err := e.Submit("wf", specs, WithConcurrency(2)); err != nil {
			t.Fatal(err)
		}
		wf, err := e.Trigger(context.Background(), "wf")
		if err != nil {
			t.Fatal(err)
		}
		var buf bytes.Buffer
		fmt.Fprintf(&buf, "%s:", wf.Status)
		for _, id := range []string{"x", "y"} {
			ts := wf.Tasks[id]
			fmt.Fprintf(&buf, "%s=%s(a%d);", id, ts.Status, ts.Attempt)
		}
		snapshots = append(snapshots, buf.String())
	}
	for i := 1; i < len(snapshots); i++ {
		if snapshots[i] != snapshots[0] {
			t.Fatalf("non-deterministic outcomes: %q vs %q", snapshots[0], snapshots[i])
		}
	}
}

func TestTrigger_NotSubmitted(t *testing.T) {
	e := NewEngine(NewMemoryStore(), WithEventSink(newTestSink(t)))
	if _, err := e.Trigger(context.Background(), "ghost"); !errors.Is(err, ErrWorkflowNotFound) {
		t.Fatalf("want ErrWorkflowNotFound, got %v", err)
	}
	if _, err := e.Resume(context.Background(), "ghost"); !errors.Is(err, ErrWorkflowNotFound) {
		t.Fatalf("want ErrWorkflowNotFound, got %v", err)
	}
	if e.Cancel("ghost") {
		t.Fatal("cancel on unknown workflow must return false")
	}
}

func TestResume_AfterTerminalFailure_NoRerun(t *testing.T) {
	sink := newTestSink(t)
	e := NewEngine(NewMemoryStore(), WithEventSink(sink))
	var runs int32
	specs := []TaskSpec{{
		ID: "doomed",
		Fn: func(ctx context.Context) error {
			atomic.AddInt32(&runs, 1)
			return errors.New("permanent")
		},
		Retry: RetryPolicy{MaxRetries: 1, Backoff: NoBackoff{}},
	}}
	if err := e.Submit("wf", specs); err != nil {
		t.Fatal(err)
	}
	wf, err := e.Trigger(context.Background(), "wf")
	if err != nil || wf.Status != WfFailed {
		t.Fatalf("want FAILED, got %s err=%v", wf.Status, err)
	}
	again, err := e.Resume(context.Background(), "wf")
	if err != nil {
		t.Fatal(err)
	}
	if again.Status != WfFailed || again.Tasks["doomed"].Status != StatusFailed {
		t.Fatalf("terminal failure must not roll back, got %s/%s",
			again.Status, again.Tasks["doomed"].Status)
	}
	if atomic.LoadInt32(&runs) != 2 { // 1 次首次 + 1 次重试，续跑零增量
		t.Fatalf("resume after terminal FAILED must not re-run, runs=%d", runs)
	}
}
