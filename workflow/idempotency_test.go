package workflow

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestIdempotent_RepeatedTrigger 验证终态后重复触发：终态唯一、任务不重复执行。
func TestIdempotent_RepeatedTrigger(t *testing.T) {
	e := newTestEngine(t)
	var execCount int64
	tasks := []*Task{
		{ID: "a", Run: func(context.Context, int) error { atomic.AddInt64(&execCount, 1); return nil }},
		{ID: "b", Deps: []string{"a"}, Run: func(context.Context, int) error {
			atomic.AddInt64(&execCount, 1)
			return nil
		}},
	}
	if err := e.Submit("idemp", tasks); err != nil {
		t.Fatal(err)
	}
	r1, err := e.Trigger(context.Background(), "idemp", 1)
	if err != nil {
		t.Fatal(err)
	}
	if r1.Status != StatusSucceeded {
		t.Fatalf("first trigger = %s", r1.Status)
	}
	for i := 0; i < 5; i++ {
		r, err := e.Trigger(context.Background(), "idemp", 1)
		if !errors.Is(err, ErrWorkflowConverged) {
			t.Fatalf("trigger #%d err = %v, want ErrWorkflowConverged", i+2, err)
		}
		if r.Status != StatusSucceeded {
			t.Fatalf("trigger #%d status = %s, want SUCCEEDED", i+2, r.Status)
		}
	}
	if got := atomic.LoadInt64(&execCount); got != 2 {
		t.Fatalf("total executions = %d, want exactly 2 (repeated triggers must not re-run)", got)
	}
	// Resume 一个已终态工作流也不应重跑，返回同一终态。
	r3, err := e.Resume(context.Background(), "idemp")
	if err != nil {
		t.Fatalf("resume converged workflow: %v", err)
	}
	if r3.Status != StatusSucceeded {
		t.Fatalf("resume = %s", r3.Status)
	}
	if got := atomic.LoadInt64(&execCount); got != 2 {
		t.Fatalf("total executions after resume = %d, want 2", got)
	}
}

// TestIdempotent_ConcurrentTriggers 验证并发触发：无论多少 goroutine 同时触发，
// 任务只执行一次，所有调用方观察到同一个终态。
func TestIdempotent_ConcurrentTriggers(t *testing.T) {
	e := newTestEngine(t)
	var execCount int64
	barrier := make(chan struct{})
	tasks := []*Task{
		{ID: "a", Run: func(context.Context, int) error {
			atomic.AddInt64(&execCount, 1)
			time.Sleep(30 * time.Millisecond) // 放大窗口，确保并发触发在运行期间到达
			return nil
		}},
		{ID: "b", Deps: []string{"a"}, Run: func(context.Context, int) error {
			atomic.AddInt64(&execCount, 1)
			return nil
		}},
	}
	if err := e.Submit("concurrent-trig", tasks); err != nil {
		t.Fatal(err)
	}
	const n = 20
	var wg sync.WaitGroup
	statuses := make([]Status, n)
	errs := make([]error, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(idx int) {
			defer wg.Done()
			<-barrier // 所有 goroutine 同时发起触发
			r, err := e.Trigger(context.Background(), "concurrent-trig", 1)
			statuses[idx] = r.Status
			errs[idx] = err
		}(i)
	}
	close(barrier)
	wg.Wait()

	successOrConverged := 0
	for i := 0; i < n; i++ {
		if statuses[i] != StatusSucceeded {
			t.Fatalf("caller %d observed status %s, want SUCCEEDED", i, statuses[i])
		}
		if errs[i] == nil {
			successOrConverged++
		} else if !errors.Is(errs[i], ErrWorkflowConverged) {
			t.Fatalf("caller %d unexpected err %v", i, errs[i])
		}
	}
	if successOrConverged != 1 {
		t.Fatalf("exactly one caller should own the run, got %d", successOrConverged)
	}
	if got := atomic.LoadInt64(&execCount); got != 2 {
		t.Fatalf("total executions under concurrent triggers = %d, want exactly 2", got)
	}
	t.Logf("%d concurrent triggers converged to one SUCCEEDED run; execution count=%d", n, execCount)
}

// TestIdempotent_ConcurrentTriggers_InterleavedWithResume 验证触发与续跑并发时
// 也只有一次执行，不存在漏跑或重复跑。
func TestIdempotent_ConcurrentTriggers_InterleavedWithResume(t *testing.T) {
	e := newTestEngine(t)
	var execCount int64
	tasks := []*Task{
		{ID: "a", Run: func(context.Context, int) error {
			atomic.AddInt64(&execCount, 1)
			time.Sleep(20 * time.Millisecond)
			return nil
		}},
	}
	if err := e.Submit("mix", tasks); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			r, err := e.Trigger(context.Background(), "mix", 0)
			if err != nil && !errors.Is(err, ErrWorkflowConverged) {
				t.Errorf("trigger: %v", err)
				return
			}
			if r.Status != StatusSucceeded {
				t.Errorf("trigger status = %s", r.Status)
			}
		}()
		go func() {
			defer wg.Done()
			r, err := e.Resume(context.Background(), "mix")
			if err != nil {
				t.Errorf("resume: %v", err)
				return
			}
			if r.Status != StatusSucceeded {
				t.Errorf("resume status = %s", r.Status)
			}
		}()
	}
	wg.Wait()
	if got := atomic.LoadInt64(&execCount); got != 1 {
		t.Fatalf("task executed %d times, want 1", got)
	}
}
