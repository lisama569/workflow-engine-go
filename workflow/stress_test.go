package workflow

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestStress_ManyWorkflowsAndTriggers 在多个工作流上以大量 goroutine 交叉
// 触发 Trigger/Resume；每个任务必须恰好成功一次，每个工作流必须收敛成功。
func TestStress_ManyWorkflowsAndTriggers(t *testing.T) {
	e := newTestEngine(t)
	const wfCount = 8
	const triggerPerWF = 12

	var totalRuns int64
	mkTasks := func(prefix string) []*Task {
		run := func(context.Context, int) error {
			atomic.AddInt64(&totalRuns, 1)
			time.Sleep(time.Millisecond)
			return nil
		}
		return []*Task{
			{ID: "a", Run: run},
			{ID: "b", Run: run},
			{ID: "c", Deps: []string{"a", "b"}, Run: run},
			{ID: "d", Deps: []string{"c"}, Run: run},
			{ID: "e", Deps: []string{"a"}, Run: run},
			{ID: "f", Deps: []string{"d", "e"}, Run: run},
		}
	}
	for i := 0; i < wfCount; i++ {
		id := fmt.Sprintf("stress-%d", i)
		if err := e.Submit(id, mkTasks(id)); err != nil {
			t.Fatal(err)
		}
	}
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < wfCount; i++ {
		id := fmt.Sprintf("stress-%d", i)
		for j := 0; j < triggerPerWF; j++ {
			wg.Add(1)
			go func(useResume bool) {
				defer wg.Done()
				<-start
				var r *RunResult
				var err error
				if useResume {
					r, err = e.Resume(context.Background(), id)
				} else {
					r, err = e.Trigger(context.Background(), id, 3)
				}
				if err != nil && !errors.Is(err, ErrWorkflowConverged) {
					t.Errorf("%s: unexpected err %v", id, err)
					return
				}
				if r.Status != StatusSucceeded {
					t.Errorf("%s: status %s", id, r.Status)
				}
			}(j%3 == 0)
		}
	}
	close(start)
	wg.Wait()

	// 每个工作流 6 个任务，各执行一次。
	if got := atomic.LoadInt64(&totalRuns); got != int64(wfCount*6) {
		t.Fatalf("total task runs = %d, want %d (no missing, no duplicate)", got, wfCount*6)
	}
	for i := 0; i < wfCount; i++ {
		id := fmt.Sprintf("stress-%d", i)
		for _, tid := range []string{"a", "b", "c", "d", "e", "f"} {
			requireStatus(t, e, id, tid, StatusSucceeded)
		}
	}
}
