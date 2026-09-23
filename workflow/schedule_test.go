package workflow

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestDAGSchedule_RespectsDependencies 验证只有依赖全部成功后任务才运行，
// 且最终全部成功、事件序列自洽。
func TestDAGSchedule_RespectsDependencies(t *testing.T) {
	e := newTestEngine(t)
	var mu sync.Mutex
	finished := map[string]bool{}
	task := func(id string, deps ...string) *Task {
		return &Task{ID: id, Deps: deps, Run: func(_ context.Context, attempt int) error {
			mu.Lock()
			defer mu.Unlock()
			for _, d := range deps {
				if !finished[d] {
					t.Errorf("task %s started before dependency %s finished", id, d)
				}
			}
			finished[id] = true
			return nil
		}}
	}
	tasks := []*Task{
		task("a"),
		task("b", "a"),
		task("c", "a"),
		task("d", "b", "c"),
	}
	if err := e.Submit("dag", tasks); err != nil {
		t.Fatal(err)
	}
	res, err := e.Trigger(context.Background(), "dag", 0)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != StatusSucceeded {
		t.Fatalf("result status = %s, want SUCCEEDED", res.Status)
	}
	for _, id := range []string{"a", "b", "c", "d"} {
		requireStatus(t, e, "dag", id, StatusSucceeded)
		requireAttempts(t, e, "dag", id, 1)
	}
	// 每个任务都必须恰好执行一次。
	if len(finished) != 4 {
		t.Fatalf("finished set = %v, want 4 distinct tasks", finished)
	}
	// 事件序列自洽性：每个任务必须先 READY 再 RUNNING 再 SUCCEEDED。
	seq := eventSequenceFor(e, "dag")
	t.Logf("event sequence: %s", sequenceString(seq))
	for _, id := range []string{"a", "b", "c", "d"} {
		phases := 0
		for _, s := range seq {
			switch {
			case phases == 0 && strings.HasPrefix(s, id+":") && strings.Contains(s, "->READY"):
				phases = 1
			case phases == 1 && strings.HasPrefix(s, id+":") && strings.Contains(s, "->RUNNING"):
				phases = 2
			case phases == 2 && strings.HasPrefix(s, id+":") && strings.Contains(s, "->SUCCEEDED"):
				phases = 3
			}
		}
		if phases != 3 {
			t.Fatalf("task %s did not follow READY->RUNNING->SUCCEEDED in order (phase=%d)", id, phases)
		}
	}
}

// TestDAGSchedule_ConcurrencyLimit 验证并发上限：在任意时刻在飞任务数不超过上限。
func TestDAGSchedule_ConcurrencyLimit(t *testing.T) {
	e := newTestEngine(t)
	var mu sync.Mutex
	inflight, maxObserved := 0, 0
	const limit = 2
	run := func(_ context.Context, _ int) error {
		mu.Lock()
		inflight++
		if inflight > maxObserved {
			maxObserved = inflight
		}
		if inflight > limit {
			mu.Unlock()
			t.Errorf("inflight %d exceeded limit %d", inflight, limit)
			return nil
		}
		mu.Unlock()
		time.Sleep(20 * time.Millisecond)
		mu.Lock()
		inflight--
		mu.Unlock()
		return nil
	}
	tasks := []*Task{
		{ID: "a", Run: run}, {ID: "b", Run: run},
		{ID: "c", Run: run}, {ID: "d", Deps: []string{"a"}, Run: run},
	}
	if err := e.Submit("limited", tasks); err != nil {
		t.Fatal(err)
	}
	res, err := e.Trigger(context.Background(), "limited", limit)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != StatusSucceeded {
		t.Fatalf("status = %s", res.Status)
	}
	if maxObserved > limit {
		t.Fatalf("max observed inflight = %d > limit %d", maxObserved, limit)
	}
	if maxObserved < limit {
		t.Fatalf("limit never reached: max observed inflight = %d", maxObserved)
	}
	t.Logf("concurrency limit respected: max observed inflight = %d", maxObserved)
}
