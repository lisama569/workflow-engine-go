package workflow

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// flakyRecord 是可计数的脆弱任务：前 failTimes 次失败，成功后保持成功。
// doneOnce 用于断言"已完成的任务在续跑后绝不重复执行"。
func resumeTasks(gate chan struct{}, counters map[string]*int, mu *sync.Mutex, done map[string]int) []*Task {
	mk := func(id string, failFirst int, deps ...string) *Task {
		return &Task{
			ID:    id,
			Deps:  deps,
			Retry: RetryPolicy{MaxAttempts: 5, InitialBackoff: time.Millisecond, Multiplier: 1},
			Run: func(ctx context.Context, attempt int) error {
				mu.Lock()
				*counters[id]++
				n := *counters[id]
				completed := done[id]
				mu.Unlock()
				if completed > 0 {
					return fmt.Errorf("task %s executed twice after completion", id)
				}
				if n <= failFirst {
					return fmt.Errorf("planned failure #%d of %s", n, id)
				}
				// 对于要被中断的任务，阻塞在 gate 上。
				if id == "long" {
					select {
					case <-gate:
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				mu.Lock()
				done[id] = attempt
				mu.Unlock()
				return nil
			},
		}
	}
	return []*Task{
		mk("a", 0),
		mk("b", 1, "a"), // b 先失败 1 次再成功
		mk("long", 0, "b"),
		mk("c", 0, "b"),
		mk("d", 0, "c", "long"),
	}
}

func newResumeState() (chan struct{}, map[string]*int, *sync.Mutex, map[string]int) {
	gate := make(chan struct{})
	mu := &sync.Mutex{}
	counters := map[string]*int{}
	for _, id := range []string{"a", "b", "long", "c", "d"} {
		var n int
		counters[id] = &n
	}
	done := map[string]int{}
	return gate, counters, mu, done
}

// TestResume_AfterInterruption_InProcess 模拟进程内中断：Trigger 被取消，
// long 正在执行被硬中断；Resume 后从中断处继续，终态唯一。
func TestResume_AfterInterruption_InProcess(t *testing.T) {
	e := newTestEngine(t)
	gate, counters, mu, done := newResumeState()
	if err := e.Submit("wf-resume", resumeTasks(gate, counters, mu, done)); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	// 等待 long 进入执行后取消：a 成功、b 重试后成功、long 在飞、c 可能在飞。
	longRunning := make(chan struct{})
	go func() {
		// 轮询直到 long 状态为 RUNNING
		for {
			st, _, _ := e.statusOfTask("wf-resume", "long")
			if st == StatusRunning {
				close(longRunning)
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()
	go func() {
		<-longRunning
		cancel()
	}()
	res, err := e.Trigger(ctx, "wf-resume", 1)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("interrupted trigger err = %v, want context.Canceled", err)
	}
	if res.Status != StatusRunning {
		t.Fatalf("after interrupt status = %s, want RUNNING (non-terminal)", res.Status)
	}

	// 中断后断言：a/b 已成功且不重跑；d 尚未运行。
	requireStatus(t, e, "wf-resume", "a", StatusSucceeded)
	requireStatus(t, e, "wf-resume", "b", StatusSucceeded)
	requireAttempts(t, e, "wf-resume", "b", 2)
	requireStatus(t, e, "wf-resume", "d", StatusPending)

	// 打开闸门并续跑。
	close(gate)
	res2, err2 := e.Resume(context.Background(), "wf-resume")
	if err2 != nil {
		t.Fatalf("resume err = %v", err2)
	}
	if res2.Status != StatusSucceeded {
		t.Fatalf("resume result = %s, want SUCCEEDED", res2.Status)
	}
	for _, id := range []string{"a", "b", "long", "c", "d"} {
		requireStatus(t, e, "wf-resume", id, StatusSucceeded)
	}
	mu.Lock()
	defer mu.Unlock()
	if *counters["a"] != 1 {
		t.Fatalf("a executed %d times across interrupt+resume, want 1", *counters["a"])
	}
	if *counters["b"] != 2 {
		t.Fatalf("b executed %d times, want 2 (1 failure + 1 success), attempts must not roll back", *counters["b"])
	}
	if *counters["d"] != 1 {
		t.Fatalf("d executed %d times, want exactly 1 (never ran before resume)", *counters["d"])
	}
	if done["a"] != 1 || done["b"] != 2 {
		t.Fatalf("completion attempts wrong: a=%d b=%d", done["a"], done["b"])
	}
	t.Logf("in-process resume converged; execution counts: a=%d b=%d long=%d c=%d d=%d",
		*counters["a"], *counters["b"], *counters["long"], *counters["c"], *counters["d"])
}

// TestResume_AcrossRestartWithSnapshot 模拟"进程重启"：引擎 A 中断，
// 序列化快照；在全新引擎 B 上用相同任务定义恢复并续跑。
func TestResume_AcrossRestartWithSnapshot(t *testing.T) {
	engineA := newTestEngine(t)
	gate, counters, mu, done := newResumeState()
	tasks := resumeTasks(gate, counters, mu, done)
	if err := engineA.Submit("wf-restart", tasks); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	go func() {
		for {
			st, _, _ := engineA.statusOfTask("wf-restart", "long")
			if st == StatusRunning {
				close(started)
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()
	go func() { <-started; cancel() }()
	if _, err := engineA.Trigger(ctx, "wf-restart", 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("want canceled, got %v", err)
	}

	snap, err := engineA.Snapshot("wf-restart")
	if err != nil {
		t.Fatal(err)
	}
	data, err := snap.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	restored, err := UnmarshalSnapshot(data)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("snapshot after interrupt: %s", string(data))

	// 全新引擎：模拟重启，只重新登记任务定义，状态全部来自快照。
	engineB := newTestEngine(t)
	if err := engineB.Restore("wf-restart", resumeTasks(gate, counters, mu, done), restored); err != nil {
		t.Fatal(err)
	}
	close(gate)
	res, err := engineB.Resume(context.Background(), "wf-restart")
	if err != nil {
		t.Fatalf("resume after restore: %v", err)
	}
	if res.Status != StatusSucceeded {
		t.Fatalf("status after restored resume = %s, want SUCCEEDED", res.Status)
	}
	mu.Lock()
	defer mu.Unlock()
	if *counters["a"] != 1 {
		t.Fatalf("a ran %d times across restart, want 1 (succeeded tasks not re-executed)", *counters["a"])
	}
	if *counters["b"] != 2 {
		t.Fatalf("b ran %d times, want 2 (attempts preserved by snapshot)", *counters["b"])
	}
}
