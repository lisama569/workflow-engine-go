package workflow_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	wf "github.com/lisama569/workflow-engine-go/workflow"
)

func Example() {
	engine := wf.NewEngine(wf.WithEventSink(func(ev wf.Event) {
		if ev.TaskID != "" {
			fmt.Printf("  %s: %s -> %s (attempt %d)\n", ev.TaskID, ev.From, ev.To, ev.Attempt)
		}
	}))

	tasks := []*wf.Task{
		{ID: "fetch", Run: func(context.Context, int) error { fmt.Println("run fetch"); return nil }},
		{ID: "parse", Deps: []string{"fetch"},
			Retry: wf.RetryPolicy{MaxAttempts: 3, InitialBackoff: time.Millisecond, Multiplier: 2},
			Run: func(_ context.Context, attempt int) error {
				if attempt < 2 {
					return errors.New("transient parse error")
				}
				fmt.Println("run parse")
				return nil
			}},
		{ID: "load", Deps: []string{"parse"}, Run: func(context.Context, int) error { fmt.Println("run load"); return nil }},
	}

	if err := engine.Submit("etl", tasks); err != nil {
		fmt.Println("submit:", err)
		return
	}
	res, err := engine.Trigger(context.Background(), "etl", 2)
	if err != nil {
		fmt.Println("trigger:", err)
		return
	}
	fmt.Println("workflow:", res.Status)

	// 重复触发得到唯一终态，不会重跑任务。
	_, err = engine.Trigger(context.Background(), "etl", 2)
	fmt.Println("re-trigger is converged:", errors.Is(err, wf.ErrWorkflowConverged))
	// Output:
	//   fetch: PENDING -> READY (attempt 0)
	//   fetch: READY -> RUNNING (attempt 1)
	// run fetch
	//   fetch: RUNNING -> SUCCEEDED (attempt 1)
	//   parse: PENDING -> READY (attempt 0)
	//   parse: READY -> RUNNING (attempt 1)
	//   parse: RUNNING -> READY (attempt 1)
	//   parse: READY -> RUNNING (attempt 2)
	// run parse
	//   parse: RUNNING -> SUCCEEDED (attempt 2)
	//   load: PENDING -> READY (attempt 0)
	//   load: READY -> RUNNING (attempt 1)
	// run load
	//   load: RUNNING -> SUCCEEDED (attempt 1)
	// workflow: SUCCEEDED
	// re-trigger is converged: true
}
