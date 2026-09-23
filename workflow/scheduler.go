package workflow

import (
	"context"
	"strings"
	"time"
)

// attemptResult 是某次任务尝试异步执行完成后的汇报。
type attemptResult struct {
	taskID  string
	attempt int
	err     error
}

// backoffEntry 描述一次"等待退避后重试"的挂起任务。
type backoffEntry struct {
	taskID  string
	attempt int // 刚失败的是第几次尝试
	readyAt time.Time
}

// run 是唯一的调度执行点：所有状态迁移都在此调用栈串行发生，
// 任务执行体本身在独立 goroutine 中并发运行，结果通过 channel 汇报。
// 因此状态迁移与依赖释放不存在数据竞争。
func (e *Engine) run(ctx context.Context, wf *wfState, concurrency int) error {
	wf.mu.Lock()
	wf.status = StatusRunning
	wf.mu.Unlock()
	e.log.Append(wf.id, "", "", StatusRunning, 0,
		"execution started: scheduler admitted with concurrency limit="+describeLimit(concurrency))

	ready := e.buildReadyQueue(wf)
	inflight := make(map[string]int) // taskID -> 正在执行的尝试序号
	var backoffs []backoffEntry
	results := make(chan attemptResult, 16)
	doneCh := ctx.Done()

	// pickReady 在并发槽位允许时取下一个就绪任务。
	pickReady := func() (string, bool) {
		for len(ready) > 0 {
			id := ready[0]
			ready = ready[1:]
			if concurrency > 0 && len(inflight) >= concurrency {
				// 放回队首：本轮槽位已满。
				ready = append([]string{id}, ready...)
				return "", false
			}
			if _, busy := inflight[id]; busy {
				continue
			}
			return id, true
		}
		return "", false
	}

	// launch 启动一个任务尝试。必须在不持有 wf.mu 的情况下运行任务体。
	launch := func(id string) {
		wf.mu.Lock()
		ts := wf.tasks[id]
		attempt := ts.attempts + 1
		from := ts.status
		ts.status = StatusRunning
		runFn := ts.def.Run
		deps := append([]string(nil), ts.deps...)
		wf.mu.Unlock()

		if runFn == nil {
			runFn = func(context.Context, int) error { return nil }
		}
		reason := "concurrency slot acquired; dependencies " + joinIDs(deps) + " all succeeded"
		if attempt > 1 {
			reason = "backoff elapsed; retrying attempt " + itoa(attempt)
		}
		e.log.Append(wf.id, id, from, StatusRunning, attempt, reason)
		inflight[id] = attempt
		go func() {
			err := runFn(ctx, attempt)
			results <- attemptResult{taskID: id, attempt: attempt, err: err}
		}()
	}

	// handleSuccess 处理尝试成功：唯一允许迁移到 SUCCEEDED 的路径。
	handleSuccess := func(taskID string, attempt int) {
		wf.mu.Lock()
		ts := wf.tasks[taskID]
		ts.attempts = attempt
		from := ts.status
		ts.status = StatusSucceeded
		ts.lastErr = ""
		wf.mu.Unlock()
		delete(inflight, taskID)
		e.log.Append(wf.id, taskID, from, StatusSucceeded, attempt,
			"attempt "+itoa(attempt)+" succeeded; releasing dependents")
		e.reconsider(wf, taskID, &ready, &backoffs)
	}

	// handleFailure 处理尝试失败：退避重试或进入终态 FAILED。
	handleFailure := func(taskID string, attempt int, execErr error) {
		wf.mu.Lock()
		ts := wf.tasks[taskID]
		from := ts.status
		maxAttempts := ts.def.Retry.NormalizedMaxAttempts()
		ts.attempts = attempt
		if execErr != nil {
			ts.lastErr = execErr.Error()
		}
		wf.mu.Unlock()
		delete(inflight, taskID)

		if attempt < maxAttempts {
			wf.mu.Lock()
			ts.status = StatusReady
			wf.mu.Unlock()
			d := ts.def.Retry.backoffForAttempt(attempt)
			e.log.Append(wf.id, taskID, from, StatusReady, attempt,
				"attempt "+itoa(attempt)+"/"+itoa(maxAttempts)+" failed ("+safeErr(execErr)+
					"); backing off "+d.String()+" before retry; state remains non-terminal")
			backoffs = append(backoffs, backoffEntry{
				taskID: taskID, attempt: attempt, readyAt: time.Now().Add(d),
			})
			return
		}
		wf.mu.Lock()
		ts.status = StatusFailed
		wf.mu.Unlock()
		e.log.Append(wf.id, taskID, from, StatusFailed, attempt,
			"attempt "+itoa(attempt)+"/"+itoa(maxAttempts)+" failed ("+safeErr(execErr)+
				"); retry budget exhausted -> terminal FAILED")
		e.propagateFailure(wf, taskID, &ready, &backoffs)
	}

	for {
		// 1) 到期的退避任务重新入队。
		remaining := backoffs[:0]
		now := time.Now()
		for _, b := range backoffs {
			if !now.Before(b.readyAt) {
				ready = append(ready, b.taskID)
			} else {
				remaining = append(remaining, b)
			}
		}
		backoffs = remaining

		// 2) 启动可运行的就绪任务。
		for {
			id, ok := pickReady()
			if !ok {
				break
			}
			launch(id)
		}

		// 3) 是否收敛？
		if status, done := e.checkConverged(wf, len(inflight), len(backoffs), len(ready)); done {
			e.finish(wf, status)
			return nil
		}

		// 4) 等待：最近的退避定时器、尝试完成、或上下文取消。
		var timerCh <-chan time.Time
		var timer *time.Timer
		if len(backoffs) > 0 {
			next := backoffs[0].readyAt
			for _, b := range backoffs[1:] {
				if b.readyAt.Before(next) {
					next = b.readyAt
				}
			}
			timer = time.NewTimer(time.Until(next))
			timerCh = timer.C
		}
		select {
		case res := <-results:
			if timer != nil {
				timer.Stop()
			}
			if res.err == nil {
				handleSuccess(res.taskID, res.attempt)
			} else {
				handleFailure(res.taskID, res.attempt, res.err)
			}
		case <-timerCh:
			// 退避到期，循环回到入队逻辑。
		case <-doneCh:
			if timer != nil {
				timer.Stop()
			}
			return e.abort(wf, results, inflight)
		}
	}
}

func describeLimit(concurrency int) string {
	if concurrency <= 0 {
		return "unbounded"
	}
	return itoa(concurrency)
}

func joinIDs(ids []string) string {
	if len(ids) == 0 {
		return "[]"
	}
	return "[" + strings.Join(ids, " ") + "]"
}

func safeErr(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// itoa 避免为简单数字转换额外引入 strconv 到多处。
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}
