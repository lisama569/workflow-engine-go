package workflow

import (
	"context"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// workerResult 工作协程回报。
type workerResult struct {
	taskID string
	err    error
}

// retryTimer 退避定时器。
type retryTimer struct {
	timer *time.Timer
	task  string
}

// run 是唯一的调度协调循环：
//   - 所有任务/工作流状态迁移只发生在本协程，并同步写穿 Store；
//   - 任务函数在独立工作协程执行，仅通过 resultCh 回报成功/失败；
//   - 因此状态机推进、依赖释放、尝试计数都不存在数据竞争，
//     对外观测到的迁移序列严格自洽。
func (e *Engine) run(ctx context.Context, id string, resume bool) (final *WorkflowState, runErr error) {
	wf, specs, topo, err := e.store.Load(ctx, id)
	if err != nil {
		return nil, err
	}

	// 本地权威副本（协调器独占），迁移后写穿 Store。
	tasks := make(map[string]*TaskState, len(wf.Tasks))
	for k, v := range wf.Tasks {
		cp := *v
		tasks[k] = &cp
	}

	// reclaimAsRetry 标记被中断回收的任务：下一次执行沿用当前尝试计数，
	// 表示“同一次尝试的重新执行”，避免中断+续跑多计一次重试。
	reclaimAsRetry := make(map[string]bool)

	if resume {
		e.emit(id, "", string(wf.Status), "", ReasonResume, "scanning interrupted state")
	}

	// 回收所有非终态任务：RUNNING/RETRYING 回收到可再次执行的 READY，
	// READY 保持 READY（中断时已规范化），PENDING 原封不动等待依赖判定。
	// 终态任务（SUCCEEDED/FAILED/SKIPPED）原样保留，绝不重复执行。
	for _, tid := range topo {
		t := tasks[tid]
		if t.Status.IsTerminal() || t.Status == StatusPending || t.Status == StatusReady {
			if t.Status == StatusReady {
				reclaimAsRetry[tid] = true
			}
			continue
		}
		from := t.Status
		t.Status = StatusReady
		t.UpdatedAt = nowHook()
		reclaimAsRetry[tid] = true
		e.persistTask(ctx, id, t)
		reason := ReasonResume
		if !resume {
			reason = ReasonScheduled
		}
		e.emit(id, tid, string(from), string(StatusReady), reason,
			"reclaimed non-terminal task; attempt count preserved")
	}

	if !wf.Status.IsTerminal() {
		e.transitionWorkflow(ctx, id, wf, WfRunning, resume)
	}

	resultCh := make(chan workerResult)
	retryChs := make(map[string]<-chan time.Time)
	timers := make(map[string]*time.Timer)
	running := 0

	// stopTimers 在中断时回收所有退避定时器。
	stopTimers := func() {
		for _, tm := range timers {
			tm.Stop()
		}
	}

	// persistTask 将单个任务快照写穿 Store。
	persist := func(t *TaskState) { e.persistTask(ctx, id, t) }

	for {
		// 1) 终态级联与就绪提升：沿拓扑序单遍扫描。
		//    依赖 FAILED/SKIPPED 的任务标记 SKIPPED；依赖全部 SUCCEEDED
		//    且仍 PENDING 的任务提升为 READY。已在执行链路上的状态不动。
		for _, tid := range topo {
			t := tasks[tid]
			if t.Status != StatusPending {
				continue
			}
			depFailed := false
			allDone := true
			for _, dep := range specs[tid].Deps {
				switch tasks[dep].Status {
				case StatusFailed, StatusSkipped:
					depFailed = true
				case StatusSucceeded:
				default:
					allDone = false
				}
			}
			if depFailed {
				e.transitionTask(ctx, id, t, StatusSkipped, ReasonDepFailed,
					"upstream dependency failed; skipped", persist)
				continue
			}
			if allDone {
				e.transitionTask(ctx, id, t, StatusReady, ReasonDepsMet,
					"all dependencies succeeded", persist)
			}
		}

		// 2) 派发：有空闲并发槽时，按拓扑序取第一个 READY 任务执行。
		for running < wf.Concurrency {
			pick := ""
			for _, tid := range topo {
				if tasks[tid].Status == StatusReady {
					pick = tid
					break
				}
			}
			if pick == "" {
				break
			}
			t := tasks[pick]
			sp := specs[pick]

			if reclaimAsRetry[pick] {
				// 被回收任务：沿用已有尝试次数重新执行（同一次尝试）。
				delete(reclaimAsRetry, pick)
				if t.Attempt == 0 {
					t.Attempt = 1
				}
				e.transitionTask(ctx, id, t, StatusRunning, ReasonScheduled,
					"resume execution with preserved attempt="+strconv.Itoa(t.Attempt), persist)
			} else {
				// 常规派发（首次或重试后的新一轮）。
				if t.Attempt == 0 {
					t.Attempt = 1
				}
				e.transitionTask(ctx, id, t, StatusRunning, ReasonScheduled,
					"slot acquired; attempt="+strconv.Itoa(t.Attempt), persist)
			}
			if t.StartedAt.IsZero() {
				t.StartedAt = nowHook()
				persist(t)
			}
			running++
			go func(tid string, fn TaskFunc) {
				err := fn(ctx)
				select {
				case resultCh <- workerResult{taskID: tid, err: err}:
				case <-ctx.Done():
					// 协调器已放弃本次执行（中断），结果不再被消费。
				}
			}(pick, sp.Fn)
		}

		// 3) 完成判定：无运行中任务、无待重试任务、无 READY 任务时结束。
		if running == 0 && len(retryChs) == 0 {
			anyReady := false
			for _, t := range tasks {
				if t.Status == StatusReady {
					anyReady = true
					break
				}
			}
			if !anyReady {
				stopTimers()
				return e.finish(ctx, id, wf, tasks, topo)
			}
		}

		// 4) 等待事件：工作协程回报、退避到期、外部中断。
		chs := make([]<-chan time.Time, 0, len(retryChs))
		taskOfCh := make(map[<-chan time.Time]string, len(retryChs))
		for tid, ch := range retryChs {
			chs = append(chs, ch)
			taskOfCh[ch] = tid
		}

		var fired string
		var res *workerResult
		var aborted bool

		if len(chs) == 0 {
			select {
			case r := <-resultCh:
				res = &r
			case <-ctx.Done():
				aborted = true
			}
		} else {
			// reflect.Select 动态监听 resultCh + 所有退避通道 + ctx.Done。
			cases := make([]reflect.SelectCase, 0, len(chs)+2)
			cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(resultCh)})
			for _, ch := range chs {
				cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ch)})
			}
			cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ctx.Done())})
			idx, v, ok := reflect.Select(cases)
			switch {
			case idx == 0:
				if ok {
					r := v.Interface().(workerResult)
					res = &r
				}
			case idx == len(cases)-1:
				aborted = true
			default:
				fired = taskOfCh[chs[idx-1]]
			}
		}

		if aborted {
			stopTimers()
			return e.abort(id, wf, tasks, topo)
		}

		if res != nil {
			running--
			t := tasks[res.taskID]
			sp := specs[res.taskID]
			if res.err == nil {
				t.LastError = ""
				t.EndedAt = nowHook()
				e.transitionTask(ctx, id, t, StatusSucceeded, ReasonSucceeded,
					"attempt "+strconv.Itoa(t.Attempt)+" succeeded", persist)
				continue
			}
			t.LastError = res.err.Error()
			if t.Attempt <= sp.Retry.MaxRetries {
				// attempt 语义：1=首次执行；当 attempt<=MaxRetries 时还可再试。
				wait := time.Duration(0)
				if sp.Retry.Backoff != nil {
					wait = sp.Retry.Backoff.Next(t.Attempt)
				}
				e.transitionTask(ctx, id, t, StatusRetrying, ReasonAttemptFailed,
					"attempt "+strconv.Itoa(t.Attempt)+" failed: "+res.err.Error()+
						"; retry scheduled in "+wait.String(), persist)
				tm := time.NewTimer(wait)
				timers[t.ID] = tm
				retryChs[t.ID] = tm.C
			} else {
				t.EndedAt = nowHook()
				e.transitionTask(ctx, id, t, StatusFailed, ReasonRetriesExhausted,
					"attempt "+strconv.Itoa(t.Attempt)+" failed: "+res.err.Error()+
						"; no retries left", persist)
			}
			continue
		}

		if fired != "" {
			delete(retryChs, fired)
			delete(timers, fired)
			t := tasks[fired]
			t.Attempt++
			e.transitionTask(ctx, id, t, StatusReady, ReasonRetryBackoff,
				"backoff elapsed; attempt="+strconv.Itoa(t.Attempt)+" ready", persist)
		}
	}
}

// finish 将工作流推进到终态：任一任务 FAILED/SKIPPED 则 FAILED，否则 SUCCEEDED。
func (e *Engine) finish(ctx context.Context, id string, wf *WorkflowState,
	tasks map[string]*TaskState, topo []string) (*WorkflowState, error) {
	target := WfSucceeded
	for _, tid := range topo {
		if tasks[tid].Status == StatusFailed || tasks[tid].Status == StatusSkipped {
			target = WfFailed
			break
		}
	}
	e.transitionWorkflow(ctx, id, wf, target, false)
	final, _, _, err := e.store.Load(ctx, id)
	return final, err
}

// abort 中断收尾：保留终态任务；RUNNING/RETRYING 回收到 READY（可续跑），
// READY/PENDING 回落到 PENDING。尝试计数保留，状态不回退、终态不丢失。
func (e *Engine) abort(id string, wf *WorkflowState,
	tasks map[string]*TaskState, topo []string) (*WorkflowState, error) {
	ctx := context.Background()
	for _, tid := range topo {
		t := tasks[tid]
		if t.Status.IsTerminal() {
			continue
		}
		from := t.Status
		switch from {
		case StatusRunning, StatusRetrying:
			t.Status = StatusReady
		default:
			t.Status = StatusPending
		}
		t.UpdatedAt = nowHook()
		e.persistTask(ctx, id, t)
		e.emit(id, tid, string(from), string(t.Status), ReasonResume,
			"interrupted; state retained for resume")
	}
	final, _, _, err := e.store.Load(ctx, id)
	return final, err
}

// transitionTask 在协调器内执行一次任务状态迁移并写穿 Store、发出事件。
func (e *Engine) transitionTask(ctx context.Context, wfID string, t *TaskState,
	to TaskStatus, reason EventReason, detail string, persist func(*TaskState)) {
	from := t.Status
	t.Status = to
	t.UpdatedAt = nowHook()
	persist(t)
	e.emit(wfID, t.ID, string(from), string(to), reason, detail)
}

// transitionWorkflow 迁移工作流聚合状态并写穿 Store。
func (e *Engine) transitionWorkflow(ctx context.Context, wfID string, wf *WorkflowState,
	to WorkflowStatus, resume bool) {
	from := wf.Status
	wf.Status = to
	wf.UpdatedAt = nowHook()
	_ = e.store.UpdateWorkflow(ctx, wfID, func(w *WorkflowState) bool {
		w.Status = to
		w.UpdatedAt = nowHook()
		return true
	})
	reason := ReasonScheduled
	if to.IsTerminal() {
		reason = ReasonWfDone
	} else if resume {
		reason = ReasonResume
	}
	e.emit(wfID, "", string(from), string(to), reason, "workflow aggregate transition")
}

// persistTask 用完整快照覆盖 Store 中的任务状态（协调器独占，无竞争）。
func (e *Engine) persistTask(ctx context.Context, wfID string, t *TaskState) {
	snap := *t
	_ = e.store.UpdateTask(ctx, wfID, t.ID, func(cur *TaskState) bool {
		*cur = snap
		return true
	})
}

func fmtSlice(xs []string) string { return strings.Join(xs, ",") }
