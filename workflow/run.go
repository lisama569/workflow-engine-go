package workflow

import (
	"context"
	"errors"
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

	// 子 ctx：预算耗尽收尾或协调循环退出时统一取消，
	// 让在跑任务的 Fn 能感知取消、尽快退出。
	ctx, stopRun := context.WithCancel(ctx)
	defer stopRun()

	// 本地权威副本（协调器独占），迁移后写穿 Store。
	tasks := make(map[string]*TaskState, len(wf.Tasks))
	for k, v := range wf.Tasks {
		cp := *v
		tasks[k] = &cp
	}

	// 总预算：Deadline 在第一次触发时按“首次触发时刻 + Budget”确定并
	// 持久化；续跑沿用同一 Deadline，保证预算语义前后一致、可预期。
	if wf.Budget > 0 && wf.Deadline.IsZero() {
		wf.Deadline = nowHook().Add(wf.Budget)
		deadline := wf.Deadline
		_ = e.store.UpdateWorkflow(ctx, id, func(w *WorkflowState) bool {
			if w.Deadline.IsZero() {
				w.Deadline = deadline
			}
			return true
		})
		e.emit(id, "", "", "", ReasonScheduled,
			"budget "+wf.Budget.String()+" armed; deadline="+wf.Deadline.Format(time.RFC3339Nano))
	}
	budgetExpired := func() bool {
		return !wf.Deadline.IsZero() && !nowHook().Before(wf.Deadline)
	}
	// 触发/续跑时预算已耗尽：不派发任何任务，直接收敛到终态。
	if budgetExpired() {
		return e.expireBudget(id, wf, tasks, topo)
	}

	// reclaimAsRetry 标记被中断回收的任务：下一次执行沿用当前尝试计数，
	// 表示“同一次尝试的重新执行”，避免中断+续跑多计一次重试。
	reclaimAsRetry := make(map[string]bool)

	if resume {
		e.emit(id, "", string(wf.Status), "", ReasonResume, "scanning interrupted state")
	}

	// 回收所有非终态任务：RUNNING/RETRYING 回收到可再次执行的 READY，
	// READY 保持 READY（中断时已规范化），PENDING 原封不动等待依赖判定。
	// 终态任务（SUCCEEDED/FAILED/SKIPPED/CANCELLED）原样保留，绝不重复执行。
	// RETRYING 回收时 Attempt+1：失败尝试已计数，中断视为退避结束，
	// 待发起的重试计为下一次尝试，避免续跑少计一次尝试。
	for _, tid := range topo {
		t := tasks[tid]
		if t.Status.IsTerminal() || t.Status == StatusPending || t.Status == StatusReady {
			if t.Status == StatusReady {
				reclaimAsRetry[tid] = true
			}
			continue
		}
		from := t.Status
		if from == StatusRetrying {
			t.Attempt++
		}
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

	// 单次尝试超时：不为每次尝试额外建 time.Timer，直接复用
	// context.WithTimeout 的 Done 通道；结果到达或超时判定后 cancel，
	// 定时器与协程随本次运行结束（defer stopRun）全部回收。
	attemptCtxs := make(map[string]context.Context)
	attemptCancels := make(map[string]context.CancelFunc)

	// 总预算定时器：整个运行只建一个，到期唤醒协调循环做预算收尾。
	var budgetTimer *time.Timer
	var budgetCh <-chan time.Time
	if !wf.Deadline.IsZero() {
		budgetTimer = time.NewTimer(wf.Deadline.Sub(nowHook()))
		budgetCh = budgetTimer.C
		defer budgetTimer.Stop()
	}

	// stopTimers 在中断/收尾时回收所有退避定时器。
	stopTimers := func() {
		for _, tm := range timers {
			tm.Stop()
		}
	}

	// persistTask 将单个任务快照写穿 Store。
	persist := func(t *TaskState) { e.persistTask(ctx, id, t) }

	for {
		// 0) 预算判定优先于一切调度动作：到期即收尾，不再派发新任务。
		if budgetExpired() {
			stopTimers()
			return e.expireBudget(id, wf, tasks, topo)
		}
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

		// 2) 派发：有空闲并发槽且预算未耗尽时，按拓扑序取第一个 READY 任务执行。
		for running < wf.Concurrency && !budgetExpired() {
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
			// 单次尝试超时：为本次尝试派生带截止时间的 ctx，
			// 任务函数能感知取消并尽快退出。
			taskCtx := ctx
			if sp.Timeout > 0 {
				var cancel context.CancelFunc
				taskCtx, cancel = context.WithTimeout(ctx, sp.Timeout)
				attemptCtxs[pick] = taskCtx
				attemptCancels[pick] = cancel
			}
			go func(tid string, fn TaskFunc, c context.Context) {
				err := fn(c)
				select {
				case resultCh <- workerResult{taskID: tid, err: err}:
				case <-ctx.Done():
					// 协调器已放弃本次执行（中断/预算收尾），结果不再被消费。
				}
			}(pick, sp.Fn, taskCtx)
		}

		// 3) 完成判定：无运行中任务、无待重试任务、无 READY 任务时结束。
		//    预算在本轮迭代中途到期的，优先走预算收尾而非正常终态。
		if running == 0 && len(retryChs) == 0 {
			if budgetExpired() {
				stopTimers()
				return e.expireBudget(id, wf, tasks, topo)
			}
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

		// 4) 等待事件：工作协程回报、退避到期、尝试超时、预算到期、外部中断。
		retryList := make([]string, 0, len(retryChs))
		attemptList := make([]string, 0, len(attemptCtxs))
		cases := make([]reflect.SelectCase, 0, len(retryChs)+len(attemptCtxs)+3)
		cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(resultCh)})
		for tid, ch := range retryChs {
			retryList = append(retryList, tid)
			cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ch)})
		}
		for tid, ac := range attemptCtxs {
			attemptList = append(attemptList, tid)
			cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ac.Done())})
		}
		budgetIdx := -1
		if budgetCh != nil {
			budgetIdx = len(cases)
			cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(budgetCh)})
		}
		cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ctx.Done())})
		abortIdx := len(cases) - 1

		var fired string
		var timedOut string
		var res *workerResult
		var aborted bool

		idx, v, ok := reflect.Select(cases)
		switch {
		case idx == 0:
			if ok {
				r := v.Interface().(workerResult)
				res = &r
			}
		case idx == abortIdx:
			aborted = true
		case idx == budgetIdx:
			// 预算到期：回到循环顶部统一走预算收尾。
		case idx <= len(retryList):
			fired = retryList[idx-1]
		default:
			tid := attemptList[idx-1-len(retryList)]
			if errors.Is(attemptCtxs[tid].Err(), context.DeadlineExceeded) {
				timedOut = tid
			} else {
				// 父 ctx 取消（外部中断）联动，按中断处理。
				aborted = true
			}
		}

		if aborted {
			stopTimers()
			return e.abort(id, wf, tasks, topo)
		}

		if res != nil {
			t := tasks[res.taskID]
			if t.Status != StatusRunning {
				// 过期回报：该尝试已被超时/预算判定，结果丢弃，
				// 不再重复计数或迁移状态。
				continue
			}
			running--
			if cancel, ok := attemptCancels[res.taskID]; ok {
				cancel() // 释放本次尝试的超时定时器
				delete(attemptCancels, res.taskID)
				delete(attemptCtxs, res.taskID)
			}
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

		if timedOut != "" {
			// 单次尝试超时：按失败处理并走原重试/退避；事件与 LastError
			// 明确标记为超时，与普通业务失败区分。任务函数的 ctx 已被
			// WithTimeout 取消，可感知退出；其迟到回报会被上方过期防护丢弃。
			running--
			if cancel, ok := attemptCancels[timedOut]; ok {
				cancel()
				delete(attemptCancels, timedOut)
				delete(attemptCtxs, timedOut)
			}
			t := tasks[timedOut]
			sp := specs[timedOut]
			t.LastError = ErrTaskTimeout.Error() + " after " + sp.Timeout.String()
			if t.Attempt <= sp.Retry.MaxRetries {
				wait := time.Duration(0)
				if sp.Retry.Backoff != nil {
					wait = sp.Retry.Backoff.Next(t.Attempt)
				}
				e.transitionTask(ctx, id, t, StatusRetrying, ReasonAttemptTimeout,
					"attempt "+strconv.Itoa(t.Attempt)+" timed out after "+sp.Timeout.String()+
						"; retry scheduled in "+wait.String(), persist)
				tm := time.NewTimer(wait)
				timers[t.ID] = tm
				retryChs[t.ID] = tm.C
			} else {
				t.EndedAt = nowHook()
				e.transitionTask(ctx, id, t, StatusFailed, ReasonRetriesExhausted,
					"attempt "+strconv.Itoa(t.Attempt)+" timed out after "+sp.Timeout.String()+
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

// expireBudget 预算耗尽收尾：不再派发新任务；未完成的任务（含 RUNNING/
// RETRYING）统一收敛到终态 CANCELLED，已完成任务的终态原样保留；
// 工作流落到 DEADLINE_EXCEEDED，与普通失败 FAILED 明确区分。
// 选择“立即取消在跑任务”而非“自然收尾”：预算的语义就是到点收场，
// 等待自然收尾可能被卡住的任务无限拖延（正是要解决的问题）；
// 在跑任务的 ctx 随协调循环退出被统一取消，可感知退出，结论可复现。
func (e *Engine) expireBudget(id string, wf *WorkflowState,
	tasks map[string]*TaskState, topo []string) (*WorkflowState, error) {
	ctx := context.Background()
	for _, tid := range topo {
		t := tasks[tid]
		if t.Status.IsTerminal() {
			continue
		}
		from := t.Status
		t.Status = StatusCancelled
		t.UpdatedAt = nowHook()
		if t.EndedAt.IsZero() && (from == StatusRunning || from == StatusRetrying) {
			t.EndedAt = t.UpdatedAt
		}
		e.persistTask(ctx, id, t)
		e.emit(id, tid, string(from), string(StatusCancelled), ReasonBudgetExhausted,
			"workflow budget exhausted; task cancelled")
	}
	from := wf.Status
	wf.Status = WfDeadlineExceeded
	wf.UpdatedAt = nowHook()
	_ = e.store.UpdateWorkflow(ctx, id, func(w *WorkflowState) bool {
		w.Status = WfDeadlineExceeded
		w.UpdatedAt = nowHook()
		return true
	})
	e.emit(id, "", string(from), string(WfDeadlineExceeded), ReasonBudgetExhausted,
		"workflow budget exhausted; no new tasks dispatched")
	final, _, _, err := e.store.Load(ctx, id)
	return final, err
}

// finish 将工作流推进到终态：任一任务 FAILED/SKIPPED/CANCELLED 则 FAILED，
// 否则 SUCCEEDED。（CANCELLED 正常只会经 expireBudget 落 DEADLINE_EXCEEDED，
// 此处防御性兜底。）
func (e *Engine) finish(ctx context.Context, id string, wf *WorkflowState,
	tasks map[string]*TaskState, topo []string) (*WorkflowState, error) {
	target := WfSucceeded
	for _, tid := range topo {
		s := tasks[tid].Status
		if s == StatusFailed || s == StatusSkipped || s == StatusCancelled {
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
			if from == StatusRetrying {
				// 退避中的任务：失败尝试已计数，中断视为退避结束，
				// 待发起的重试计为下一次尝试，续跑不会少计。
				t.Attempt++
			}
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
