package workflow

import (
	"context"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// workerResult 工作协程回报。attempt 用于识别迟到结果：
// 超时/预算收口后，被遗弃的尝试若最终返回，其 attempt 与当前在途
// 尝试对不上，结果直接丢弃，状态机不受干扰。
type workerResult struct {
	taskID  string
	attempt int
	err     error
}

// run 是唯一的调度协调循环：
//   - 所有任务/工作流状态迁移只发生在本协程，并同步写穿 Store；
//   - 任务函数在独立工作协程执行，仅通过 resultCh 回报成功/失败；
//   - 任务超时通过每次尝试独立的 context.WithTimeout 实现，任务可感知取消；
//   - 工作流预算通过单一截止定时器实现，到点即收口到终态；
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

	// 预算语义：Deadline 在第一次运行的起点设定并持久化，续跑沿用同一
	// Deadline，即“按第一次触发算起的总时长”。续跑时若预算早已用尽，
	// 不执行任何任务，直接以终态 CANCELLED 收口。
	budgetActive := wf.Budget > 0
	if budgetActive {
		if wf.Deadline.IsZero() {
			wf.Deadline = nowHook().Add(wf.Budget)
			_ = e.store.UpdateWorkflow(ctx, id, func(w *WorkflowState) bool {
				w.Deadline = wf.Deadline
				w.UpdatedAt = nowHook()
				return true
			})
			e.emit(id, "", "", "", ReasonScheduled,
				"budget armed: "+wf.Budget.String()+", deadline="+wf.Deadline.Format(time.RFC3339Nano))
		} else if !nowHook().Before(wf.Deadline) {
			e.emit(id, "", "", "", ReasonBudgetExhausted,
				"budget already exhausted before (re)start; finalizing without dispatch")
			return e.budgetCutoff(id, wf, tasks, topo)
		}
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

	// 在途尝试登记：taskID -> attempt。配合 attemptCancels/timeoutChs
	// 管理每次尝试的取消函数与超时通道，尝试一结束即释放定时器。
	runningTasks := make(map[string]int)
	attemptCancels := make(map[string]context.CancelFunc)
	timeoutChs := make(map[string]<-chan struct{})

	// 预算截止定时器：整个运行期只有一个，退出时回收。
	var budgetTimer *time.Timer
	var budgetCh <-chan time.Time
	if budgetActive {
		budgetTimer = time.NewTimer(wf.Deadline.Sub(nowHook()))
		budgetCh = budgetTimer.C
	}

	// cleanup 在退出协调循环前回收所有退避定时器、尝试定时器与预算定时器，
	// 并取消全部在途尝试，长时间反复运行不会积累定时器与协程。
	cleanup := func() {
		for _, tm := range timers {
			tm.Stop()
		}
		for _, cancel := range attemptCancels {
			cancel()
		}
		if budgetTimer != nil {
			budgetTimer.Stop()
		}
	}

	budgetExpired := func() bool {
		return budgetActive && !nowHook().Before(wf.Deadline)
	}

	// persistTask 将单个任务快照写穿 Store。
	persist := func(t *TaskState) { e.persistTask(ctx, id, t) }

	// failAttempt 把一次失败尝试推进状态机：还有重试额度则 RETRYING 并挂
	// 退避定时器，否则终态 FAILED。reason 区分普通失败与超时失败；
	// errText 写入 LastError 与事件 Detail，超时失败含 ErrTaskTimeout 文本。
	failAttempt := func(t *TaskState, sp TaskSpec, errText string, reason EventReason) {
		t.LastError = errText
		if t.Attempt <= sp.Retry.MaxRetries {
			// attempt 语义：1=首次执行；当 attempt<=MaxRetries 时还可再试。
			wait := time.Duration(0)
			if sp.Retry.Backoff != nil {
				wait = sp.Retry.Backoff.Next(t.Attempt)
			}
			e.transitionTask(ctx, id, t, StatusRetrying, reason,
				"attempt "+strconv.Itoa(t.Attempt)+" failed: "+errText+
					"; retry scheduled in "+wait.String(), persist)
			tm := time.NewTimer(wait)
			timers[t.ID] = tm
			retryChs[t.ID] = tm.C
			return
		}
		t.EndedAt = nowHook()
		e.transitionTask(ctx, id, t, StatusFailed, ReasonRetriesExhausted,
			"attempt "+strconv.Itoa(t.Attempt)+" failed: "+errText+
				"; no retries left", persist)
	}

	for {
		// 0) 预算到点：不再提升/派发任何任务，在途尝试取消，直接收口终态。
		if budgetExpired() {
			cleanup()
			return e.budgetCutoff(id, wf, tasks, topo)
		}

		// 1) 终态级联与就绪提升：沿拓扑序单遍扫描。
		//    依赖 FAILED/SKIPPED/CANCELLED 的任务标记 SKIPPED；依赖全部 SUCCEEDED
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
				case StatusFailed, StatusSkipped, StatusCancelled:
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
		//    每次尝试使用独立 context：声明了 Timeout 的带超时，未声明的
		//    仅随运行取消（行为与旧版一致）。
		for len(runningTasks) < wf.Concurrency {
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

			attempt := t.Attempt
			actx, acancel := context.WithCancel(ctx)
			if sp.Timeout > 0 {
				actx, acancel = context.WithTimeout(ctx, sp.Timeout)
				timeoutChs[pick] = actx.Done()
			}
			attemptCancels[pick] = acancel
			runningTasks[pick] = attempt

			go func(tid string, fn TaskFunc, actx context.Context, att int) {
				err := fn(actx)
				select {
				case resultCh <- workerResult{taskID: tid, attempt: att, err: err}:
				case <-ctx.Done():
					// 协调器已放弃本次执行（中断/收口），结果不再被消费。
				}
			}(pick, sp.Fn, actx, attempt)
		}

		// 3) 完成判定：无运行中任务、无待重试任务、无 READY 任务时结束。
		if len(runningTasks) == 0 && len(retryChs) == 0 {
			anyReady := false
			for _, t := range tasks {
				if t.Status == StatusReady {
					anyReady = true
					break
				}
			}
			if !anyReady {
				cleanup()
				return e.finish(ctx, id, wf, tasks, topo)
			}
		}

		// 4) 等待事件：工作协程回报、退避到期、尝试超时、预算到点、外部中断。
		cases := make([]reflect.SelectCase, 0, len(retryChs)+len(timeoutChs)+3)
		cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(resultCh)})
		retryKeys := make([]string, 0, len(retryChs))
		for tid, ch := range retryChs {
			cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ch)})
			retryKeys = append(retryKeys, tid)
		}
		timeoutKeys := make([]string, 0, len(timeoutChs))
		for tid, ch := range timeoutChs {
			cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ch)})
			timeoutKeys = append(timeoutKeys, tid)
		}
		budgetIdx := -1
		if budgetCh != nil {
			budgetIdx = len(cases)
			cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(budgetCh)})
		}
		doneIdx := len(cases)
		cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ctx.Done())})

		idx, v, ok := reflect.Select(cases)

		var res *workerResult
		var fired, timedOut string
		var aborted, budgetHit bool
		switch {
		case idx == 0:
			if ok {
				r := v.Interface().(workerResult)
				res = &r
			}
		case idx == doneIdx:
			aborted = true
		case idx == budgetIdx:
			budgetHit = true
		case idx-1 < len(retryKeys):
			fired = retryKeys[idx-1]
		default:
			timedOut = timeoutKeys[idx-1-len(retryKeys)]
		}

		if aborted {
			cleanup()
			return e.abort(id, wf, tasks, topo)
		}

		if budgetHit {
			cleanup()
			return e.budgetCutoff(id, wf, tasks, topo)
		}

		if res != nil {
			att, ok := runningTasks[res.taskID]
			if !ok || att != res.attempt {
				// 超时/预算收口后迟到的结果：状态机已推进，直接丢弃。
				continue
			}
			delete(runningTasks, res.taskID)
			if cancel, ok := attemptCancels[res.taskID]; ok {
				cancel() // 立即释放该尝试的超时定时器
				delete(attemptCancels, res.taskID)
			}
			delete(timeoutChs, res.taskID)

			t := tasks[res.taskID]
			sp := specs[res.taskID]
			if res.err == nil {
				t.LastError = ""
				t.EndedAt = nowHook()
				e.transitionTask(ctx, id, t, StatusSucceeded, ReasonSucceeded,
					"attempt "+strconv.Itoa(t.Attempt)+" succeeded", persist)
				continue
			}
			failAttempt(t, sp, res.err.Error(), ReasonAttemptFailed)
			continue
		}

		if timedOut != "" {
			// 本次尝试按失败处理：取消尝试 ctx 让任务感知退出，
			// 错误文本含 ErrTaskTimeout，与普通业务失败可区分。
			delete(runningTasks, timedOut)
			if cancel, ok := attemptCancels[timedOut]; ok {
				cancel()
				delete(attemptCancels, timedOut)
			}
			delete(timeoutChs, timedOut)

			t := tasks[timedOut]
			sp := specs[timedOut]
			failAttempt(t, sp,
				ErrTaskTimeout.Error()+" after "+sp.Timeout.String(),
				ReasonAttemptTimeout)
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

// budgetCutoff 预算耗尽收口：已终态任务结果保留；其余任务（含在途 RUNNING）
// 一律以终态 CANCELLED 落定，工作流聚合为终态 CANCELLED。
//
// 在途任务选择“立刻取消”而非“自然收尾”：预算的意义就是给整条流水线一个
// 硬性总时长上界，若允许在途任务自然结束，一个卡住的外部调用仍会把工作流
// 无限挂住——正是要解决的问题。取消通过 context 传递，配合的任务会尽快退出；
// 不配合的任务其迟到结果会被协调器按 attempt 对账丢弃，结论依然确定。
func (e *Engine) budgetCutoff(id string, wf *WorkflowState,
	tasks map[string]*TaskState, topo []string) (*WorkflowState, error) {
	ctx := context.Background()
	now := nowHook()
	for _, tid := range topo {
		t := tasks[tid]
		if t.Status.IsTerminal() {
			continue
		}
		from := t.Status
		t.Status = StatusCancelled
		t.LastError = ErrBudgetExhausted.Error()
		t.EndedAt = now
		t.UpdatedAt = now
		e.persistTask(ctx, id, t)
		e.emit(id, tid, string(from), string(StatusCancelled), ReasonBudgetExhausted,
			"workflow budget exhausted; task finalized as CANCELLED")
	}
	e.transitionWorkflowReason(ctx, id, wf, WfCancelled, ReasonBudgetExhausted,
		"workflow budget exhausted; no further dispatch")
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

// transitionWorkflow 迁移工作流聚合状态并写穿 Store（依据按目标状态推导）。
func (e *Engine) transitionWorkflow(ctx context.Context, wfID string, wf *WorkflowState,
	to WorkflowStatus, resume bool) {
	reason := ReasonScheduled
	if to.IsTerminal() {
		reason = ReasonWfDone
	} else if resume {
		reason = ReasonResume
	}
	e.transitionWorkflowReason(ctx, wfID, wf, to, reason, "workflow aggregate transition")
}

// transitionWorkflowReason 以显式依据迁移工作流聚合状态并写穿 Store。
func (e *Engine) transitionWorkflowReason(ctx context.Context, wfID string, wf *WorkflowState,
	to WorkflowStatus, reason EventReason, detail string) {
	from := wf.Status
	wf.Status = to
	wf.UpdatedAt = nowHook()
	_ = e.store.UpdateWorkflow(ctx, wfID, func(w *WorkflowState) bool {
		w.Status = to
		w.UpdatedAt = nowHook()
		return true
	})
	e.emit(wfID, "", string(from), string(to), reason, detail)
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
