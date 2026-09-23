package workflow

import "context"

// buildReadyQueue 为（续）跑构造初始就绪队列：处于 PENDING/READY 且依赖已全部
// 成功的任务按提交顺序入队。已终态的任务绝不会入队，因此不会被重复执行。
func (e *Engine) buildReadyQueue(wf *wfState) []string {
	wf.mu.Lock()
	var ready []string
	type readyInfo struct {
		id       string
		attempts int
	}
	var infos []readyInfo
	for _, id := range wf.order {
		ts := wf.tasks[id]
		if ts.status != StatusPending && ts.status != StatusReady {
			continue
		}
		if e.allDepsSucceededLocked(wf, ts) {
			ts.status = StatusReady
			infos = append(infos, readyInfo{id: id, attempts: ts.attempts})
			ready = append(ready, id)
		}
	}
	wf.mu.Unlock()
	for _, info := range infos {
		e.log.Append(wf.id, info.id, StatusPending, StatusReady, info.attempts,
			"becomes ready: dependencies "+joinIDs(wf.tasks[info.id].deps)+" all succeeded")
	}
	return ready
}

func (e *Engine) allDepsSucceededLocked(wf *wfState, ts *taskState) bool {
	for _, d := range ts.deps {
		if wf.tasks[d].status != StatusSucceeded {
			return false
		}
	}
	return true
}

// reconsider 在一个任务成功后，释放其下游：只把"依赖现已全部成功且仍为 PENDING"
// 的任务移入就绪队列。同一任务只会被释放一次，杜绝重复入队/重复运行。
func (e *Engine) reconsider(wf *wfState, completedID string, ready *[]string, backoffs *[]backoffEntry) {
	wf.mu.Lock()
	var newlyReady []string
	for _, depID := range wf.tasks[completedID].dependents {
		ts := wf.tasks[depID]
		if ts.status == StatusPending && e.allDepsSucceededLocked(wf, ts) {
			ts.status = StatusReady
			newlyReady = append(newlyReady, depID)
		}
	}
	wf.mu.Unlock()
	for _, id := range newlyReady {
		e.log.Append(wf.id, id, StatusPending, StatusReady, wf.tasks[id].attempts,
			"dependency released by upstream success; all dependencies succeeded -> READY")
		*ready = append(*ready, id)
	}
}

// propagateFailure 在任务终态失败后，把所有以其（直接或间接）为前置、
// 且尚未进入终态的下游任务标记为 SKIPPED，并把它们从就绪/退避集合中摘除。
// 扫描按提交顺序重复进行直到无变化，保证传播完整且结论确定。
func (e *Engine) propagateFailure(wf *wfState, failedID string, ready *[]string, backoffs *[]backoffEntry) {
	skipped := map[string]bool{}
	for {
		changed := false
		for _, id := range wf.order {
			ts := wf.tasks[id]
			if ts.status.Terminal() {
				continue
			}
			if ts.status == StatusRunning {
				continue // 理论上不会发生（失败前置不可能先放行下游），防御性保留
			}
			for _, d := range ts.deps {
				ds := wf.tasks[d].status
				if ds == StatusFailed || (ds == StatusSkipped) {
					wf.mu.Lock()
					from := ts.status
					ts.status = StatusSkipped
					ts.lastErr = "skipped due to upstream " + d + ": " + ds.String()
					wf.mu.Unlock()
					skipped[id] = true
					e.log.Append(wf.id, id, from, StatusSkipped, ts.attempts,
						"upstream dependency "+d+" is "+ds.String()+
							"; task can never become ready -> terminal SKIPPED")
					changed = true
					break
				}
			}
		}
		if !changed {
			break
		}
	}
	if len(skipped) == 0 {
		return
	}
	*ready = filterReady(*ready, func(id string) bool {
		st, _, _ := e.statusOfTask(wf.id, id)
		return !st.Terminal()
	})
	kept := (*backoffs)[:0]
	for _, b := range *backoffs {
		if st, _, _ := e.statusOfTask(wf.id, b.taskID); !st.Terminal() {
			kept = append(kept, b)
		}
	}
	*backoffs = kept
}

// checkConverged 在没有任何在飞尝试、退避等待和就绪任务时判定收敛：
// 全部终态且没有失败/跳过 -> SUCCEEDED；全部终态但存在失败 -> FAILED。
func (e *Engine) checkConverged(wf *wfState, inflight, backoffN, readyN int) (Status, bool) {
	if inflight > 0 || backoffN > 0 || readyN > 0 {
		return "", false
	}
	wf.mu.Lock()
	defer wf.mu.Unlock()
	allTerminal := true
	anyFailure := false
	for _, id := range wf.order {
		st := wf.tasks[id].status
		if !st.Terminal() {
			allTerminal = false
		}
		if st == StatusFailed || st == StatusSkipped {
			anyFailure = true
		}
	}
	if !allTerminal {
		return "", false
	}
	if anyFailure {
		return StatusFailed, true
	}
	return StatusSucceeded, true
}

// finish 提交工作流终态并记录终态事件。
func (e *Engine) finish(wf *wfState, status Status) {
	wf.mu.Lock()
	wf.status = status
	wf.mu.Unlock()
	reason := "all tasks reached terminal state; workflow converged to " + string(status)
	e.log.Append(wf.id, "", "", status, 0, reason)
}

// abort 处理上下文取消（中断）：
//   - 等待所有在飞 goroutine 退出并排空结果，避免泄漏；
//   - 在飞尝试尚未产出"已提交结论"，其状态回到 READY，已消耗尝试次数不变（不回退）；
//   - 已成功的任务保持 SUCCEEDED、已失败的保持 FAILED，续跑时绝不重复执行；
//   - 等待退避的任务保持 READY，续跑时继续剩余退避安排。
func (e *Engine) abort(wf *wfState, results <-chan attemptResult, inflight map[string]int) error {
	n := len(inflight)
	for i := 0; i < n; i++ {
		<-results // 刻意丢弃中断窗口内的结果：其结论对工作流不生效
	}
	type resetInfo struct {
		id       string
		attempts int
	}
	var resets []resetInfo
	wf.mu.Lock()
	for id := range inflight {
		ts := wf.tasks[id]
		ts.status = StatusReady
		// 不改动 ts.attempts：本次尝试未产出结论，下次续跑仍使用同一尝试序号区间。
		resets = append(resets, resetInfo{id: id, attempts: ts.attempts})
	}
	wf.status = StatusRunning // 保持非终态：等待 Resume 续跑
	wf.mu.Unlock()
	for _, r := range resets {
		e.log.Append(wf.id, r.id, StatusRunning, StatusReady, r.attempts,
			"execution interrupted: in-flight attempt produced no committed conclusion; reset to READY without consuming retry budget")
	}
	return context.Canceled
}

// filterReady 按保留谓词过滤就绪队列。
func filterReady(in []string, keep func(string) bool) []string {
	out := in[:0]
	for _, id := range in {
		if keep(id) {
			out = append(out, id)
		}
	}
	return out
}

// String 让 Status 可直接以字符串方式拼接进日志。
func (s Status) String() string { return string(s) }
