package workflow

import (
	"context"
	"sync"
)

// taskState 是任务在引擎内的可变运行时状态。
type taskState struct {
	def        *Task
	deps       []string // 归一化（去重）后的依赖
	dependents []string // 反向边：依赖本任务的任务
	status     Status
	attempts   int // 已"完整消耗"的尝试次数：只有产出结论的尝试才计数
	lastErr    string
}

// wfState 是单个工作流的全部运行时状态。
type wfState struct {
	mu sync.Mutex

	id     string
	order  []string // 提交顺序，保证就绪队列选取的确定性
	tasks  map[string]*taskState
	status Status // PENDING / RUNNING / SUCCEEDED / FAILED

	// runDone：非 nil 表示有一次执行正在进行，channel 会在执行结束时关闭。
	// 并发触发方等待同一 channel，从而共享唯一一次执行与唯一终态。
	runDone chan struct{}
}

// Engine 管理若干工作流的提交、触发、续跑与状态查询。
type Engine struct {
	mu        sync.Mutex
	workflows map[string]*wfState
	log       *EventLog
}

// Option 配置 Engine。
type Option func(*Engine)

// RunResult 是一次触发/续跑收敛后的结论。
type RunResult struct {
	WorkflowID string
	Status     Status // 工作流终态：SUCCEEDED / FAILED（中断时为 RUNNING）
	Events     []Event
}

// WithEventSink 注册一个状态迁移事件订阅者。
func WithEventSink(sink EventSink) Option {
	return func(e *Engine) { e.log.AddSink(sink) }
}

// NewEngine 创建引擎。
func NewEngine(opts ...Option) *Engine {
	e := &Engine{workflows: make(map[string]*wfState), log: NewEventLog()}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

// Submit 提交任务图并在提交阶段完成校验（重复/缺失/循环依赖）。
// 校验不通过时工作流不会被登记，可修正后重新提交。
func (e *Engine) Submit(workflowID string, tasks []*Task) error {
	if workflowID == "" {
		return newValidationError(KindInvalidArgument, "", "", "workflow id is empty")
	}
	deps, err := validateGraph(tasks)
	if err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, exists := e.workflows[workflowID]; exists {
		return newValidationError(KindInvalidArgument, "", "",
			"workflow id already submitted: "+workflowID)
	}
	wf := &wfState{id: workflowID, status: StatusPending, tasks: make(map[string]*taskState)}
	for _, t := range tasks {
		wf.order = append(wf.order, t.ID)
		wf.tasks[t.ID] = &taskState{def: t, deps: deps[t.ID], status: StatusPending}
	}
	for id, ts := range wf.tasks {
		for _, d := range ts.deps {
			wf.tasks[d].dependents = append(wf.tasks[d].dependents, id)
		}
	}
	e.workflows[workflowID] = wf
	return nil
}

// acquireRun 返回 owner=true 表示当前调用方成为本次执行的唯一持有者；
// owner=false 表示已有执行在进行，调用方已等待其结束，应直接读取当前结论。
func (e *Engine) acquireRun(ctx context.Context, wf *wfState) (owner bool, err error) {
	wf.mu.Lock()
	for {
		if wf.runDone == nil {
			if wf.status.Terminal() {
				wf.mu.Unlock()
				return false, nil
			}
			wf.runDone = make(chan struct{})
			wf.mu.Unlock()
			return true, nil
		}
		ch := wf.runDone
		wf.mu.Unlock()
		select {
		case <-ch:
			// 上一轮执行结束（可能已终态，也可能被中断），重新竞争。
		case <-ctx.Done():
			return false, ctx.Err()
		}
		wf.mu.Lock()
	}
}

// releaseRun 结束执行持有并唤醒所有等待方。
func (wf *wfState) releaseRun() {
	wf.mu.Lock()
	ch := wf.runDone
	wf.runDone = nil
	wf.mu.Unlock()
	if ch != nil {
		close(ch)
	}
}

// Snapshot 导出工作流当前状态，可序列化后模拟进程重启。
func (e *Engine) Snapshot(workflowID string) (*Snapshot, error) {
	wf, err := e.get(workflowID)
	if err != nil {
		return nil, err
	}
	wf.mu.Lock()
	defer wf.mu.Unlock()
	snap := &Snapshot{WorkflowID: wf.id, Status: wf.status}
	for _, id := range wf.order {
		ts := wf.tasks[id]
		snap.Tasks = append(snap.Tasks, TaskSnapshot{
			ID: id, Status: ts.status, Attempts: ts.attempts, Err: ts.lastErr,
		})
	}
	return snap, nil
}

// Restore 用快照恢复工作流：任务定义（含执行函数）需重新提交登记，
// 状态则完全以快照为准。快照中处于 RUNNING 的任务属于"被硬中断、
// 未产出结论"的尝试，恢复为 READY，且尝试次数不增加、不回退。
func (e *Engine) Restore(workflowID string, tasks []*Task, snap *Snapshot) error {
	if snap == nil {
		return newValidationError(KindInvalidArgument, "", "", "snapshot is nil")
	}
	if snap.WorkflowID != workflowID {
		return newValidationError(KindInvalidArgument, "", "",
			"snapshot workflow id does not match")
	}
	if err := e.Submit(workflowID, tasks); err != nil {
		return err
	}
	e.mu.Lock()
	wf := e.workflows[workflowID]
	e.mu.Unlock()

	wf.mu.Lock()
	snapStatus := snap.Status
	var interrupted []TaskSnapshot
	for _, ts := range snap.Tasks {
		st, ok := wf.tasks[ts.ID]
		if !ok {
			wf.mu.Unlock()
			return newValidationError(KindInvalidArgument, ts.ID, "",
				"snapshot contains task not present in submitted definitions")
		}
		st.attempts = ts.Attempts
		st.lastErr = ts.Err
		switch ts.Status {
		case StatusRunning:
			st.status = StatusReady
			interrupted = append(interrupted, ts)
		default:
			st.status = ts.Status
		}
	}
	switch snapStatus {
	case StatusSucceeded, StatusFailed:
		wf.status = snapStatus
	default:
		// PENDING/RUNNING 等非终态统一恢复为 RUNNING，表示存在未完成的执行。
		wf.status = StatusRunning
	}
	wf.mu.Unlock()
	for _, ts := range interrupted {
		e.log.Append(wf.id, ts.ID, StatusRunning, StatusReady, ts.Attempts,
			"restored from snapshot: interrupted attempt produced no conclusion, reset to READY without consuming retry budget")
	}
	return nil
}

func (e *Engine) get(workflowID string) (*wfState, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	wf, ok := e.workflows[workflowID]
	if !ok {
		return nil, ErrWorkflowNotFound
	}
	return wf, nil
}

// resultLocked 要求调用方不持锁；内部加锁组装收敛结论。
func (e *Engine) resultFor(wf *wfState) *RunResult {
	wf.mu.Lock()
	status := wf.status
	wf.mu.Unlock()
	var events []Event
	for _, ev := range e.log.Events() {
		if ev.WorkflowID == wf.id {
			events = append(events, ev)
		}
	}
	return &RunResult{WorkflowID: wf.id, Status: status, Events: events}
}

// statusOfTask 返回某任务的当前状态（供测试断言）。
func (e *Engine) statusOfTask(workflowID, taskID string) (Status, int, error) {
	wf, err := e.get(workflowID)
	if err != nil {
		return "", 0, err
	}
	wf.mu.Lock()
	defer wf.mu.Unlock()
	ts, ok := wf.tasks[taskID]
	if !ok {
		return "", 0, newValidationError(KindInvalidArgument, taskID, "", "task not found")
	}
	return ts.status, ts.attempts, nil
}
