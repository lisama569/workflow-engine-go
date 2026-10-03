package workflow

import (
	"context"
	"sync"
	"time"
)

// noopSink 默认事件接收器：丢弃所有事件。
type noopSink struct{}

func (noopSink) OnEvent(Event) {}

// Engine 工作流编排引擎。零值不可用，请使用 NewEngine。
type Engine struct {
	store       Store
	defaultConc int
	sink        EventSink

	runMu sync.Mutex
	runs  map[string]*runHandle
}

// runHandle 一次在途执行（同一工作流全局唯一）。
type runHandle struct {
	done     chan struct{}
	cancelFn context.CancelFunc
	mu       sync.Mutex
	state    *WorkflowState // 终态快照
	err      error
}

func (h *runHandle) cancel() {
	if h.cancelFn != nil {
		h.cancelFn()
	}
}

// Option 引擎级配置。
type Option func(*Engine)

// WithEventSink 注入状态迁移事件输出。
func WithEventSink(sink EventSink) Option {
	return func(e *Engine) {
		if sink != nil {
			e.sink = sink
		}
	}
}

// WithDefaultConcurrency 设置未显式声明并发上限时的默认值（默认 1）。
func WithDefaultConcurrency(n int) Option {
	return func(e *Engine) {
		if n > 0 {
			e.defaultConc = n
		}
	}
}

// NewEngine 创建引擎。
func NewEngine(store Store, opts ...Option) *Engine {
	e := &Engine{
		store:       store,
		defaultConc: 1,
		sink:        noopSink{},
		runs:        make(map[string]*runHandle),
	}
	for _, o := range opts {
		o(e)
	}
	return e
}

// SubmitOption 提交级配置。
type SubmitOption func(*submitConfig)

type submitConfig struct {
	concurrency int
	budget      time.Duration
}

// WithConcurrency 设置该工作流的最大并发任务数（默认取引擎默认值）。
func WithConcurrency(n int) SubmitOption {
	return func(c *submitConfig) { c.concurrency = n }
}

// WithBudget 设置工作流总执行时间预算：从首次触发（含续跑的第一次运行）
// 起算的总时长上限。预算耗尽后不再派发新任务，在途任务被取消，
// 未完成任务以终态 CANCELLED 收口，工作流落到终态 CANCELLED。
// 不设置（0）表示不限制。
func WithBudget(d time.Duration) SubmitOption {
	return func(c *submitConfig) { c.budget = d }
}

// Submit 提交工作流。静态校验（重复 ID / 缺失依赖 / 循环依赖 / 非法配置）
// 在此时同步完成并以可区分的错误拒绝，绝不留到运行阶段。
func (e *Engine) Submit(id string, specs []TaskSpec, opts ...SubmitOption) error {
	cfg := submitConfig{concurrency: e.defaultConc}
	for _, o := range opts {
		o(&cfg)
	}
	if cfg.concurrency <= 0 {
		return &ValidationError{Kind: ErrInvalidConcurrency, Detail: "concurrency must be > 0"}
	}
	if cfg.budget < 0 {
		return &ValidationError{Kind: ErrInvalidBudget, Detail: "budget must be >= 0 (0 = unlimited)"}
	}

	topo, err := validateAndBuild(specs)
	if err != nil {
		return err
	}
	for _, sp := range specs {
		if sp.Retry.MaxRetries < 0 {
			return &ValidationError{Kind: ErrInvalidRetry, TaskID: sp.ID, Detail: "max retries must be >= 0"}
		}
		if sp.Timeout < 0 {
			return &ValidationError{Kind: ErrInvalidTimeout, TaskID: sp.ID, Detail: "timeout must be >= 0 (0 = unlimited)"}
		}
		if sp.Fn == nil {
			return &ValidationError{Kind: ErrEmptyWorkflow, TaskID: sp.ID, Detail: "task function is nil"}
		}
	}

	now := nowHook()
	wf := &WorkflowState{
		ID:          id,
		Status:      WfSubmitted,
		Tasks:       make(map[string]*TaskState, len(specs)),
		Concurrency: cfg.concurrency,
		Budget:      cfg.budget,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	specByID := make(map[string]TaskSpec, len(specs))
	for _, sp := range specs {
		specByID[sp.ID] = sp
		wf.Tasks[sp.ID] = &TaskState{ID: sp.ID, Status: StatusPending, UpdatedAt: now}
	}
	if err := e.store.Create(context.Background(), wf, specByID, topo); err != nil {
		return err
	}
	e.emit(id, "", "", string(WfSubmitted), ReasonSubmitted,
		"accepted with topo="+fmtSlice(topo))
	return nil
}

// Trigger 触发工作流并阻塞至终态（或 ctx 取消）。
// 对同一工作流的并发/重复触发共享同一次执行：不重复运行任何任务，
// 等待者拿到同一份终态；已在终态的工作流直接返回既有终态。
func (e *Engine) Trigger(ctx context.Context, id string) (*WorkflowState, error) {
	return e.startOrJoin(ctx, id, false)
}

// Resume 从上次中断处续跑。已处于终态（成功/失败）的任务一律跳过，
// 被中断的中间态任务回收后续跑；工作流已终态时直接返回快照。
func (e *Engine) Resume(ctx context.Context, id string) (*WorkflowState, error) {
	return e.startOrJoin(ctx, id, true)
}

// Cancel 请求中断一个在途执行。协调器会在下一个调度点停止派发，
// 已完成的任务保持终态，未完成的中间态任务回收为可续跑状态。
func (e *Engine) Cancel(id string) bool {
	e.runMu.Lock()
	h, ok := e.runs[id]
	e.runMu.Unlock()
	if !ok {
		return false
	}
	h.cancel()
	return true
}

// GetState 读取工作流状态的深拷贝快照。
func (e *Engine) GetState(ctx context.Context, id string) (*WorkflowState, error) {
	wf, _, _, err := e.store.Load(ctx, id)
	return wf, err
}

// startOrJoin 幂等入口：存在在途执行则加入等待，否则启动唯一执行。
func (e *Engine) startOrJoin(ctx context.Context, id string, resume bool) (*WorkflowState, error) {
	wf, _, _, err := e.store.Load(ctx, id)
	if err != nil {
		return nil, err
	}

	e.runMu.Lock()
	if wf.Status.IsTerminal() {
		e.runMu.Unlock()
		e.emit(id, "", "", string(wf.Status), ReasonDeduplicated,
			"workflow already terminal; no task re-executed")
		return wf, nil
	}
	if h, ok := e.runs[id]; ok {
		e.runMu.Unlock()
		e.emit(id, "", "", "", ReasonDeduplicated,
			"concurrent trigger joined the in-flight run")
		return h.wait(ctx)
	}

	runCtx, cancel := context.WithCancel(context.Background())
	h := &runHandle{done: make(chan struct{}), cancelFn: cancel}
	e.runs[id] = h
	e.runMu.Unlock()

	go func() {
		final, runErr := e.run(runCtx, id, resume)
		// 运行结束即取消 runCtx：被超时/预算遗弃的工作协程
		// （若其函数尚未返回）会观察到 ctx 关闭并退出发送，避免协程滞留。
		cancel()
		h.mu.Lock()
		h.state, h.err = final, runErr
		h.mu.Unlock()
		close(h.done)

		e.runMu.Lock()
		if cur, ok := e.runs[id]; ok && cur == h {
			delete(e.runs, id)
		}
		e.runMu.Unlock()
	}()

	return h.wait(ctx)
}

func (h *runHandle) wait(ctx context.Context) (*WorkflowState, error) {
	select {
	case <-h.done:
		h.mu.Lock()
		defer h.mu.Unlock()
		return cloneWorkflowState(h.state), h.err
	case <-ctx.Done():
		// 调用方放弃等待，不影响后台唯一执行的生命周期。
		return nil, ctx.Err()
	}
}

// emit 输出状态迁移事件（统一加时间戳）。
func (e *Engine) emit(wfID, taskID, from, to string, reason EventReason, detail string) {
	e.sink.OnEvent(Event{
		Time:     nowHook(),
		Workflow: wfID,
		TaskID:   taskID,
		From:     from,
		To:       to,
		Reason:   reason,
		Detail:   detail,
	})
}

// nowHook 便于测试控制时间。
var nowHook = func() time.Time { return time.Now() }
