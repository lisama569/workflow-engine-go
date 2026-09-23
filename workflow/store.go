package workflow

import (
	"context"
	"sync"
)

// Store 状态持久化抽象。引擎所有状态读写均经过 Store，
// 断点续跑时可基于上次中断的状态继续（本版本提供内存实现，
// 生产中可替换为数据库实现，接口语义保持不变）。
type Store interface {
	// Create 写入一个新提交的工作流（状态必须为 SUBMITTED，任务全部 PENDING）。
	Create(ctx context.Context, wf *WorkflowState, specs map[string]TaskSpec, topo []string) error
	// Load 读取工作流状态、任务规格与拓扑序；不存在返回 ErrWorkflowNotFound。
	// 返回的是深拷贝，调用方可安全持有。
	Load(ctx context.Context, id string) (*WorkflowState, map[string]TaskSpec, []string, error)
	// UpdateTask 在锁内原子地读取-迁移-写回单个任务状态。
	// mutate 返回 false 表示放弃本次迁移（状态保持不变）。
	UpdateTask(ctx context.Context, wfID string, taskID string, mutate func(t *TaskState) bool) error
	// UpdateWorkflow 在锁内原子迁移工作流聚合状态与任务集合。
	// mutate 返回 false 表示放弃。
	UpdateWorkflow(ctx context.Context, wfID string, mutate func(w *WorkflowState) bool) error
}

// memoryEntry 内存存储条目。
type memoryEntry struct {
	mu    sync.Mutex
	state *WorkflowState
	specs map[string]TaskSpec
	topo  []string
}

// MemoryStore 基于 map + 互斥锁的内存 Store。
type MemoryStore struct {
	mu   sync.RWMutex
	data map[string]*memoryEntry
}

// NewMemoryStore 创建内存 Store。
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{data: make(map[string]*memoryEntry)}
}

func (s *MemoryStore) Create(ctx context.Context, wf *WorkflowState, specs map[string]TaskSpec, topo []string) error {
	if wf == nil {
		return &ValidationError{Kind: ErrEmptyWorkflow, Detail: "nil workflow state"}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.data[wf.ID]; ok {
		return &ValidationError{Kind: ErrDuplicateTask, TaskID: wf.ID, Detail: "workflow id already submitted"}
	}
	entry := &memoryEntry{
		state: cloneWorkflowState(wf),
		specs: cloneSpecs(specs),
		topo:  append([]string(nil), topo...),
	}
	s.data[wf.ID] = entry
	return nil
}

func (s *MemoryStore) Load(ctx context.Context, id string) (*WorkflowState, map[string]TaskSpec, []string, error) {
	s.mu.RLock()
	entry, ok := s.data[id]
	s.mu.RUnlock()
	if !ok {
		return nil, nil, nil, ErrWorkflowNotFound
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	return cloneWorkflowState(entry.state), cloneSpecs(entry.specs), append([]string(nil), entry.topo...), nil
}

func (s *MemoryStore) UpdateTask(ctx context.Context, wfID string, taskID string, mutate func(t *TaskState) bool) error {
	s.mu.RLock()
	entry, ok := s.data[wfID]
	s.mu.RUnlock()
	if !ok {
		return ErrWorkflowNotFound
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	t, ok := entry.state.Tasks[taskID]
	if !ok {
		return &ValidationError{Kind: ErrWorkflowNotFound, TaskID: taskID, Detail: "task missing in state"}
	}
	// 在拷贝上执行 mutate，失败则不写回，避免回调中途污染存储。
	candidate := *t
	if !mutate(&candidate) {
		return nil
	}
	cp := candidate
	entry.state.Tasks[taskID] = &cp
	entry.state.UpdatedAt = nowHook()
	return nil
}

func (s *MemoryStore) UpdateWorkflow(ctx context.Context, wfID string, mutate func(w *WorkflowState) bool) error {
	s.mu.RLock()
	entry, ok := s.data[wfID]
	s.mu.RUnlock()
	if !ok {
		return ErrWorkflowNotFound
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	candidate := cloneWorkflowState(entry.state)
	if !mutate(candidate) {
		return nil
	}
	entry.state = candidate
	entry.state.UpdatedAt = nowHook()
	return nil
}

func cloneSpecs(in map[string]TaskSpec) map[string]TaskSpec {
	if in == nil {
		return nil
	}
	out := make(map[string]TaskSpec, len(in))
	for k, v := range in {
		cp := v
		cp.Deps = append([]string(nil), v.Deps...)
		out[k] = cp
	}
	return out
}

func cloneWorkflowState(w *WorkflowState) *WorkflowState {
	if w == nil {
		return nil
	}
	cp := *w
	if w.Tasks != nil {
		cp.Tasks = make(map[string]*TaskState, len(w.Tasks))
		for k, v := range w.Tasks {
			t := *v
			cp.Tasks[k] = &t
		}
	}
	return &cp
}
