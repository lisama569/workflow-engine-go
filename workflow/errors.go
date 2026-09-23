package workflow

import (
	"errors"
	"fmt"
)

// 提交阶段可区分的校验错误（哨兵错误，可用 errors.Is 判断）。
var (
	// ErrCycleDetected 任务依赖图中存在循环依赖。
	ErrCycleDetected = errors.New("workflow: cycle detected in task dependencies")
	// ErrDependencyMissing 任务引用了不存在的依赖。
	ErrDependencyMissing = errors.New("workflow: task depends on a missing task")
	// ErrEmptyWorkflow 工作流没有任何任务。
	ErrEmptyWorkflow = errors.New("workflow: workflow has no tasks")
	// ErrDuplicateTask 任务 ID 重复。
	ErrDuplicateTask = errors.New("workflow: duplicate task id")
	// ErrInvalidConcurrency 并发上限非法。
	ErrInvalidConcurrency = errors.New("workflow: invalid concurrency limit")
	// ErrInvalidRetry 重试配置非法。
	ErrInvalidRetry = errors.New("workflow: invalid retry config")
	// ErrWorkflowNotFound 存储中找不到对应工作流。
	ErrWorkflowNotFound = errors.New("workflow: workflow not found")
	// ErrAlreadyTerminal 工作流已进入终态，不能再次触发。
	ErrAlreadyTerminal = errors.New("workflow: workflow already in terminal state")
	// ErrNotSubmitted 工作流尚未提交，无法触发。
	ErrNotSubmitted = errors.New("workflow: workflow not submitted")
)

// ValidationError 携带具体任务上下文的校验错误。
type ValidationError struct {
	Kind   error  // 哨兵错误
	TaskID string // 相关任务 ID（若适用）
	Detail string // 可读说明
}

func (e *ValidationError) Error() string {
	if e.TaskID != "" {
		return fmt.Sprintf("%v: task=%q: %s", e.Kind, e.TaskID, e.Detail)
	}
	return fmt.Sprintf("%v: %s", e.Kind, e.Detail)
}
func (e *ValidationError) Unwrap() error { return e.Kind }
