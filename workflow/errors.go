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
	// ErrInvalidTimeout 任务超时配置非法（负数）。
	ErrInvalidTimeout = errors.New("workflow: invalid task timeout")
	// ErrInvalidBudget 工作流总预算非法（<= 0）。
	ErrInvalidBudget = errors.New("workflow: invalid workflow budget")
	// ErrTaskTimeout 单次尝试超出任务声明的最长执行时间。
	// 出现在 TaskState.LastError 与事件 Detail 中，与普通业务失败可区分。
	ErrTaskTimeout = errors.New("workflow: task attempt timed out")
	// ErrBudgetExhausted 工作流总预算耗尽。
	// 出现在被收口任务的 LastError 与 BUDGET_EXHAUSTED 事件 Detail 中。
	ErrBudgetExhausted = errors.New("workflow: workflow budget exhausted")
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
