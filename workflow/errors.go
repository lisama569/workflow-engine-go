package workflow

import (
	"errors"
	"fmt"
)

// ErrorKind 以可区分的类别标识提交期（验证期）失败原因。
type ErrorKind string

const (
	KindDuplicateTask     ErrorKind = "duplicate_task"
	KindMissingDependency ErrorKind = "missing_dependency"
	KindCycleDependency   ErrorKind = "cycle_dependency"
	KindInvalidArgument   ErrorKind = "invalid_argument"
)

// ValidationError 在工作流提交阶段返回，调用方可用 Kind 区分失败原因。
type ValidationError struct {
	Kind   ErrorKind
	TaskID string // 触发问题的任务（可能为空）
	DepID  string // 相关依赖任务（如缺失的依赖）
	Detail string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("workflow validation failed (kind=%s task=%q dep=%q): %s",
		e.Kind, e.TaskID, e.DepID, e.Detail)
}

var (
	// ErrWorkflowNotFound 表示向未提交的工作流发起触发/续跑。
	ErrWorkflowNotFound = errors.New("workflow not found")
	// ErrWorkflowConverged 表示工作流已到达终态，重复触发不会再次执行。
	ErrWorkflowConverged = errors.New("workflow already in terminal state")
)

func newValidationError(kind ErrorKind, taskID, depID, detail string) *ValidationError {
	return &ValidationError{Kind: kind, TaskID: taskID, DepID: depID, Detail: detail}
}
