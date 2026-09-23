package workflow

import (
	"context"
	"time"
)

// TaskStatus 任务生命周期状态。
type TaskStatus string

const (
	StatusPending   TaskStatus = "PENDING"   // 已提交，等待依赖
	StatusReady     TaskStatus = "READY"     // 依赖全部成功，等待调度
	StatusRunning   TaskStatus = "RUNNING"   // 正在执行
	StatusRetrying  TaskStatus = "RETRYING"  // 一次尝试失败，退避后重试
	StatusSucceeded TaskStatus = "SUCCEEDED" // 终态：成功
	StatusFailed    TaskStatus = "FAILED"    // 终态：重试耗尽
	StatusSkipped   TaskStatus = "SKIPPED"   // 终态：依赖失败被传播跳过
)

// WorkflowStatus 工作流聚合状态。
type WorkflowStatus string

const (
	WfSubmitted WorkflowStatus = "SUBMITTED" // 已提交未触发
	WfRunning   WorkflowStatus = "RUNNING"   // 运行中
	WfSucceeded WorkflowStatus = "SUCCEEDED" // 终态：全部成功
	WfFailed    WorkflowStatus = "FAILED"    // 终态：有任务失败/跳过
)

// IsTerminal 判断任务状态是否终态。
func (s TaskStatus) IsTerminal() bool {
	return s == StatusSucceeded || s == StatusFailed || s == StatusSkipped
}

// IsTerminal 判断工作流状态是否终态。
func (s WorkflowStatus) IsTerminal() bool {
	return s == WfSucceeded || s == WfFailed
}

// TaskFunc 任务执行函数。返回 error 表示本次尝试失败。
type TaskFunc func(ctx context.Context) error

// Backoff 退避策略：第 attempt 次重试（attempt 从 1 开始）前等待时长。
type Backoff interface {
	Next(attempt int) time.Duration
}

// RetryPolicy 重试策略。MaxRetries 为额外尝试次数（不含首次执行）。
type RetryPolicy struct {
	MaxRetries int
	Backoff    Backoff
}

// TaskSpec 任务声明。
type TaskSpec struct {
	ID           string
	Deps         []string
	Fn           TaskFunc
	Retry        RetryPolicy
	DepsOptional bool // 预留：非强依赖标记（本版本恒为强依赖）
}

// TaskState 任务的运行时状态。
type TaskState struct {
	ID        string
	Status    TaskStatus
	Attempt   int    // 已发起的尝试次数（首次执行计为 1）
	LastError string // 最近一次失败原因
	UpdatedAt time.Time
	StartedAt time.Time
	EndedAt   time.Time
}

// WorkflowState 工作流的运行时状态。
type WorkflowState struct {
	ID          string
	Status      WorkflowStatus
	Tasks       map[string]*TaskState
	Concurrency int // 提交时确定的并发上限，续跑沿用
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// EventReason 状态迁移触发依据。
type EventReason string

const (
	ReasonSubmitted        EventReason = "SUBMITTED"         // 提交/校验通过
	ReasonDepsMet          EventReason = "DEPS_MET"          // 依赖全部成功
	ReasonScheduled        EventReason = "SCHEDULED"         // 获得并发槽被调度
	ReasonAttemptFailed    EventReason = "ATTEMPT_FAILED"    // 单次尝试失败，将重试
	ReasonRetryBackoff     EventReason = "RETRY_BACKOFF"     // 退避结束发起重试
	ReasonSucceeded        EventReason = "SUCCEEDED"         // 执行成功
	ReasonRetriesExhausted EventReason = "RETRIES_EXHAUSTED" // 重试耗尽
	ReasonDepFailed        EventReason = "DEP_FAILED"        // 依赖失败导致跳过
	ReasonWfDone           EventReason = "WF_DONE"           // 工作流到达终态
	ReasonResume           EventReason = "RESUME"            // 断点续跑扫描
	ReasonDeduplicated     EventReason = "DEDUPLICATED"      // 重复触发被幂等忽略
)

// Event 对外可观测的状态迁移事件。
type Event struct {
	Time     time.Time
	Workflow string
	TaskID   string
	From     string
	To       string
	Reason   EventReason
	Detail   string
}

// EventSink 事件输出接口（日志/测试断言用）。
type EventSink interface {
	OnEvent(e Event)
}

// EventSinkFunc 函数式适配器。
type EventSinkFunc func(e Event)

func (f EventSinkFunc) OnEvent(e Event) { f(e) }
