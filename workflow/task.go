package workflow

import (
	"context"
	"time"
)

// TaskFunc 是任务的可执行体。attempt 从 1 开始，表示这是第几次尝试。
type TaskFunc func(ctx context.Context, attempt int) error

// RetryPolicy 描述失败后的重试与退避规则。
type RetryPolicy struct {
	// MaxAttempts 总尝试次数（含首次）。<=0 时按 1 处理，即不重试。
	MaxAttempts int
	// InitialBackoff 首次重试前的退避时长。
	InitialBackoff time.Duration
	// MaxBackoff 退避上限，0 表示不限制。
	MaxBackoff time.Duration
	// Multiplier 退避乘数，<=1 时按 1（不增长）处理。
	Multiplier float64
}

// Task 是工作流中的一个节点定义。
type Task struct {
	ID    string
	Deps  []string
	Retry RetryPolicy
	Run   TaskFunc
}

// NormalizedMaxAttempts 返回归一化后的总尝试次数：小于 1 视为 1（不重试）。
func (p RetryPolicy) NormalizedMaxAttempts() int {
	if p.MaxAttempts < 1 {
		return 1
	}
	return p.MaxAttempts
}

// backoffForAttempt 计算"第 attempt 次尝试失败后、下一次尝试前"的退避时长。
// attempt 从 1 开始；若已经是最后一次尝试，调用方不会再调用本方法。
func (p RetryPolicy) backoffForAttempt(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	mult := p.Multiplier
	if mult < 1 {
		mult = 1
	}
	d := float64(p.InitialBackoff)
	for i := 1; i < attempt; i++ {
		d *= mult
	}
	backoff := time.Duration(d)
	if p.MaxBackoff > 0 && backoff > p.MaxBackoff {
		backoff = p.MaxBackoff
	}
	if backoff < 0 {
		backoff = 0
	}
	return backoff
}
