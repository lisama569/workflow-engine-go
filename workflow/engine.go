package workflow

import (
	"context"
)

// Trigger 幂等触发工作流执行：
//   - 工作流已在终态时，重复触发直接返回同一终态（ErrWorkflowConverged 作为可识别信号）；
//   - 并发触发时，后续调用方等待首个调用方的执行并共享唯一终态；
//   - concurrency<=0 表示无并发上限。
func (e *Engine) Trigger(ctx context.Context, workflowID string, concurrency int) (*RunResult, error) {
	return e.trigger(ctx, workflowID, concurrency, true)
}

// Resume 从上次中断处续跑：语义等价于以无上限并发重新参与调度，
// 但已处于终态（SUCCEEDED/FAILED/SKIPPED）的任务不会被再次执行或计数。
func (e *Engine) Resume(ctx context.Context, workflowID string) (*RunResult, error) {
	return e.trigger(ctx, workflowID, 0, false)
}

func (e *Engine) trigger(ctx context.Context, workflowID string, concurrency int, failIfConverged bool) (*RunResult, error) {
	wf, err := e.get(workflowID)
	if err != nil {
		return nil, err
	}

	wf.mu.Lock()
	if wf.status.Terminal() {
		status := wf.status
		wf.mu.Unlock()
		if failIfConverged {
			return e.resultFor(wf), &convergedError{status: status}
		}
		return e.resultFor(wf), nil
	}
	wf.mu.Unlock()

	owner, err := e.acquireRun(ctx, wf)
	if err != nil {
		return nil, err
	}
	if !owner {
		// 并发触发：他人执行已收敛，共享其唯一终态。
		res := e.resultFor(wf)
		if res.Status.Terminal() {
			// Trigger 的重复/并发调用方收到一致的可识别信号；Resume 不报错。
			if failIfConverged {
				return res, &convergedError{status: res.Status}
			}
			return res, nil
		}
		// 唯一非终态可能：等待到的执行被中断，本调用方未要求继续执行。
		return res, context.Canceled
	}

	runErr := e.run(ctx, wf, concurrency)
	wf.releaseRun()
	res := e.resultFor(wf)
	if runErr != nil {
		return res, runErr
	}
	return res, nil
}

// convergeError 已在 errors 语义中描述；run 在下方调度器文件中实现。
type convergedError struct{ status Status }

func (c *convergedError) Error() string {
	return ErrWorkflowConverged.Error() + ": " + string(c.status)
}
func (c *convergedError) Unwrap() error { return ErrWorkflowConverged }
