package workflow

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// testLoggerSink 把每次状态迁移与触发依据打印到单测日志，满足
// "单测日志中需打印任务状态迁移与触发依据" 的要求。
func testLoggerSink(t *testing.T) EventSink {
	t.Helper()
	return func(ev Event) {
		if ev.TaskID == "" {
			t.Logf("[WF %s] %s -> %s | %s", ev.WorkflowID, ev.From, ev.To, ev.Reason)
			return
		}
		t.Logf("[WF %s] task=%-4s attempt=%d %-8s -> %-9s | %s",
			ev.WorkflowID, ev.TaskID, ev.Attempt, ev.From, ev.To, ev.Reason)
	}
}

func newTestEngine(t *testing.T) *Engine {
	t.Helper()
	return NewEngine(WithEventSink(testLoggerSink(t)))
}

func okTask(id string, deps ...string) *Task {
	return &Task{ID: id, Deps: deps, Run: func(_ context.Context, _ int) error { return nil }}
}

func asValidationError(t *testing.T, err error) *ValidationError {
	t.Helper()
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *ValidationError, got %T: %v", err, err)
	}
	return ve
}

func requireStatus(t *testing.T, e *Engine, wfID, taskID string, want Status) {
	t.Helper()
	got, attempts, err := e.statusOfTask(wfID, taskID)
	if err != nil {
		t.Fatalf("query task %s: %v", taskID, err)
	}
	if got != want {
		t.Fatalf("task %s status = %s, want %s (attempts=%d)", taskID, got, want, attempts)
	}
}

func requireAttempts(t *testing.T, e *Engine, wfID, taskID string, want int) {
	t.Helper()
	_, got, err := e.statusOfTask(wfID, taskID)
	if err != nil {
		t.Fatalf("query task %s: %v", taskID, err)
	}
	if got != want {
		t.Fatalf("task %s attempts = %d, want %d", taskID, got, want)
	}
}

// eventSequenceFor 提取某工作流的 "task:FROM->TO" 序列，供断言自洽性。
func eventSequenceFor(e *Engine, wfID string) []string {
	var seq []string
	for _, ev := range e.log.Events() {
		if ev.WorkflowID != wfID {
			continue
		}
		if ev.TaskID == "" {
			seq = append(seq, "WF:"+string(ev.To))
		} else {
			seq = append(seq, ev.TaskID+":"+string(ev.From)+"->"+string(ev.To))
		}
	}
	return seq
}

func sequenceString(seq []string) string { return strings.Join(seq, " | ") }
