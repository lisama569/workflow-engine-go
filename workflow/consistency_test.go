package workflow

import (
	"context"
	"sync/atomic"
	"testing"
)

// TestStateMachine_TerminalHasNoOutgoing 直接验证状态机声明：终态没有任何合法出边。
func TestStateMachine_TerminalHasNoOutgoing(t *testing.T) {
	for _, terminal := range []Status{StatusSucceeded, StatusFailed, StatusSkipped} {
		for _, target := range []Status{StatusPending, StatusReady, StatusRunning, StatusSucceeded, StatusFailed, StatusSkipped} {
			if validTransition(terminal, target) {
				t.Fatalf("terminal state %s must not transition to %s", terminal, target)
			}
		}
	}
}

// TestEventSequence_NoRollbackAfterRetry 验证重试与失败场景下，事件序列中
// 不存在"终态之后再迁移"或"RUNNING 直接回 PENDING"等回退。
func TestEventSequence_NoRollbackAfterRetry(t *testing.T) {
	e := newTestEngine(t)
	var n int32
	flaky := &Task{ID: "x", Retry: RetryPolicy{MaxAttempts: 3}, Run: func(context.Context, int) error {
		if atomic.AddInt32(&n, 1) < 3 {
			return errFlaky
		}
		return nil
	}}
	if err := e.Submit("no-rollback", []*Task{flaky}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Trigger(context.Background(), "no-rollback", 1); err != nil {
		t.Fatal(err)
	}
	seq := eventSequenceFor(e, "no-rollback")
	t.Logf("sequence: %s", sequenceString(seq))
	seenTerminal := map[string]bool{}
	for _, s := range seq {
		taskID, to := splitTransition(s)
		if taskID == "" {
			continue // 工作流级事件
		}
		if seenTerminal[taskID] {
			t.Fatalf("transition %s after terminal state", s)
		}
		if to == StatusPending.String() {
			t.Fatalf("observed rollback to PENDING: %s", s)
		}
		if Status(to).Terminal() {
			seenTerminal[taskID] = true
		}
	}
}

// splitTransition 解析 "a:RUNNING->READY" 形式，返回任务 ID 与目标状态。
func splitTransition(s string) (string, string) {
	colon := -1
	arrow := -1
	for i := 0; i < len(s); i++ {
		if s[i] == ':' && colon == -1 {
			colon = i
		}
		if s[i] == '-' && i+1 < len(s) && s[i+1] == '>' {
			arrow = i
			break
		}
	}
	if colon < 0 || arrow < 0 {
		return "", ""
	}
	return s[:colon], s[arrow+2:]
}
