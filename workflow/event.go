package workflow

import (
	"sync"
	"time"
)

// Event 描述一次可观测的任务状态迁移及其触发依据。
type Event struct {
	Seq        int64
	Time       time.Time
	WorkflowID string
	TaskID     string
	From       Status
	To         Status
	Attempt    int
	// Reason 用自然语言说明本次迁移的触发依据，
	// 例如 "dependencies [a b] all succeeded" 或 "retry 2/3 after 10ms backoff"。
	Reason string
}

// EventSink 接收状态迁移事件（测试中可接 t.Logf 打印）。
type EventSink func(Event)

// EventLog 以全序记录所有状态迁移事件，并发安全。
type EventLog struct {
	mu     sync.Mutex
	events []Event
	seq    int64
	sinks  []EventSink
}

// NewEventLog 创建空的事件日志。
func NewEventLog() *EventLog { return &EventLog{} }

// NewEventLog 已在下方其它构造中使用；AddSink 注册一个事件订阅者。
// 订阅者在记录锁之外被同步调用，从而可安全地打印到测试日志。
func (l *EventLog) AddSink(sink EventSink) {
	if sink == nil {
		return
	}
	l.mu.Lock()
	l.sinks = append(l.sinks, sink)
	l.mu.Unlock()
}

// Append 记录一次迁移，赋予单调递增的全序序列号并返回落库后的事件。
func (l *EventLog) Append(workflowID, taskID string, from, to Status, attempt int, reason string) Event {
	l.mu.Lock()
	l.seq++
	ev := Event{
		Seq:        l.seq,
		Time:       time.Now(),
		WorkflowID: workflowID,
		TaskID:     taskID,
		From:       from,
		To:         to,
		Attempt:    attempt,
		Reason:     reason,
	}
	l.events = append(l.events, ev)
	sinks := append([]EventSink(nil), l.sinks...)
	l.mu.Unlock()
	for _, sink := range sinks {
		sink(ev)
	}
	return ev
}

// Events 返回事件的快照副本（按全序序列号排列）。
func (l *EventLog) Events() []Event {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]Event, len(l.events))
	copy(out, l.events)
	return out
}
