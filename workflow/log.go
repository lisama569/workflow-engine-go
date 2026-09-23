package workflow

import (
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

// LogEventSink 将状态迁移事件以单行文本输出，便于人工排查与测试日志留痕。
type LogEventSink struct {
	mu sync.Mutex
	w  io.Writer
}

// NewLogEventSink 创建日志接收器；w 为 nil 时输出到 stdout。
func NewLogEventSink(w io.Writer) *LogEventSink {
	if w == nil {
		w = os.Stdout
	}
	return &LogEventSink{w: w}
}

func (l *LogEventSink) OnEvent(e Event) {
	l.mu.Lock()
	defer l.mu.Unlock()
	ts := e.Time
	if ts.IsZero() {
		ts = time.Now()
	}
	target := e.Workflow
	if e.TaskID != "" {
		target = e.Workflow + "/" + e.TaskID
	}
	from := e.From
	if from == "" {
		from = "-"
	}
	fmt.Fprintf(l.w, "[%s] %s %s -> %s | reason=%s %s\n",
		ts.Format(time.RFC3339Nano), target, from, e.To, e.Reason, e.Detail)
}
