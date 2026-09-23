package workflow

// Status 是任务（及工作流）在其生命周期中的状态。
type Status string

const (
	// StatusPending 已登记，但依赖尚未全部成功。
	StatusPending Status = "PENDING"
	// StatusReady 依赖已全部成功，等待并发槽位。
	StatusReady Status = "READY"
	// StatusRunning 正在执行（含某次重试尝试）。
	StatusRunning Status = "RUNNING"
	// StatusSucceeded 终态：执行成功。
	StatusSucceeded Status = "SUCCEEDED"
	// StatusFailed 终态：重试耗尽后仍失败。
	StatusFailed Status = "FAILED"
	// StatusSkipped 终态：上游依赖失败，任务永远不具备运行条件。
	StatusSkipped Status = "SKIPPED"
)

// Terminal 报告状态是否为不可再迁移的终态。
func (s Status) Terminal() bool {
	switch s {
	case StatusSucceeded, StatusFailed, StatusSkipped:
		return true
	default:
		return false
	}
}

// validTransitions 集中声明允许的任务状态迁移。
// 终态（SUCCEEDED/FAILED/SKIPPED）没有任何出边，因此重试与恢复都不会造成状态回退。
var validTransitions = map[Status]map[Status]bool{
	StatusPending: {
		StatusReady:   true, // 依赖全部成功，进入就绪队列
		StatusSkipped: true, // 上游终态失败，本任务永不可运行
	},
	StatusReady: {
		StatusRunning: true, // 获得并发槽位
	},
	StatusRunning: {
		StatusSucceeded: true, // 本次尝试成功
		StatusFailed:    true, // 最后一次尝试仍失败
		StatusReady:     true, // 尝试失败但仍有剩余重试，退避后重新就绪
	},
}

// validTransition 报告 from->to 是否为合法迁移；非法迁移会被引擎拒绝并 panic 式记录，
// 避免观测到自相矛盾的状态序列。
func validTransition(from, to Status) bool {
	return validTransitions[from][to]
}
