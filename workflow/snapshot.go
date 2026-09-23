package workflow

import "encoding/json"

// TaskSnapshot 是单个任务在某一时刻的可恢复状态。
type TaskSnapshot struct {
	ID       string `json:"id"`
	Status   Status `json:"status"`
	Attempts int    `json:"attempts"` // 已消耗的尝试次数（成功/失败均计入，续跑不重置）
	Err      string `json:"err,omitempty"`
}

// Snapshot 是整个工作流的可恢复状态。
type Snapshot struct {
	WorkflowID string         `json:"workflow_id"`
	Status     Status         `json:"status"`
	Tasks      []TaskSnapshot `json:"tasks"`
}

// Marshal 把快照序列化为 JSON 字节，模拟把状态写入持久化介质。
func (s *Snapshot) Marshal() ([]byte, error) {
	return json.Marshal(s)
}

// UnmarshalSnapshot 从 JSON 字节恢复快照，模拟重启后从介质读回状态。
func UnmarshalSnapshot(data []byte) (*Snapshot, error) {
	var s Snapshot
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, err
	}
	return &s, nil
}
