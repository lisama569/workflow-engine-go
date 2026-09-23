package workflow

import (
	"fmt"
	"sort"
)

// validateAndBuild 提交阶段校验：空图、重复 ID、缺失依赖、循环依赖。
// 通过后返回确定性的拓扑序（同层按 ID 排序），使调度顺序可复现、可解释。
func validateAndBuild(specs []TaskSpec) ([]string, error) {
	if len(specs) == 0 {
		return nil, &ValidationError{Kind: ErrEmptyWorkflow, Detail: "no task defined"}
	}

	specByID := make(map[string]TaskSpec, len(specs))
	indeg := make(map[string]int, len(specs))
	dependents := make(map[string][]string, len(specs)) // dep -> 依赖它的任务们

	for _, sp := range specs {
		if sp.ID == "" {
			return nil, &ValidationError{Kind: ErrDuplicateTask, Detail: "task id must not be empty"}
		}
		if _, ok := specByID[sp.ID]; ok {
			return nil, &ValidationError{Kind: ErrDuplicateTask, TaskID: sp.ID, Detail: "task id declared more than once"}
		}
		specByID[sp.ID] = sp
		indeg[sp.ID] = 0
	}

	// 缺失依赖必须在提交阶段被明确拒绝。
	for _, sp := range specs {
		for _, dep := range sp.Deps {
			if _, ok := specByID[dep]; !ok {
				return nil, &ValidationError{
					Kind:   ErrDependencyMissing,
					TaskID: sp.ID,
					Detail: fmt.Sprintf("dependency %q does not exist", dep),
				}
			}
		}
	}

	for _, sp := range specs {
		// 去重，避免同一条依赖被写两次而破坏入度计数。
		seen := make(map[string]struct{}, len(sp.Deps))
		for _, dep := range sp.Deps {
			if dep == sp.ID {
				return nil, &ValidationError{
					Kind:   ErrCycleDetected,
					TaskID: sp.ID,
					Detail: "task depends on itself",
				}
			}
			if _, dup := seen[dep]; dup {
				continue
			}
			seen[dep] = struct{}{}
			indeg[sp.ID]++
			dependents[dep] = append(dependents[dep], sp.ID)
		}
	}

	// Kahn 拓扑排序；结束时若仍有节点未入序，则存在循环依赖。
	// 每层对候选集合排序，保证同层调度顺序确定、结论可复现。
	ready := make([]string, 0)
	for id, d := range indeg {
		if d == 0 {
			ready = append(ready, id)
		}
	}
	sort.Strings(ready)

	topo := make([]string, 0, len(specs))
	for len(ready) > 0 {
		id := ready[0]
		ready = ready[1:]
		topo = append(topo, id)

		next := make([]string, 0)
		next = append(next, ready...)
		for _, child := range dependents[id] {
			indeg[child]--
			if indeg[child] == 0 {
				next = append(next, child)
			}
		}
		sort.Strings(next)
		ready = next
	}

	if len(topo) != len(specs) {
		remaining := make([]string, 0)
		for id, d := range indeg {
			if d > 0 {
				remaining = append(remaining, id)
			}
		}
		sort.Strings(remaining)
		return nil, &ValidationError{
			Kind:   ErrCycleDetected,
			Detail: fmt.Sprintf("cycle among tasks: %v", remaining),
		}
	}

	return topo, nil
}
