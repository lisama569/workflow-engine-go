package workflow

// validateGraph 在提交阶段对任务图做完整校验：
//  1. 任务非空且 ID 非空；
//  2. 任务 ID 不重复；
//  3. 每条依赖边都指向已登记的任务（缺失依赖）；
//  4. 图中不存在环（循环依赖，含自依赖）。
//
// 返回去重后的依赖列表（同一依赖重复声明只计一次，避免释放时被重复计数）。
// 任何问题都在提交期立即以可区分 Kind 的 *ValidationError 返回，绝不留到运行期卡死。
func validateGraph(tasks []*Task) (map[string][]string, error) {
	if len(tasks) == 0 {
		return nil, newValidationError(KindInvalidArgument, "", "", "task list is empty")
	}
	deps := make(map[string][]string, len(tasks))
	index := make(map[string]*Task, len(tasks))
	for _, t := range tasks {
		if t == nil {
			return nil, newValidationError(KindInvalidArgument, "", "", "nil task in list")
		}
		if t.ID == "" {
			return nil, newValidationError(KindInvalidArgument, "", "", "task with empty id")
		}
		if _, dup := index[t.ID]; dup {
			return nil, newValidationError(KindDuplicateTask, t.ID, "",
				"task id is declared more than once")
		}
		index[t.ID] = t
	}
	for _, t := range tasks {
		seen := make(map[string]bool, len(t.Deps))
		for _, dep := range t.Deps {
			if dep == "" {
				return nil, newValidationError(KindInvalidArgument, t.ID, "",
					"empty dependency id")
			}
			if _, ok := index[dep]; !ok {
				return nil, newValidationError(KindMissingDependency, t.ID, dep,
					"task depends on a task that was not submitted")
			}
			if !seen[dep] {
				seen[dep] = true
				deps[t.ID] = append(deps[t.ID], dep)
			}
		}
	}
	if cycle := findCycle(deps); cycle != "" {
		return nil, newValidationError(KindCycleDependency, cycle, "",
			"dependency graph contains a cycle (including self-dependency); task is on or behind the cycle")
	}
	return deps, nil
}

// findCycle 用 Kahn 拓扑排序定位环：
// 入度 = 尚未完成的依赖数；从无依赖节点出发，沿"下游"边释放。
// 排序结束后入度仍大于 0 的节点要么位于环上，要么依赖环上的节点。
func findCycle(deps map[string][]string) string {
	indeg := make(map[string]int, len(deps))
	// dependents[x] = 直接依赖 x 的任务集合（x 的下游出边）。
	dependents := make(map[string][]string, len(deps))
	for id, ds := range deps {
		indeg[id] = len(ds)
		for _, d := range ds {
			dependents[d] = append(dependents[d], id)
		}
	}
	var queue []string
	for id, n := range indeg {
		if n == 0 {
			queue = append(queue, id)
		}
	}
	processed := 0
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		processed++
		for _, next := range dependents[cur] {
			indeg[next]--
			if indeg[next] == 0 {
				queue = append(queue, next)
			}
		}
	}
	if processed == len(deps) {
		return ""
	}
	// 残余图（只含入度仍大于 0 的节点）中每个连通部分必然含环。
	// 在残余边上做 DFS 即可定位一个明确处于环上的节点，绝不能跨入已处理节点，
	// 否则在无环图上可能沿"残余节点 -> 已处理节点 -> 残余节点"走出伪环。
	var start string
	for id, n := range indeg {
		if n > 0 {
			start = id
			break
		}
	}
	color := map[string]int{} // 0=未访问 1=在当前路径上 2=已完成
	var dfs func(string) string
	dfs = func(cur string) string {
		color[cur] = 1
		for _, d := range deps[cur] {
			if indeg[d] == 0 {
				continue // 已被拓扑处理，不属于残余图
			}
			if color[d] == 1 {
				return d
			}
			if color[d] == 0 {
				if found := dfs(d); found != "" {
					return found
				}
			}
		}
		color[cur] = 2
		return ""
	}
	return dfs(start)
}
