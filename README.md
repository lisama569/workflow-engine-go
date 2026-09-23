# 本地工作流编排引擎（Go）

一个纯内存状态的本地工作流编排引擎，用 goroutine + channel + 互斥锁模拟
任务依赖调度、失败重试、中断续跑与幂等触发，目标是让并发调度、失败恢复
在不同入口（`Trigger` / `Resume` / 快照恢复）下都得到**一致且可解释**的结论。

仅依赖 Go 标准库。

## 1. 核心概念

| 类型 | 说明 |
| --- | --- |
| `Task` | 任务节点：`ID`、`Deps`（前置依赖）、`Retry`（重试策略）、`Run`（执行体） |
| `RetryPolicy` | `MaxAttempts`（含首次的总尝试次数）、`InitialBackoff`、`MaxBackoff`、`Multiplier` 指数退避 |
| `Engine` | 提交、触发、续跑、快照导出/恢复多个工作流 |
| `Event` | 一次状态迁移记录，含全序序列号与"触发依据"自然语言原因 |

### 任务生命周期状态

```
PENDING ──依赖全部成功──▶ READY ──获得并发槽──▶ RUNNING
   │                       ▲    │                  │
   │                       │    │ 尝试失败但仍有预算 │ 成功
   │ 上游 FAILED/SKIPPED   └────┴──退避后重新就绪   ▼
   ▼                                         SUCCEEDED（终态）
 SKIPPED（终态）                               最后一次失败
                                                  ▼
                                               FAILED（终态）
```

终态（`SUCCEEDED` / `FAILED` / `SKIPPED`）没有任何合法出边，
所有迁移在 `state.go` 的 `validTransitions` 中集中声明，从机制上杜绝状态回退。

## 2. 调度规则

1. **依赖门槛**：任务仅当其所有依赖都为 `SUCCEEDED` 时才会进入 `READY`。
   就绪选取按提交顺序（FIFO），结论确定、可复现。
2. **并发上限**：`Trigger(ctx, id, concurrency)` 中 `concurrency` 限制同时
   在飞的任务数；`<=0` 表示无上限。槽位释放前不会超额启动。
3. **失败传播**：任务终态 `FAILED` 后，其全部直接与间接下游（且尚未进入终态）
   被确定性标记为 `SKIPPED`——它们永远不再具备运行条件，因此**不会漏跑**
   （SKIPPED 是明确终态而非悬挂），也不会执行。
4. **收敛判定**：无在飞任务、无退避等待、无就绪任务时收敛：
   - 全部终态且无失败/跳过 → 工作流 `SUCCEEDED`
   - 全部终态但存在失败/跳过 → 工作流 `FAILED`
5. **提交期校验**（绝不留到运行中途静默卡死）：
   - 空任务 / 空 ID → `invalid_argument`
   - 任务 ID 重复 → `duplicate_task`
   - 依赖未提交 → `missing_dependency`（错误中同时给出任务与缺失依赖）
   - 存在环（含自依赖）→ `cycle_dependency`（Kahn 拓扑排序定位环上节点）

   校验失败以 `*ValidationError` 返回，调用方可用其 `Kind` 字段区分原因，
   且失败的提交不会登记工作流。

## 3. 重试语义

- `MaxAttempts` 是**总尝试次数（含首次）**；`<=0` 视为 1，即不重试。
- 某次尝试失败后：
  - 若 `attempt < MaxAttempts`：任务回到 `READY`，等待
    `InitialBackoff * Multiplier^(attempt-1)`（受 `MaxBackoff` 截断）后重试；
  - 若已是最后一次：进入终态 `FAILED` 并触发失败传播。
- 只有**产出结论的尝试**才消耗计数；`RUNNING -> READY`（等待重试）不回退计数。
- 成功即止：成功后 `attempts` 记录成功所在的尝试序号，之后不再执行。

## 4. 恢复与断点续跑语义

有两条续跑路径，语义一致：

1. **进程内中断 + `Resume`**：`Trigger` 的 context 被取消时，引擎
   - 等待所有在飞 goroutine 退出（无泄漏），中断窗口内的尝试结果**不生效**；
   - 在飞任务回到 `READY`，且**不消耗重试预算、不回退已消耗次数**；
   - 已 `SUCCEEDED`/`FAILED`/`SKIPPED` 的任务保持终态；
   - `Resume` 从上次中断处继续，终态任务绝不重复执行。
2. **跨"进程重启" + 快照**：`Snapshot()` 导出 JSON 可序列化状态，
   用 `Engine.Restore(id, tasks, snap)` 在新引擎中重新登记任务定义（`Run`
   函数无法序列化，需重新提供）并恢复状态；快照中处于 `RUNNING` 的任务
   一律恢复为 `READY`（被硬中断、未产出结论），`attempts` 原样保留。

## 5. 幂等与并发一致性

- 同一工作流的 `Trigger`/`Resume` 在引擎内部通过"执行归属锁"串行化：
  同一时刻只存在一场执行；并发调用方等待同一场执行并共享唯一终态。
- 工作流进入终态后，重复 `Trigger` 不执行任何任务，返回包装了
  `ErrWorkflowConverged` 的错误（`errors.Is` 可识别）与唯一终态；
  `Resume` 返回同一终态且不报错、不重跑。
- 所有状态迁移都在唯一调度循环内串行发生，任务执行体在独立 goroutine 中
  并发运行，仅通过带缓冲 channel 汇报结果，因此**依赖释放与状态迁移不存在竞争**。
- `EventLog` 以单调递增序列号给出全序变更序列；每个事件都带有触发依据
  （如 "dependencies [a] all succeeded"、"retry 2/3 after … backoff"），
  可通过 `WithEventSink` 订阅打印，对外观测始终自洽。

## 6. 快速上手

```go
engine := workflow.NewEngine(workflow.WithEventSink(func(ev workflow.Event) {
    log.Printf("%s: %s -> %s | %s", ev.TaskID, ev.From, ev.To, ev.Reason)
}))

err := engine.Submit("etl", []*workflow.Task{
    {ID: "fetch", Run: fetchFn},
    {ID: "parse", Deps: []string{"fetch"},
        Retry: workflow.RetryPolicy{MaxAttempts: 3, InitialBackoff: 10 * time.Millisecond, Multiplier: 2},
        Run: parseFn},
    {ID: "load", Deps: []string{"parse"}, Run: loadFn},
})
if err != nil {
    var ve *workflow.ValidationError
    if errors.As(err, &ve) {
        switch ve.Kind { // 在提交阶段明确区分拒绝原因
        case workflow.KindCycleDependency, workflow.KindMissingDependency:
            // …
        }
    }
}

res, err := engine.Trigger(context.Background(), "etl", 4) // 并发上限 4
fmt.Println(res.Status) // SUCCEEDED / FAILED
```

## 7. 验证方法

```bash
go test ./... -race -v          # 全量用例 + 竞态检测 + 状态迁移日志
go test ./... -race -count=10   # 反复运行并发用例，排除偶发竞争
go vet ./... && gofmt -l .      # 静态检查与格式
```

测试覆盖矩阵：

| 场景 | 用例 |
| --- | --- |
| 循环依赖 / 自依赖 / 缺失依赖 / 重复任务 | `TestSubmit_*` |
| 依赖门槛与执行顺序 | `TestDAGSchedule_RespectsDependencies` |
| 并发上限不被突破 | `TestDAGSchedule_ConcurrencyLimit` |
| 重试成功（退避、序号） | `TestRetry_SucceedsWithinBudget` |
| 重试耗尽 → FAILED + 下游 SKIPPED | `TestRetry_ExhaustedLeadsToTerminalFailure` |
| 失败沿传递链跳过 | `TestFailurePropagation_TransitiveSkip` |
| 进程内中断续跑（不重跑/计数不回退） | `TestResume_AfterInterruption_InProcess` |
| 快照跨"重启"续跑 | `TestResume_AcrossRestartWithSnapshot` |
| 重复触发幂等 | `TestIdempotent_RepeatedTrigger` |
| 并发触发唯一终态 | `TestIdempotent_ConcurrentTriggers*` |
| 高并发压力（无漏跑/重复跑） | `TestStress_ManyWorkflowsAndTriggers` |
| 终态无出边 / 序列无回退 | `TestStateMachine_*`、`TestEventSequence_NoRollbackAfterRetry` |

所有用例都通过 `WithEventSink` 把任务状态迁移与触发依据打印到测试日志，
可用 `go test -v` 观察完整决策链。

## 8. 包内文件一览

- `errors.go`：可区分的提交期错误与终态/未找到哨兵错误
- `state.go`：状态枚举与合法迁移表
- `task.go`：任务定义、重试策略归一化与退避计算
- `event.go`：全序事件日志与订阅
- `snapshot.go`：快照与 JSON 序列化
- `validate.go`：提交期图校验（重复 / 缺失 / 环）
- `engine.go`：`Trigger` / `Resume` 入口与幂等归属
- `scheduler.go`：唯一调度循环（就绪、槽位、退避、收敛、中断排空）
- `scheduler_helpers.go`：依赖释放、失败传播、收敛判定、中断处理
- `runtime.go`：引擎与工作流运行时状态、提交、快照、恢复
