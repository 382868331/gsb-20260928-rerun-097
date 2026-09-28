# Go 持久化流程引擎示例

这是一个只使用 Go 标准库的本地持久化流程引擎，用事件溯源表达长期任务。

- `defs/approval.json`：流程定义，与运行数据分离。
- `history.jsonl`：所有运行共享的追加式事件历史，重启后只从事件重放状态。
- `external-invocations.jsonl`：模拟外部系统自己的幂等键台账。

## 恢复语义

动作执行记录为 `action_started`，外部调用返回后才写 `action_completed`。如果进程在两者之间退出，历史会保留“不确定窗口”：本地不知道外部副作用是否已发生。

本实现不承诺分布式意义上的恰好一次。重启后会用 `runID:action` 作为外部幂等键重试；模拟外部系统识别重复键，只执行一次副作用，但本地历史仍记录这次重复调用尝试。真实系统也应由外部接收方提供幂等键、去重令牌或可查询结果。

等待节点先持久化 `wait_started` 和截止时间。外部结果只有在当前节点确实等待它时才写 `external_result_accepted`；重复结果会写 `external_result_ignored`，不会再次触发后续动作。超过截止时间后重启，会依据历史中的截止时间进入超时分支。

## 运行

```sh
go test ./...
go run ./cmd/flow-engine demo --data demo-data
```

演示自动覆盖：

1. 首次通知发出后、本地确认前模拟进程退出。
2. 重启重放历史并重试，外部台账拦截重复通知。
3. 重复投递同一个外部结果，只确认一次。
4. 等待超过持久化截止时间，重启后进入超时分支。

手动命令：

```sh
go run ./cmd/flow-engine start --data data --run run-1
go run ./cmd/flow-engine resume --data data --run run-1 --result approval
go run ./cmd/flow-engine history --data data
```
