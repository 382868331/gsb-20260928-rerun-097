# 并行等待组（parallel wait groups）

`Kind: "group"` 步骤声明一个并行等待组，组内分支并发执行外部等待或先触发
外部 action。所有状态迁移仍然遵循引擎既有规则：**先写历史、再生效**。

## 继续条件

- `mode: all`：所有分支终态后继续；失败是否算数由 `count_failures` 决定。
- `mode: any`：任意一个满足条件的终态出现后继续。
- `mode: quorum` + `quota: N`：达到 N 个满足条件的有效结果后继续。
- 失败（显式失败、取消、超时）是否计入条件由 `count_failures` 明确声明。

决策后行为：

- `collect_remaining: true`：继续收集剩余分支结果；迟到结果记录到分支上，
  但决策快照（`selected_tokens`）冻结不变。
- `collect_remaining: false`：对未完成分支记录 `group_cancel_requested`。
  已产生但未核实的外部效果单独保留在 `UncertainEffects()` 中，
  不会因为分支不再被需要而消失。

## 唯一性与竞争

结果、分支超时、整组超时、取消在同一临界区裁决；每个组只会落盘一条
`group_decided` 与一条 `group_completed`。迟到输入仍以历史事件记录
（`detail: "late"`），但不会悄悄替换决策时选定的结果集合。

## 嵌套与重试

- 分支可通过 `subgroup` 嵌套；子组形成自己的唯一决策后，作为父组的一个
  分支以其 token 参与父组裁决。
- `RetryBranch(token, actions)` 对单个分支开始新的一轮（attempt+1）；
  组决策后重试返回 `ErrGroupDecided`。
- 分支 action 已触发但未确认时禁止重试：先 `ConfirmBranchAction` 核实。

## 可观测性与恢复

- `GroupStatus(stepIndex)` 展示每个分支的结果/终态、是否参与决策、
  是否仍在等待、是否已请求取消、是否有未核实外部效果。
- 在组决策前后保存并恢复运行状态：恢复后沿用已确认的决策与结果归属，
  截止时间按历史重放，已触发未确认的 action 绝不重新执行。
