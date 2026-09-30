# 持久化流程引擎

流程定义（`Definition`，不可变）与运行历史（`Event`，只追加）分离。
执行器不保存任何内存检查点：重建 `Engine` 时重放历史即可恢复全部进度。

## 存储

- `FileStore`：JSON Lines 文件，`Append` 后 `fsync`；新建执行器指向同一文件即恢复。
- `MemoryStore`：测试用。

## 执行模型

- `NewEngine`：重放历史；没有历史则生成 `run_id` 并写 `run_started`。
- `Advance(actions)`：按定义顺序推进。动作步骤执行外部副作用；等待步骤登记
  `wait_scheduled`（含截止时间）后阻塞。可重复调用，决策只依据历史。
- `DeliverResult(token, result)`：外部结果到达，完成等待或确认动作。
- `CheckTimeouts()`：截止时间到达后写 `wait_timed_out`。
- `VerifyAction(token, result, occurred)`：核实不确定动作。
- `Status()`：报告已完成步骤、当前步骤、未决等待和不确定动作。

## 关键语义

1. **超时不等于没发生**：`wait_timed_out` 后等待仍是未决项（`pending_waits`
   中 `timed_out=true`）；迟到结果照常完成它。超时与结果同时到达时，两种
   输入顺序收敛到同一最终状态（等待以结果完成）。
2. **动作确认丢失不自动重试**：外部调用已发出而本地确认未写时，动作处于
   `uncertain`，`Advance` 返回 `ErrActionUnknown` 且不会再次调用外部。必须先
   `VerifyAction`：核实确实发生则补结果继续；核实确认未发生（`occurred=false`）
   才允许重新执行一次。
3. **历史按实际顺序追加**：重复结果/重复确认只追加一条
   `external_result_duplicate` 审计事件，同一动作或等待不会生效第二次。
4. **崩溃恢复**：`SetCrashHook` 可在任一事件落盘后模拟进程退出。由于事件先
   `fsync` 再更新投影，重建后进度与崩溃前一致，`Status()` 清楚列出待处理项。

## 虚拟过程核对

`engine_test.go` 用文件存储 + 假时钟 + 外部服务模拟覆盖：

- 执行到一半退出（`action_invoked` 后崩溃）→ 重建 → 核实已发生 → 继续到完成，
  外部副作用总次数正确；
- 等待超时后结果迟到、超时与结果同时到达（两种顺序）、重复投递结果；
- 确认丢失且外部核实未发生 → 仅执行一次真实调用；
- 重复确认同一动作只产生一条 duplicate 事件。
