# 四核 Calendar AllGather 回放

从 `wse-model` 目录执行以下命令。入口使用相邻的 `SST/env.sh` 和 SST Core 16.1 构建外部 element，使用相邻的 `network-viewer` 和 Node.js 打包离线页面。构建产物位于临时目录。

```bash
sst/calendar/run.sh --suite --out .wse_model_out/calendar
```

打开输出目录中的 **replay.html**，用案例菜单切换七次 SST 运行。初始画面停在首批 flit 飞行途中。选择逻辑参与者 B 可以追踪该源的全部分片，选择 B/f0 可以单独追踪首个分片。点箭头查看配对的发送与到达事件，用「±1 tick」逐拍前进。链路虚线显示当前预约，「故障」跳转到首次违约。每个案例的 `report.json` 中，`highlights` 给出扰动的事件 ID、拍号和相对于报告目录的回放链接。

| 案例目录 | 输入组合与验收 |
| --- | --- |
| A | 1×4，每人 64 B。每核收到 192 B 唯一远端数据，发布 256 B Tile。 |
| B | 使用 A 的负载，在 2×3 mesh 的 0、1、2、4 号节点形成 T 形树。B/f0 从 R1 同拍发往三个方向。四份 Tile 与 A 逐字节相同。 |
| C | 使用 A 的架构，每人 128 B。每核收到 384 B 唯一远端数据，发布 512 B Tile。 |
| late | D 晚报到 7 拍。观察其余三核的等待时间及首笔注入的平移。 |
| credit | R1 向 R0 的预约出口暂扣可用 credit。观察 C/f0 的原子分叉失败、在途分片的最终状态和失败代际排空。 |
| duplicate | R1 的本地落核接口重复交付 A/f0。第二份在第一份等待 Shared 写确认时被识别并去重。 |
| delay_ack | D 最后一个分片在 R0 的 Shared 写确认延迟 9 拍。停在第 40 拍，所有分片均已落核，R0 仍在等写确认。 |
| invalid | 2×2 四角同时参与，诱导子图有环。加载器在启动 SST 前返回 `V-TREE-ACYCLIC`，诊断保存在该目录的报告中。 |

## 单次运行与独立检查

```bash
cal=sst/calendar
inputs=("$cal/inputs/workload-64.json" "$cal/inputs/architecture-line.json" "$cal/inputs/deployment-line-64.json")
"$cal/run.sh" "${inputs[@]}" --reference-only
"$cal/run.sh" "${inputs[@]}" --case "$cal/cases/credit.json" --out .wse_model_out/credit
"$cal/run.sh" "${inputs[@]}" --case "$cal/cases/credit.json" --check-events .wse_model_out/credit/events.tsv
```

`--reference-only` 显示各源在对称 Shared Tile 中的位置和期望字节。`--check-events` 直接重读三份输入与 SST 原始日志。检查器配对每跳发送与到达，重建 FIFO、credit、原子分叉、去重和可见字节覆盖，并将发布内容与逻辑负载逐字节比较。违约案例通过验收时，报告同时给出 `passed: true` 和代际结果 `outcome: aborted`。不符合案例预期的观测使进程以非零状态退出。

每次 SST 运行输出 `events.tsv`、`reference.json`、`trace.json`（network-trace 1.0）、`replay.html` 和 `report.json`。原始日志的整数 ID 对应回放中的 `e<ID>`，`cause` 指向实际前序事件。`suite.json` 汇总八项验收。

## 输入约定

- **负载**指定逻辑参与者、贡献字节数、测试数据种子和拼接顺序。每个源的第 i 个字节为 `(seed + i) % 256`。消费者依赖本核 Tile 发布。
- **架构**用行列数展开规则 mesh。每个格点有一个 router 和一个可映射的核，节点按行编号。每对正交邻居构成双向链路，各方向有独立的 ingress FIFO 与 credit。本地落核端口接一个串行 Shared 写服务队列。时钟周期单位为 ps，传输宽度单位为字节，延迟单位为时钟拍，队列容量单位为 flit。
- **部署**给出逻辑到物理映射、对称 Tile 基址和每源的 opcode、线序小端 routeBits、端口预约。每行预约形如 `[1, [2, "local"], [6, 26]]`，表示 R1 在相对 BSP 原点的第 6、26 拍分别处理该源的 f0、f1，两次均须同拍接受向 R2 的转发及本地落核。时隙从文件读取。加载器校验每个分片的完整树、端口冲突、到达先后、FIFO 顺序、缓冲深度和 credit 周转。
- **案例参数**独立选择一个迟报者、一次出口 credit 扣留、一次本地重复交付或一笔延迟写确认。正常运行省略 `--case`。

## 参考时序与范围

`wsecalendar.fabric` 是一个 SST 时钟驱动的单时钟域 element。router、核、各方向链路的传输流水和 credit 返回流水在 element 内推进。原始拍号来自 SST 时钟回调。所有案例使用 epoch 1。本例的链路宽度包括逐 flit 头部，因而 64 B 贡献在直线上拆为 61 B 和 3 B，在六节点 mesh 上拆为 60 B 和 4 B。

第 1 拍绑定源数据、登记接收范围、预留目标容量并完成本地段写入。BSP 在最后一核报到的同拍建立原点。每拍先处理到期的 credit、链路到达和 Shared 确认，再服务预约。输入直到所有转发出口和本地队列共同接受后才释放，随后经过配置的返回延迟归还 credit。写服务时间包含确认，本例的延迟确认会占用对应 Shared 服务端口。

本参考实现将整轮预约均已通过作为发布的一个条件，确保一次时隙违约能使整个代际保持未发布。TMU 在最终预约和本核最后一次有效确认之后的时钟沿检查发布条件。排空检查在发布后观察到所有消费者依赖已满足、FIFO 和在途记录为空、所有 credit 已归还时完成。故障后，已经接受的 Shared 写继续确认，尚未转发的输入和之后到达的在途 flit 留下 drop 记录。缓冲、延迟和原子分叉策略是本轮显式参考参数。该实现覆盖四核验收范围，32 核 ROW → COL FFN 是后续阶段。
