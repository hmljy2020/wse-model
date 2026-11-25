# 四核 Calendar AllGather 回放

从 `wse-model` 目录执行以下命令。入口使用相邻的 `SST/env.sh` 和 SST Core 16.1 构建外部 element，使用相邻的 `network-viewer` 和 Node.js 打包离线页面。构建产物位于临时目录。

```bash
sst/calendar/run.sh --suite --out .wse_model_out/calendar
```

打开输出目录中的 **replay.html**，用案例菜单切换七个运行案例。初始画面停在首批 flit 飞行途中。选择逻辑参与者 B 可以追踪该源的全部分片，选择 B/f0 可以单独追踪首个分片。点箭头查看配对的发送与到达事件，用「±1 tick」逐拍前进。链路虚线显示当前预约，「故障」跳转到首次违约。每个案例的 `report.json` 中，`highlights` 给出扰动的事件 ID、拍号和相对于报告目录的回放链接。

| 案例目录 | 输入组合与验收 |
| --- | --- |
| A | 1×4，每人 64 B。每核收到 192 B 唯一远端数据，发布 256 B Tile。 |
| B | 使用 A 的负载，在 2×3 mesh 的 0、1、2、4 号节点形成 T 形树。B/f0 从 R1 同拍发往三个方向。四份 Tile 与 A 逐字节相同。 |
| C | 使用 A 的架构，每人 128 B。每核收到 384 B 唯一远端数据，发布 512 B Tile。 |
| late | D 晚报到 7 拍。观察其余三核的等待时间及首笔注入的平移。 |
| credit | R1 向 R0 的预约出口暂扣可用 credit。C/f0 在相对第 8 拍违约，R1 刷新诊断并调用 SST fatal，回放停在该拍。 |
| duplicate | R1 的本地落核接口重复交付 A/f0。第二份在第一份等待 Shared 写确认时被识别并去重。 |
| delay_ack | D 最后一个分片在 R0 的 Shared 写确认延迟 9 拍。停在 BSP 相对第 40 拍，所有分片均已落核，R0 仍在等写确认。 |
| invalid | 2×2 四角同时参与，诱导子图有环。加载器在启动 SST 前返回 `V-TREE-ACYCLIC`，诊断保存在该目录的报告中。 |

## 单次运行与独立检查

```bash
cal=sst/calendar
inputs=("$cal/inputs/workload-64.json" "$cal/inputs/architecture-line.json" "$cal/inputs/deployment-line-64.json")
"$cal/run.sh" "${inputs[@]}" --reference-only
"$cal/run.sh" "${inputs[@]}" --case "$cal/cases/credit.json" --out .wse_model_out/credit
"$cal/run.sh" "${inputs[@]}" --case "$cal/cases/credit.json" --check-events .wse_model_out/credit/events.tsv
```

`--reference-only` 显示各源在对称 Shared Tile 中的位置和期望字节。`--check-events` 直接重读三份输入与 SST 原始日志。检查器配对每跳发送与到达，重建 FIFO、credit、原子分叉、去重和可见字节覆盖，并将发布内容与逻辑负载逐字节比较。单独运行 credit 案例返回 SST 退出码 42。验收套件同时核对退出码、SST fatal 诊断、首个违约的位置和相对拍，匹配时报告 `passed: true`、`outcome: fatal` 和 `drain_cycle: null`。

每次运行保存 SST 导出的组件、端口、连线和线程分区图 `sst-topology.json`、进程输出 `sst.log`、每个物理节点的原始观测 `R*.tsv`、按拍合并的 `events.tsv`、`reference.json`、`trace.json`（network-trace 1.0）、`replay.html` 和 `report.json`。原始日志的整数 ID 对应回放中的 `e<ID>`，`cause` 指向实际前序事件。`suite.json` 汇总八项验收。每个可执行案例均以单、双线程运行；双线程结果在 `two-threads/` 下，相邻编号交替分配线程。可用 `--threads 2` 单独复现。

## 输入约定

- **负载**指定逻辑参与者、贡献字节数、测试数据种子和拼接顺序。每个源的第 i 个字节为 `(seed + i) % 256`。消费者依赖本核 Tile 发布。
- **架构**用行列数展开规则 mesh。每个格点有一个 router 和一个可映射的核，节点按行编号。每对正交邻居构成双向链路，各方向有独立的 ingress FIFO 与 credit。本地落核端口接一个串行 Shared 写服务队列。时钟周期单位为 ps，传输宽度单位为字节，延迟单位为时钟拍，队列容量单位为 flit。
- **部署**给出逻辑到物理映射、对称 Tile 基址和每源的 opcode、线序小端 routeBits、端口预约。每行预约形如 `[1, [2, "local"], [6, 26]]`，表示 R1 在相对 BSP 原点的第 6、26 拍分别处理该源的 f0、f1，两次均须同拍接受向 R2 的转发及本地落核。时隙从文件读取。加载器校验每个分片的完整树、端口冲突、到达先后、FIFO 顺序、缓冲深度和 credit 周转。
- **案例参数**独立选择一个迟报者、一次出口 credit 扣留、一次本地重复交付或一笔延迟写确认。正常运行省略 `--case`。

## 参考时序与范围

`sdl.py` 按架构行列数创建 `wsecalendar.node`，通过一条双向 `sst.Link` 连接每对正交邻居。1×4 有四个组件和三条链路；2×3 有六个组件和七条链路，含未映射负载的节点。每个节点只装入本核的源分片和本路由器的预约，管理自己的 Tile、Shared 队列、输入 FIFO 和出口 credit。回放的物理连线取自 SST 配置图。所有案例使用 epoch 1。64 B 贡献在直线上拆为 61 B 和 3 B，在六节点 mesh 上拆为 60 B 和 4 B。

启动和完成采用一棵以 R0 为根的控制树。路径先沿第一列向下，再沿目标行横向延伸。第 1 拍各核准备源数据和本地段；晚到者在指定拍报到。每个节点收齐子树就绪消息并完成本地准备后向父节点报告。根选择「收齐拍 + 树高 × link_cycles」作为共同原点，并沿树下发。直线案例 A 的原点是绝对第 13 拍；相对第 1 拍 R0 发送 A/f0，第 3 拍 R1 收到，第 6 拍 R1 同时落核并转发。

链路的基础延迟取 `link_cycles` 与 `credit_cycles` 中较小者，发送者按消息类型补足差值。flit 和控制消息逐跳耗时 `link_cycles`，归还 credit 耗时 `credit_cycles`。接收、credit 和 Shared 确认的 SST 事件优先级早于同拍预约。只有全部出口可接受时才执行分叉并释放输入槽。TMU 在整轮最后预约拍与本核最后一次 Shared 确认之后检查发布条件。节点在本地排空、credit 全数归还并收齐子树完成报告后向上报告，根记录最终排空拍。

硬预约缺少输入、credit 或本地容量时，发现问题的节点记录 epoch、相对拍、端口和分片并终止 SST。节点观测 ID 随 flit 和 credit 跨组件传递。双线程在终止瞬间可能处于不同的局部仿真时间，合并回放截至首次违约拍，原始分节点文件保留现场。
