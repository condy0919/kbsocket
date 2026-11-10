# 连接线协议、顺序窗口与授信原语

本文记录 [roadmap](roadmap.md) 的 M4.2 原语及后续生命周期接线。`kbsocket/connection/` 的这些原语提供有界、单 owner 的编解码、身份分配、发送顺序、接收重排及物理 RQE 授信账本，本身不提交 WR。M5 已增加 [连接建链与导入缓存](connection-handshake.md)、[Eager Connection](eager-connection.md) 和 [Peer 授信撤回协调器](peer-credit-control.md)；稳定应用 ABI 与完整资源装配尚未交付。宿主负责把可信的 post、CQE、Lease 归还和协议证明接入对应账本。

## Wire v1 格式

[`wire_frame.hpp`](../kbsocket/connection/wire_frame.hpp) 固定 IMM 数值布局如下。`imm_data` 直接使用这个 64-bit 数值，不额外执行 `htonll`；payload 内多字节字段逐字段使用大端编码，不发送宿主 C++ 结构体布局。

| IMM 位段 | 字段 |
| --- | --- |
| 63..60 | `WireFrameType`：DATA=0、DATA_WITH_CREDIT=1、CONTROL=2 |
| 59..56 | Flags：数据帧低三位为 First/Last/Urgent；控制帧低三位为子类型；bit 3 为扩展身份 |
| 55..48 | 短 generation，取值 1–255 |
| 47..32 | 短 destination slot，取值 0–65535 |
| 31..0 | 方向内 SN；CREDIT/RST 固定为 0 |

完整 token 是 32-bit slot 与 32-bit generation，generation 不能为零。任一字段超出短格式范围时，payload 前置 8 B `BE slot32 + BE generation32`，IMM 中短 slot/generation 均置零。解码拒绝短/扩展字段同时带身份，也拒绝把能用短格式表达的 token 放进非规范扩展帧。高代际不会截取低 8 位。

所有 WR 的 payload 总长仍不得超过 4096 B，头部、数据和所有 SGE 长度一起计入，没有补齐到 4096 B 的要求：

| 数据帧 | 协议头字节 | 最大业务数据 |
| --- | --- | --- |
| 短 DATA | 0 | 4096 B |
| 扩展 DATA | 8 | 4088 B |
| 短 DATA_WITH_CREDIT | 8 | 4088 B |
| 扩展 DATA_WITH_CREDIT | 16 | 4080 B |

两类数据帧都要求非空业务数据。捎带头是一个大端 `max_credit_seq:uint64`，仅更新数据类物理授信，epoch 继承当前 Session 的握手绑定；更换 epoch 必须建立新 Session，不能让旧捎带帧进入新 epoch。`First/Last` 只被编解码保留，完整消息分片状态机尚未实现。

控制帧在可选的 8 B 身份扩展之后使用以下精确 body，不接受尾随字节：

| 子类型 | 编号 | body 顺序与长度 |
| --- | --- | --- |
| CREDIT_UPDATE | 0 | 48 B：六个大端 uint64，依次为 session、credit_epoch、max_credit_seq、max_control_credit_seq、max_frame_seq、max_stream_bytes |
| FIN | 4 | 8 B：大端 session:uint64；FIN 的终结 SN 位于 IMM |
| RST | 5 | 12 B：大端 session:uint64、非零 reason:uint32 |
| RNDV_RTS | 1 | 64 B：当前路由 session 和原事务完整身份、总长度；IMM 携带原 SN |
| RNDV_GRANT / DONE | 2 / 3 | 104 B：RTS 公共字段及 Arena/Grant 身份、目标地址；精确布局见 [RNDV 协议](rndv-protocol.md) |

`max_frame_seq` 是累计序号独占上界，最多为 `2^32`；`max_stream_bytes` 是累计业务字节独占上界，不是一次更新新增的字节数。数据/控制物理授信上界独立，不能把控制保留额度无条件交给任意 Peer 使用。普通 DATA 不能使用 `UINT32_MAX`，该 SN 为最后一个 FIN 保留。

`EncodeWireFrame` 将头部和数据写入调用方连续输出区间。`EncodeWireHeader` 只写头部并返回 `{imm_data, payload_bytes, header_bytes}`，不读取或复制业务数据；短 DATA 的 `header_bytes=0`，可传空输出区间。宿主可用一个注册 header SGE 加原有 Lease 数据 SGE 组成 WR，纯控制帧的 header 就是完整 payload。两接口在校验失败或输出不足时都不改写输出，输出区间不得覆盖输入数据。它们不注册内存、不持有 Lease，也不延长 DMA 生命周期。

`DecodeWireFrame` 的输入必须恰好截取到 CQE 的 `completion_len`，返回的 `frame.data` 借用原 payload。`ValidateWireIdentity` 比较完整 `WireReceiveContext`、token，以及控制帧的 session/credit epoch。完整来源、runtime、接收域 incarnation 和 Session 必须由宿主已协商并校验的路由事实提供；8 B 扩展身份本身不携带这些事实，此函数也不实现认证或握手。不得用“预期 context”冒充未经验证的实际来源。

`WireSessionBinding.credit_epoch` 绑定**对端接收域**广告的授信 epoch，用于校验入站 CREDIT_UPDATE 以及解释捎带授信；它不是本地物理 RQE 被消耗时使用的 epoch。M5.1 在 HELLO 提前交换该值，READY 重复确认，所以路由在收到首包前就有完整绑定。两端 epoch 可以不同，同一个 PeerRuntime/接收域的多个逻辑连接仍共用一个 SenderCreditAccount，不能每次握手重建额度。

## Token 与发送顺序

[`ConnectionTokens`](../kbsocket/connection/connection_tokens.hpp) 在一个接收域 incarnation 内分配唯一 `{slot,generation}`。配置最多 `1 << 20` 个槽，slot 超出短格式范围时自动交由 wire 扩展格式表达。退休后 generation 递增；达到 `UINT32_MAX` 的槽退休后永久退出空闲表，不回绕。相同对象只能成功 `Open` 一次，`Close` 后不能重新打开；宿主同样不能为同一域新建另一张表来重置身份。`initial_generation` 只配置首次建表起点，不是复用旧 token 的恢复开关。

`Retire` 仅退还路由身份，不撤销 Peer credits、远端 Grant 或旧帧。令牌表须活到全部路由使用者退出；旧授权的归属和 tombstone 由对应账本继续保留。

[`TxSequenceWindow`](../kbsocket/connection/tx_sequence_window.hpp) 将一个 Session 的累计序号与累计业务字节同时限制在接收端授权之内：

- `Update` 按完整 Session 接受累计 `max_frame_seq` 与 `max_stream_bytes`，分别取单调最大值，重复或倒序更新不额外生成额度。
- `Reserve` 一次预留最多 256 个帧计划，先为全部帧确定 SN。有未决批次时拒绝新批次，不允许后续帧越过未提交后缀。
- `Resolve(ticket, accepted_prefix)` 只推进可信已接受前缀；`pending()` 保留未接受后缀及其原 SN。重新提交该后缀前重新取得必要的物理 credits 和 SQ 票据，不能重放已经接受的前缀。
- 只有整批从未接受时才可 `Cancel`。部分接受后必须继续处理原后缀或显式失败；接受范围未知时以 `nullopt` 结账并永久停止新准入，不能猜测哪个 SN 可以复用。
- FIN 只可在批尾，业务字节数为零；被接受后停止后续准入。普通帧不占最终保留 SN，SN 不回绕。`Fail` 后仍允许为此前已经发生的可信接受结账，但不会重新开放发送。

该组件不拥有 payload、不消费 TX CQE，也不改变远端物理授信。`initial_seq` / `initial_bytes` 只设置一次 Session 的初始账本；不能借由重建同 Session 账本跳过旧未决责任。

## 重排、应用持有与 EOF

[`ReorderWindow`](../kbsocket/connection/reorder_window.hpp) 默认窗口 64、驻留预算 256 KiB、缺口超时 1 s；窗口上限 4096，描述符在 `Open` 时预分配。它接受 `raw::RxBufferLease` 或 M4.1 的 `memory::BufferLease`，保留原 payload 视图，不复制业务数据。宿主先验证 CQE、payload 区间、完整 Session 及底层 Lease；复制描述符不增加真实内存所有权。

`Push` 成功才接管 Lease。旧 Session、重复 SN、越窗、错误类型、重复持有同一 Lease 等错误均不接管输入，由调用方按真实所有权清理；不能因某个描述符副本被拒绝就再次归还窗口已经持有的同一个 Lease。越窗、超字节上界和 FIN 冲突会使当前流显式失败。`kValidatedRmaDone` 只提供将已验证大包放入同一顺序窗口的入口，不代表 DONE 协议、Grant 撤权或 DMA 证明已经实现。

M6.3.3 增加 `ReserveRndv`：在 RTS 准入时占用共同 SN 和完整业务字节额度，返回单调代次 reservation；没有目标 Lease 的 pending 项阻止后继 Eager/FIN 交付。`CompleteRndv` 只接受相同 session/SN/长度及有效 Lease 的可信完成项，原位转为可重排数据，不重复扣字节。停止或失败后的 `AbandonRndv` 仅删除占位元数据，不终结远端 Grant，也不增加消费前缀。见 [完整契约](rndv-protocol.md)。

M6.3.5 增加 `GrantHandle` 借用分支，只允许实际 Borrowed Grant 的精确只读范围作为 `kValidatedRmaDone` 进入窗口。`ValidateDelivery` 核验真实 Delivered receipt 与完整 frame/Lease/span；[接收协调器](rndv-receiver.md) 的 ReturnGrant 先真实归还目标，再释放窗口 receipt。Grant payload 不能通过物理 RX 或 Slab 的普通归还路径释放。协调器对 GrantManager 和窗口独占绑定，绑定期间禁止关闭依赖。M6.3.7 已提供 [实际 RNDV 接收分发和混合读取](rndv-rx-ingress.md)：EagerRxStream 可绑定同一 Receiver 处理 Grant 短读与 Lease 归还，所有 stream 都须取得窗口唯一读者绑定。`observed_prefix` 记录连续已收到/占位前缀，Pending 也可以推进；它与交付、消费前缀独立，不恢复授信，只用于防止乱序 RTS 提前耗尽前序资源。

`Poll` 每次最多交付 `min(output.size(),64)` 个连续帧。交付后 Lease 由应用/宿主持有，窗口仍以 `ReorderReceipt` 记录驻留责任。只有真实 Lease 已归还、应用不再访问后，才能 `Release(receipt)`；该调用不会代替 `ReleaseCpu` 或 `RxBufferPool::Release`。迟到/重复 receipt 不得释放新一轮槽位。

窗口公布两个累计上界：

- `max_frame_seq = min(2^32, released_prefix + window)`。只有连续归还前缀推进才扩大序号上界；即使 `rcv_next` 已前进，仍由应用持有的槽也不重新授出。
- `max_stream_bytes = consumed_bytes + max_resident_bytes`，发生整数上界时饱和。收到的数据与尚未归还的驻留字节分别记录，不因 Poll 交付就提前恢复字节窗口。

缺口从“已有后续缓冲帧但当前 SN 缺失”，或存在尚未完成的 RNDV 占位开始计时，要求传入单调的 `now_ns`。到期后失败不可被迟到帧消除；失败后仍可交付已经验证的连续前缀。FIN 同样进入重排窗口，只有 FIN 及此前连续数据都已归还，且流没有失败，`eof()` 才成立；Poll 看到 FIN 本身不等于应用已经读尽数据。

停止或失败后可调用 `Drain`，按最多 64 个扫描槽的预算交出待清理描述符，`discarded=true` 不得作为有序业务或正常 EOF。由于扫描有界且可能遇到空洞，一次返回 0 不等于全部清空；按 `buffered_frames` 继续推进。Drain 不抢回已交付给应用的 Lease，也不暴露未完成的 RNDV 目标；清理者仍须完成真实归还和 receipt 归还。`Close` 在存在 pending、缓冲或已交付责任时拒绝完成。

## 物理 RQE Credits

[`ReceiveCreditDomain`](../kbsocket/connection/credit_account.hpp) 按 `{PeerRuntime, ReceiveDomain incarnation, epoch}` 记账，同 Peer 的逻辑连接共用一个授信关系。默认域容量 8192、控制保留 512、基础暖窗口 8、Peer 表容量 1024；显式上限分别是容量 `1 << 20`、Peer 表 65536。数据与控制是共享 JFR 上的软件预算，没有硬件独立分区。

F 是可授出的已 post 槽，G 是已经授出的承诺，U 是尚未初始 post 或已消耗但尚未补投的槽。正常归属下，数据类和控制类分别保持 `F + G + U = 对应配置容量`；基础暖窗口、弹性池只是数据类的进一步分账。

| 事实与接口 | 账本变化 |
| --- | --- |
| 初始 post 确定接受后 `RecordInitialPosted` | 对应类别 U→F；未接受部分继续为 U |
| `Admit` | 从数据弹性 F 划定该 Peer 基础暖窗口，不额外创造物理槽 |
| 发布前 `Grant` | F→G，返回该类别累计授信上界；发布失败不擅自撤回可能已暴露的授权 |
| 消耗 RQE 的可信 CQE，调用 `Consume` | 原 Peer/类别 G→U，返回唯一 `RxCreditReceipt` |
| 实际 Lease 归还后 `Release(receipt)` | receipt 从 Held 转为可补投，仍为 U |
| 补投确定接受后 `RecordRefillAccepted` | 被接受 receipt 的 U→F；未接受、接受未知或仍 Held 的项不恢复 F |

物理 receipt 与重排 receipt 是不同责任，宿主须在预分配元数据中保存对应关系。旧逻辑 Session 的合法迟到帧仍消耗原 Peer/epoch 的物理 RQE；在丢弃内容时也不能跳过物理扣账。`FreezePeer` 停止新 Grant，但允许既有授权的迟到消耗结账。

无法归属的 CQE 调用 `ConsumeUnknown`：减少可观测的物理 posted，记录 `unattributed` 并冻结整个域，不猜测哪份 G 已被使用。此时保留此前分类账和未归属差额，不以错误分类恢复额度。未知 receipt 可以归还 CPU 持有，但不能通过普通 refill 路径重新授信；当前没有人工解除冻结的接口。

`Consume` 只用于已经发生的真实 RX，不是查询额度的试探接口。若真实 RX 无法成功结入原 Peer/类别，包括 G 不足、Peer 已退休、身份无效或无法确定类别，宿主必须在实际来源接收域调用 `ConsumeUnknown`，扣除物理消耗并冻结；不能因 `Consume` 返回错误就遗漏该 RQE。若连未归属记账都无法完成，继续保留该域及全部依赖，不能恢复准入。对 refill 接受范围未知等不能推断 RX 数量的情况，使用 `Freeze()` 仅封禁新准入/授信，不伪造接收或补投证据；已知事实仍须继续结账。

`SenderCreditAccount` 对同一个授信关系维护数据/控制两组累计上界、spent 和当前 reservation。`Reserve` 先扣可用额度；可信 partial post 只将接受前缀计为 spent，并解除未接受后缀 reservation。接受范围未知时把整批保守记为 spent，停止新准入，并将本 epoch 的 `accepted_known` 永久置为 false；这个上界不能作为授信撤回的确定接受量。TX 完成、超时和逻辑连接关闭都没有增加远端 credits 的接口。重复 CREDIT 更新只取累计最大值。

M6.3.4 增加未来控制票据 `ReserveControl/ActivateControl/ReleaseControl`：从同一 Peer 共账保留 GRANT/DONE 的额度，避免等待 WRITE 时占用唯一即时 post 预约。最多 256 份，ReserveControl 始终留下一个 CREDIT 额度；普通 Reserve 不能使用已承诺额度。Activate 后只计入当前 reservation，零接受恢复原票据，已接受或未知接受消费票据。Stop 后仍可 Release/Resolve，但禁止新预约与激活；Close 和撤回停止快照都必须等待全部承诺结束。详细责任见 [通知发送器](rndv-notification-tx.md)。

数据额度耗尽时控制额度仍可单独预留，但必须确实获得控制授权并取得本地 SQ 资源。RNDV RTS 属于数据入口需求，必须走 data；codec、接收域扣账及独立通知发送器已支持此分类，GRANT/DONE 使用 control，Connection/Runtime 的 RNDV 装配尚未接入。M5.3.7 的独立 TCP 控制流仅用于整份 Peer 授信撤回，尚未承担运行中 CREDIT 的通用逃生或跨 Peer 公平发放；不能仅凭两个额度分类就宣称完整协议没有死锁。

`RetirePeer` 要求宿主同时证明发送端已停止、真实接受集合已结清、未使用授权已撤销，并等待该 Peer 所有接收 receipt 正常 refill 结账；失联、TTL 或空 CQ 不构成证明。退休后保留 tombstone 到域关闭，不复用原表项和旧 epoch。相同 Peer 重新准入要求更高 epoch，且旧关系已经退休。Sender 重开也要求同 Peer/域且 epoch 严格增加，这不代替 Receiver 的终结证明。

M5.3.7 的 `PeerCreditControl` 独占绑定 Sender 共账和本端接收 Peer，通过独立已认证控制 fd 交换停止后的最终 DATA/CONTROL spent。成功 Open 即停止共账新 Reserve、冻结本端 Peer 新 Grant；已有 reservation 必须先按真实接受量结清，RX Consume/Release/实际 refill 继续运行。接收端消费量逐类等于远端最终 spent、`pending_rx==0` 且无 unknown 责任后才内部退休并 ACK。绑定期间拒绝普通 Sender Close、外部 RetirePeer 及整个账本的销毁清退；控制器 Close 只解除绑定，不恢复发送或授权。完整契约见 [Peer 授信撤回](peer-credit-control.md)。

接收授信域与 `EagerReceiveDomain` 采用唯一绑定，同一 Credit 域不能同时被第二个 Eager 域接管。绑定期间直接调用 Credit `Close` 返回 InUse，沿用原 Close 的冻结授信行为。须先成功关闭 Eager，或走下述整域核销入口，避免一份物理关闭证据清掉另一接收域仍在使用的账本。普通 Credit `Close` 只关闭元数据，不销毁真实 JFR，不终结 Posted DMA，也不替代 [M1 接收域关闭](design/kbsocket-design-v1.md#53-接收域安全关闭状态机m1-核心规范)。对象重开必须使用更大的接收域 incarnation，不能用于重置旧域的承诺。

M5.3.6 增加控制路径桥接 `EagerReceiveDomain::CloseDestroyed(const raw::ReceiveDomainCloser&)`，用于接收域已经物理销毁后清退冻结、已释放但未补投的 receipt。该入口要求 M1 协调器真实到达 `kClosed`，且对应同一 RxBufferPool 的本次成功 Open incarnation；同时核对 Eager 打开时捕获的 Credit 接收域 incarnation，阻止旧关闭证据作用于重开的域。即使残留 Posted 为 0，也必须实际完成 M1 关闭；空 CQ、Stopped、控制断开或 `retired_on_delete()==0` 都不能替代证据。

核销前整批核验：没有 Held 或正在 Dispatch 的条目，所有 queued receipt 身份仍有效且已经 Release，Credit 全局 Held 为 0，released 总数与这批 receipt 一致，且没有仍绑定的 PeerCreditControl。任一检查失败，两层元数据都保留。成功后永久清退本层与整个接收授信域的元数据，含已释放的 unknown receipt 和旧授权；过程不 post、不把 U 转为 F、不重授信，也不按猜测给 unknown 分配类别。等待 Lease、DeleteJfr/注销失败或其他未完成 M1 阶段都保留到重试成功；故障 CQ 的保留流程不支持此核销。授信撤回、来源退休和 Runtime 解绑仍是独立责任，不能由此接口自动推断。

## 宿主组合顺序与范围

控制路径创建真实 raw/M4.1 内存资源后，宿主先建立域授信账本并记录实际初始 post，准入 Peer；再为连接分配 token、绑定完整 Session，创建接收重排和发送顺序账本。经过可信握手发布 token、累计 credits 与连接窗口后，才允许该 Session 发送。

发送时先取得本地缓存、顺序窗口、Peer credits、物理 SQ 票据及 DMA 责任；编码头部和 SGE，最后调用 provider。每项 Reserve 都必须有明确的回滚或结账路径：确定未暴露时取消，可信部分接受按相同前缀更新各账本，接受未知时停止并保留全部可能暴露的依赖。`TxSequenceWindow` 保留的未接受后缀必须继续使用原 SN，而 `SenderCreditAccount` 的物理 reservation 需要为后续 post 重新取得。

接收时先按真实来源核验 wire 与授信归属，记录物理 RQE 消耗，再把合法数据/FIN 投入对应重排窗口。纯 CREDIT/RST 不占数据 SN。应用完成消费后归还底层 Lease、重排 receipt 和对应物理 receipt；只有实际 refill 的接受证据才能恢复可发放 credits。公布更新时分别使用物理授信累计值和重排窗口累计值，不能用 TX CQE 代替其中任何一种。

以下测试无需真机：

```bash
bazel test //kbsocket/connection:all --test_output=errors
```

单元测试覆盖 wire 字节格式、身份/序号边界、部分接受、乱序/慢读/FIN、数据与控制授信、receipt 复用和隔离；`eager_primitives_test.cpp` 使用 Mock Slab 与这些原语组合验证。M5 的独立测试另行覆盖非阻塞握手、Connection 和 Peer 授信撤回；上述原语测试不代表已经完成双机 Eager 真机验证、通用 TCP 信用逃生、自动 Credit scheduler、稳定应用 ABI 或完整 RNDV。
