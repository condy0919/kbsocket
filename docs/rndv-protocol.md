# M6.3.3：WRITE RNDV 线格式与共同序号占位

本阶段提供 [`wire_frame`](../kbsocket/connection/wire_frame.hpp) 的 RTS/GRANT/DONE 编解码、[`rndv_protocol`](../kbsocket/connection/rndv_protocol.hpp) 的路由和事务身份适配，以及 [`ReorderWindow`](../kbsocket/connection/reorder_window.hpp) 的待完成占位。已有 Eager/FIN/CREDIT/RST 编码不变。Connection/Runtime 尚未启用自动 RNDV；当前 Eager 入口仍拒绝 RNDV 业务处理。

## 固定格式与方向身份

三类帧使用原有 CONTROL 编号 1/2/3，IMM 的低 32 位均为原 WRITE 事务的 SN，最高 SN 留给 FIN。外层短/扩展 token 定位当前消息的目的连接，payload 中的原事务 token 则标识 WRITE 接收端；GRANT 回程的 session 和 token 可以与原事务不同。

控制 body 使用大端字段，外部可再前置原有的 8 B 扩展路由身份；不使用 provider 原生结构的内存布局。

| 帧 | body 长度 | 字段顺序 |
| --- | --- | --- |
| RTS | 64 B | 当前路由 session64、原 sender_runtime64、receiver_runtime64、receive_domain64、原 session64、原 token.slot32/generation32、transfer_id64、总长度64 |
| GRANT / DONE | 104 B | RTS 公共字段，再加 Arena id64/generation64、Grant slot32、保留零32、Grant generation64、目标地址64 |

必须身份与长度非零，slot 和普通 SN 可为零；Grant generation 使用实现中的 64 位。精确校验长度、保留零位、非 RNDV 帧的空闲字段和地址区间溢出。语义适配层进一步要求 Grant slot 小于 `GrantManager::kMaxWindow`。GRANT 引用已认证的 Arena/Slab 目录；段描述、token 权限及 Jetty 导入绑定仍由已有缓存和宿主核验，不把网络字节直接解释为 `urma_seg_t` 或本地指针。

`BuildRndvRts` 和 `BuildRndvGrant` 只构造值对象，不发布授权或预留物理授信。任何可能发送 GRANT 的操作前都必须先执行 `GrantManager::CommitPublication`，所有可能暴露授权的发送路径都遵守此顺序。GRANT 的路由必须来自认证握手中的反向目的信息，不能从原事务身份推导另一端的 session/token。

`ValidateRndvRts/Grant/Done` 同时校验实际 RX 来源、当前方向的完整路由绑定，以及本地预期的原事务/Grant 身份。`observed` 必须来自可信来源登记，不能复制预期值充当接收事实；原事务身份匹配也不能替代认证。验证器返回值不持有 Grant、导入票据或应用 Lease。

## DONE 的成功门禁

`BuildRndvDone` 只接受真实 `WriteRndvEngine`，或 `WriteRndvPool` 加当前事务句柄；内部调用其 `DoneEvidence`。未提交完、在途、失败、取消、未知接受或失效句柄均不能构造成功通知。即使取消后所有已提交 WR 最终成功，通用 `TerminalEvidence` 也不能通过此入口。成功构造取得的是当时的快照，后续取消不能撤回已经交给宿主的通知。

编码/身份校验本身不证明远端 DMA 已终结。收到合法 DONE 不会自动构造 `GrantTerminalEvidence`、解除 Grant 或交付目标；接收状态机仍须确认当前已发布 Grant、可信发送端遵守逐 WR 成功与完整覆盖契约，以及取消状态。也不能从单独的网络计数、本地 CQE 或空 CQ 推导上述事实。未完成证明的 Grant 继续隔离保留。

成功 WRITE 后的 DONE 仍须通过 SEND_WITH_IMM 提交，并取得真实控制授信、SQ 票据和注册源责任。M6.3.4 的 [通知发送器](rndv-notification-tx.md) 已提供独立的有界提交与退休组件；单独构造帧仍不等同于对端收到通知。接收域已按 RTS 使用 data 授信、GRANT/DONE 使用 control 授信扣除真实 RQE 消耗；物理 refill 与逻辑序号释放保持独立。

## RTS 占位与有序交付

发送方用同一个 `TxSequenceWindow` 将 RNDV 作为一个总长度帧预留 SN，与 Eager 共用累计序号/字节上界。RTS 的可信接受前缀决定该逻辑 SN 的接受；GRANT 和 DONE 不额外推进数据序号，也不重复计入业务字节。

接收方验证 RTS 后调用 `ReserveRndv(session, sn, bytes, now)`：

- 占用一个窗口槽与全部业务字节额度，返回带对象、槽和单调代次的 reservation；不持有目标内存，不发布 Grant。
- 同 SN 的重复 RTS、Eager、直接 Push DONE 或冲突 FIN 不能替换占位。后续 Eager/FIN 可以乱序驻留，但 Poll 不能越过未完成项。
- 即使没有后续帧，待完成占位也计入缺口超时。超时显式失败，不产生 Grant 终结或目标复用证明。

临时 Grant 资源不足时保留有界待处理请求和原 reservation，重试准入；不能在活跃连接中撤销占位后重用 SN。只有获得可信 DONE、完成 Grant 责任校验和合法目标 Lease 后，才用 `CompleteRndv` 将原占位转换为 `kValidatedRmaDone`。它要求原 session/SN/长度和有效且不重复持有的 Lease，成功才接管描述符，不再次扣字节额度。

目标进入连续前缀后，仍按 Poll → 应用消费/真实归还 → Release(receipt) 扩大窗口。停止或失败后，`AbandonRndv` 只清理未完成占位的元数据；不前进累计消费或释放前缀，不释放远端 Grant/backing。Drain 不会交付待完成项；Close 在 pending、buffered 或已交付未归还项存在时拒绝完成。reservation 和 receipt 在对象 Close/Open 后不复用代次。

## 验证与后续接入

无真机测试覆盖双向不同 session/token、短/扩展编码、错误来源与完整 Grant 身份、取消后的 DONE 禁发、旧事务句柄、Eager/RNDV/FIN 共同窗口、数量/字节额度和零分配。Mock 测试中的显式 DMA 事实不代表 UB 真机端到端验证。

M6.3.4 已绑定 RTS 发送 SN、未来 DONE 额度、本地通知槽、Grant 发布及导入引用。后续仍须完成接收端可信 DONE 终结、Grant 只读借用与共同接收流归还、能力协商及 Arena 描述交换，随后接入 Connection/RawDomain/Runtime 和应用入口。
