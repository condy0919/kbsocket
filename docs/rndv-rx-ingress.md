# M6.3.7：RNDV 接收分发、背压与混合读取

[`RndvRxIngress`](../kbsocket/connection/rndv_rx_ingress.hpp) 连接真实 `EagerReceiveDomain`、[接收协调器](rndv-receiver.md) 和 [GRANT 通知队列](rndv-notification-tx.md)。RTS、GRANT、DONE 分别使用预分配的暂存槽，接收器和通知发送器均绑定到唯一 ingress。`EagerRxStream` 可显式绑定同一接收器，按共同序号读取 Eager、WRITE 结果和 FIN。

这是可独立装配的接收路径。应用 `Connection::Open` 仍拒绝提前声明 RNDV capability；[发送事务协调器](rndv-tx-coordinator.md) 已提供 GRANT 回调及后续 WRITE/DONE 推进，协商预算到实际资源的映射、Connection/Runtime 自动装配与完整应用关闭尚未完成。

## 接收责任与重试

接收域先按真实 CQE 来源登记核验并扣物理授信，将实际 `WireReceiveContext` 保存在 packet 中。Ingress 和混合流读取这份来源，不把预期路由复制成实际来源。RTS 消耗 data credit，GRANT/DONE 消耗 control credit；业务处理与物理 RQE 补投仍是两套独立账务。

宿主的接收回调将三类 RNDV 控制帧交给 `Accept`：

- 成功后返回 `EagerRxDisposition::kHeld`。Ingress 接管 packet，后续从 `Progress` 重试，回调不能再次归还。
- 失败时本次输入未被接管，由接收域的错误路径归还。不得再次提交已经交出的 packet 描述符副本。
- 接收器、目标缓冲区或通知额度不足时，不通过回调错误来表示普通背压；否则接收域会立即归还请求。只在已保留的责任上重试。

成功接收 RTS 后，固定 job 保存接收句柄，物理 RTS 即可归还。后续 job 分阶段取得目标、排入同一个 GRANT 通知并等待本地通知终结。每个成功阶段先记录结果，物理 Release 失败只重试归还，不重复 OnRts、发布或读取已经释放的 backing。DONE 处理错误可能已保存终结证明；该帧不重复执行 OnDone，后续由接收器清理。

宿主继续显式调用通知队列的 `Progress` 并分发真实 TX 完成；Ingress 不轮询 JFC、不代替通知 CQE，也不关闭整条共享通知队列。它只取消和释放自己创建的 GRANT 通知。

收到 GRANT 时调用配置的 `on_grant`。发送侧回调必须将完整身份与尚未完成的原 RTS 匹配，成功表示已复制或接管事务状态；返回 WouldBlock 必须无副作用。回调不能保留输入 span，也不能重入 ingress。本阶段提供分发与保留接口，WRITE pool 的完整发送事务编排仍待下一步。

## 乱序与容量

硬件 SEND 不保证顺序。为避免后序请求占光前序请求的元数据或目标，Ingress 按共同窗口的 `observed_prefix` 决定 RTS 准入：前面的 Eager/RTS 尚未收到时，后序 RTS 保留在物理 packet 槽；前序已进入窗口后才取得接收事务槽。`observed_prefix` 只表示连续接收或占位，不表示 WRITE 完成、业务交付或内存已归还；`rcv_next`、`released_prefix` 和累计消费仍独立推进。

已接收 job 按 SN 优先取得目标：较早 job 尚未取得目标时，较晚 job 等待；较早目标已经预留后，后续事务仍可并发发布和执行 WRITE。这样在只有一个目标缓冲区时，乱序 RTS 也不会形成“后序 DONE 等前序交付、前序又等后序释放目标”的循环。

Ingress 对尚未入窗、等待前序的 RTS 使用独立 `gap_timeout_ns`，第一次进度检查开始计时，同一缺口的重试不重置期限；缺口前缀变化时重新计时。前序 RTS 已暂存、只是在等待接收容量或应用归还时暂停该计时，不将正常背压误判为丢帧。已入窗的缺口继续由共同窗口检查。单次 `Progress` 最多扫描配置预算个 packet/job 槽，游标跨调用轮转；背压不计成有效进度。接收器内部仍按自身固定容量有界推进。

RTS、GRANT、DONE 的暂存容量独立，阻塞的 GRANT 不占用 DONE 保留槽。宿主必须按协商及授信限制配置在途帧数、暂存容量和字节预算，不能把可编码的协议上限当作资源已准备。暂存满槽是显式逻辑流失败，不丢弃帧后假装可以重试；也不会自动扩容。物理 Release 仅进入待补投，实际 refill 成功后才恢复可授出 RQE。

## 混合流读取与关闭

`EagerRxStream::Open` 可传入匹配的 `RndvReceiver`，核对同一 GrantManager/窗口/路由关系。所有 stream（包括默认 Eager 模式）都独占共同窗口的读者绑定，防止另一个 Eager-only stream 取走 Grant 却没有归还路径。

复制读取和只读 Lease 读取都保留当前 delivery 与偏移。未完成 RTS 阻止后序 Eager/FIN；WRITE 结果取得可信 DONE 后才能进入同一交付顺序。原始 RX 归还接收域，Grant 交给 `ReturnGrant` 同时归还目标和共同 receipt，不再次释放该 receipt。只有完整消费后才恢复窗口预算；FIN 按序消费后才产生 EOF。

Cancel 使整个逻辑接收流失败，停止新 RTS/GRANT，仍接受匹配的迟到 DONE。已发布目标隔离，应用持有的只读 Lease 不被 Drain/Close 抢回；通知源是否终结仍由本地 SEND CQE 决定。Close 在 packet、通知 job 或接收端 Grant 尚有责任时返回 Retained，宿主须继续推进、归还视图和提供真实终结证据。接收器及通知队列在 ingress 绑定期间拒绝提前 Close。

没有剩余物理或窗口交付责任的 stream 可以先解除绑定；尚未终结的已发布目标仍由接收器保留。故障清理阶段必须继续保留合法 DONE 的路由及全部底层依赖，不能借控制断链、空 CQ、取消成功或析构来推导 DMA 已结束。

## 验证范围

测试使用真实 Slab、Grant、重排、物理 Credit、RX buffer、TX ledger 与通知组件，URMA 注册/收发由 Mock 提供。覆盖乱序与单槽背压、控制帧独立暂存、通知终结、真实来源拒绝、混合短读/Lease/FIN、取消与迟到 DONE、依赖绑定及热路径零分配。测试计数见 [roadmap](roadmap.md)。UB 真机及完整应用 WRITE RNDV 端到端测试未执行。
