# 双端 RM_CTP SEND 硬件测试

这是单设备、单 jetty、单 owner 的正确性测试。TCP 仅交换端点、同步接收就绪和校验完成；payload 通过真实 URMA RM_CTP SEND 传输，不走 TCP。发送端使用 JettyPool、AttemptLedger、TxSender 和 TxCompletionProcessor；接收端向共享 JFR 投递已注册 buffer，并轮询 RX JFC。

## 编译与运行

```sh
bazel build //tools/send_test:send_test
```

两台机器需要可用的 raw UB 设备、驱动、匹配的 liburma.so，以及可互通的 EID。TCP 控制地址和 UB EID 是两套独立地址；`--address` 使用数字 IPv4。用已有 device_info 工具查看本机设备与 EID 索引。

先在机器 A 启动接收端，替换设备名、EID 索引和库路径：

```sh
./bazel-bin/tools/send_test/send_test \
  --server --address=0.0.0.0 --port=18515 \
  --device=RAW_DEVICE_A --eid_index=0 \
  --library=/opt/urma/lib/liburma.so \
  --bytes=4096 --messages=1024 --batch=32 --timeout_ms=60000
```

再在机器 B 启动发送端，将 `192.0.2.10` 替换为机器 A 的 TCP 地址：

```sh
./bazel-bin/tools/send_test/send_test \
  --address=192.0.2.10 --port=18515 \
  --device=RAW_DEVICE_B --eid_index=0 \
  --library=/opt/urma/lib/liburma.so \
  --bytes=4096 --messages=1024 --batch=32 --timeout_ms=60000
```

两端必须具有相同的 bytes/messages/batch；设备名、EID 索引、库路径和超时可不同。角色互换后再运行一次，可验证相反方向。TCP 端口仅用于一次测试连接，不实现认证或正式建链服务。

## 参数与判定

- `bytes`：每条 WR 的 payload 长度，范围 8–1048576 字节，还必须不超过设备 max_msg_size。前 8 字节是消息序号，其余内容由序号和偏移确定。
- `messages`：总消息数，必须大于 0，可超过队列深度。默认 1024。
- `batch`：每轮最多 1–256 条，默认 32。工具将 TX/RX 深度设为 batch，TX CQ 深度设为 batch+1，RX CQ 深度设为 batch，避免依赖宿主的默认深度 flags。
- `timeout_ms`：每个 TCP 控制操作和每轮完成等待的超时，范围 1–3600000 毫秒。服务端等待客户端连接也使用此超时。

每轮接收端先投递 RQ，发出 READY；发送端收到后提交 SEND。接收端按 CQE 的 user_ctx 定位 buffer，校验长度、消息序号、重复消息及所有数据字节，允许批内接收顺序不同。发送端等待所有 TX attempt 退休，并收到对端校验 ACK，才复用 buffer 进入下一轮。

两端都输出 `PASS` 且退出码为 0 才算通过。建议依次测试：

```text
--bytes=4096 --messages=1    --batch=1
--bytes=4096 --messages=1000 --batch=32
--bytes=4096 --messages=1024 --batch=256
```

最后一组需要设备支持至少 256 个工作队列槽和 257 个 TX CQ 槽。参数或设备能力不满足时显式失败，不自动裁剪。退出码 2 表示命令行参数错误，1 表示初始化、通信、校验或清理失败。

## 资源与失败语义

本工具使用非 inline、全 signal、无 token 验证的普通 SEND，注册段只授予本地访问，不交换远端内存地址。正常退出顺序为：数据路径排空、关闭发送器/账本、解除远端导入、销毁 jetty/JFR/JFC、注销内存、关闭 runtime、卸载库。

超时、post 部分成功、错误或边界 CQE 都导致测试失败；不重放、不自动恢复。若无法证明全部 DMA 已结束，Close 会拒绝清理；入口以 NoDestructor 保持会话和 buffer 到进程退出，交由进程/驱动回收，不能在这条路径上先 free buffer 或 Uninit/Unload。初始化中途失败也可能保留已创建对象至进程退出。报错中的 code 来自对应阶段的 errno、URMA 状态或内部错误枚举。

这是正确性 smoke test：逐批 READY/ACK 避免接收额度不足，因此不用于测吞吐、延迟或 RNR/EPOLLOUT 背压行为；暂不覆盖多远端、软件 flush、异步事件及故障恢复。TCP 控制等待有界，但驱动提供的同步创建/销毁调用仍受驱动自身行为影响。

## 无硬件测试

```sh
bazel test //tools/send_test:send_test_test
```

使用 gMock 替代 URMA 调用，使用真实 socketpair 检查控制协议。覆盖线格式、版本/参数拒绝、超时/EOF、数据损坏、发送端多批次、接收完成乱序和清理失败重试。这些测试不依赖本机 liburma.so，但不等价于真实设备验证。

## 注册失败诊断

工具把 URMA 日志转发到 stderr；增加 `--urma_debug` 可打开 URMA debug 级别。UDMA/UMMU 可能使用独立日志通道，仍需查看服务器对应日志。`register local memory` 表示 RegisterSeg 返回空指针，尚未进入 TCP 等待或数据发送。若 provider 未设置 errno，工具会明确标注 `without errno` 并使用工具侧 EIO，而不是打印误导性的 0；EIO 不代表已经定位到具体驱动故障。应结合 alloc token、segment grant、pin segment 等日志判断失败阶段。
