# device_info 设备能力工具

工具使用 gflags 解析 `--library`，其余位置参数为裸设备名；`--help` 在加载 URMA 前处理，无需本机安装 liburma.so。支持 `--library PATH` 和 `--library=PATH`。例如：

```sh
bazel run //tools/capabilities:device_info -- --library=/opt/urma/lib/liburma.so raw0 raw1
```

每台设备分组输出 GUID、传输模式及 TP 类型、资源数量、队列深度、消息/READ/WRITE/inline 大小、三类 SGE 限制、EID 容量、页大小能力、端口状态/速率/宽度/MTU、原子操作支持及大小、RM 顺序能力、乱序接收与乱序完成能力。大小输出为字节；端口使用属性数组索引标识，不推断物理端口编号。未知枚举保留数值，能力位图保留原始十六进制值。

输出代表 provider 上报能力，不代表当前剩余资源或实际可分配容量；零值不解释为无限制。工具继续使用 DeviceCatalog 的裸设备/RM_CTP 门禁，不用于枚举被门禁拒绝的设备。输出测试使用构造的设备属性，不需要 liburma.so 或真实硬件。

在仓库根目录构建与验证：

```sh
bazel build //tools/capabilities:device_info
bazel test //tools/capabilities:device_info_output_test
```

设备筛选与 EID 缓存规则见 [DeviceCatalog](../../docs/device-catalog-reference.md)；收发验证使用 [SEND 测试工具](../send_test/README.md)。
