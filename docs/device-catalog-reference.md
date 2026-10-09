# Raw 设备与 EID 目录

[文档入口](README.md) · [当前代码流程](raw-transport-flow.md)


`kbsocket/transport/raw/device_catalog.*` 参考 UMQ 的初始化缓存方式，将设备发现与连接创建分开。该类只负责发现与查找，不创建 context、jetty 或注册内存，也不修改公共 C ABI。

## 使用顺序

1. 控制线程初始化 URMA；仅在需要 UB 后端时执行。
2. 向 `DeviceCatalog::Initialize` 传入非空、无重复的 raw 设备白名单。
3. 初始化成功后发布目录，使用设备名与 **实际 eid_index** 解析 `LocalEndpoint`；Initialize 返回 `std::expected<void, CatalogError>`，Find 返回 `std::expected<LocalEndpoint, CatalogError>`。
4. 后续资源准备层用端点创建 context，并核对 context 的实际 EID。连接使用准备好的资源 handle，不再次枚举。
5. 停止使用端点，安全终结所有硬件资源，最后结束 URMA 会话。

设备目录直接调用进程级 `UrmaApi`，构造函数不传函数表。测试用 `ScopedUrmaOverride` 安装测试替身，因此不需要硬件、provider 插件或 URMA 初始化。目录不拥有 URMA 会话，也不延长 `urma_device_t` 生命周期。

## 策略与边界

- 只对选中的设备查询能力与 EID。未找到指定设备、设备不支持 RM_CTP 或查询失败，整体初始化失败；不静默选择其他设备。
- raw 筛选要求 UB transport，并排除当前 URMA 约定的 `bonding_dev` 前缀。不读取私有 provider ops。该规则与当前 provider 命名约定相关，并非未来所有虚拟设备的通用识别机制。
- RM_CTP 能力检查仅证明设备报告支持，不能代替 context/jetty 创建与真实收发验证。
- 保存完整设备身份、EID 与实际 index。依赖 UB 在同一 CLAN 内保证 EID 唯一，按 EID 查找首次匹配即返回。设备重名仍返回 `CatalogErrorCode::kAmbiguous`。
- 没有 UMQ 的固定 256 项数组；存储按返回数量分配，数量和 index 受设备报告的 `max_eid_cnt` 校验。
- 初始化使用临时目录，全部成功后才发布；失败释放查询结果并允许重试。成功后禁止重复初始化，查询无分配、无 provider 调用。
- 设备按白名单顺序保存，每个设备的 EID 按实际 index 排序；设备名查找线性扫描设备，index 查找二分搜索，纯 EID 查找遍历设备及其 EID，首次匹配即返回。连接保存解析结果，查询过程不分配内存。
- 当前 URMA 的空 EID 列表也可能表现为 NULL/EIO，因此保留 `CatalogErrorCode::kQueryFailed` 和 provider 错误值，不把它误报成确定的“未配置 EID”。`CatalogErrorCode::kNoEids` 用于设备报告零 EID 容量，或接口明确返回非空指针和零数量的情况。
- `provider_error` 在枚举/EID 失败时为 errno，在 query_device 失败时为 URMA 状态码。
- 首版不支持热插拔或运行中刷新。目录存活期间，同一进程的使用者必须协调，禁止再次设备枚举或提前 uninit；URMA 重枚举可能释放卸载设备对象。缓存不会提供额外的对象固定保证。
- 目录仅在单次 URMA 会话中使用，不提供跨会话稳定 ID 或 generation。后续实现动态重配置时，需要单独设计设备句柄固定、资源退休及会话身份，不能只增加 TTL。


## 查询示例

```cpp
DeviceCatalog catalog;
auto initialized = catalog.Initialize({"raw0"});
if (!initialized) {
    return std::unexpected(initialized.error());
}
auto endpoint = catalog.Find("raw0", 7);
if (!endpoint) {
    return std::unexpected(endpoint.error());
}
// endpoint->device 与 endpoint->eid_index 供后续资源层创建 context。
```

目录自身不执行 Load/Init/Uninit/Unload，也不创建 context、jetty 或注册内存。缓存的 EID 是发现时快照，不代表此后的端口健康或远端可达性。

UMQ 当前也在初始化时缓存 EID；其按 EID 查找返回第一个匹配项，并将空列表与查询失败都当作没有可用 EID 继续初始化。这里采用显式白名单和失败回滚，拒绝设备重名，避免发布不完整配置；EID 查找依赖同一 CLAN 内唯一的约束。

## 构建与验证

在 kbsocket 根目录执行：

```sh
USE_BAZEL_VERSION=9.2.0 bazel test //kbsocket/transport/raw:device_catalog_test
USE_BAZEL_VERSION=9.2.0 bazel run //tools/capabilities:device_info -- raw_device_name
```

设备名由实际部署配置提供。工具仅依赖 URMA 头文件和系统动态加载接口，不链接 URMA core/common。运行时通过 `UrmaApi` 加载 `liburma.so`，仍需要部署匹配的 URMA 库、raw provider 和驱动；未禁用系统安装的 bonding provider 初始化行为。

单测覆盖稀疏 index、跨设备唯一 EID 查找、bonding/非 UB 排除、RM_CTP 门禁、查询失败和回滚、非法 provider 数据，以及重复查找期间不调用 provider、查询缓冲释放后快照仍有效。这是调用边界验收，不是 40k/s 建链性能证明。

能力查看工具的参数和输出见 [device_info 使用说明](../tools/capabilities/README.md)。目录之上的 context 创建与会话管理见 [URMA 加载与运行时](urma-runtime.md)。
