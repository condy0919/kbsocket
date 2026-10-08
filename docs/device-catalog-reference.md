# Raw 设备与 EID 目录参考实现

`kbsocket/transport/raw/device_catalog.*` 参考 UMQ 的初始化缓存方式，将设备发现与连接创建分开。当前只实现发现与查找，不创建 context、jetty 或注册内存，也不修改公共 C ABI。

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

GoogleTest 使用 BCR 修订版 `1.17.0.bcr.2`，已显式加载 rules_cc，无需自动加载兼容选项。设备名由实际部署配置提供。工具仅依赖 URMA 头文件和系统动态加载接口，不链接 URMA core/common。运行时通过 `UrmaApi` 加载 `liburma.so`，仍需要部署匹配的 URMA 库、raw provider 和驱动；未禁用系统安装的 bonding provider 初始化行为。

单测覆盖稀疏 index、跨设备唯一 EID 查找、bonding/非 UB 排除、RM_CTP 门禁、查询失败和回滚、非法 provider 数据，以及重复查找期间不调用 provider、查询缓冲释放后快照仍有效。这是调用边界验收，不是 40k/s 建链性能证明。

## 动态加载层

`urma_api.hpp/.cpp` 参考 ubsocket 的函数表加载方式，使用 `decltype` 从 URMA 头文件取得准确签名。当前与 UMQ 的 URMA 符号表对齐，共加载 50 个必需符号，覆盖设备/context、JFC/JFCE/JFR、jetty 与收发、内存段、异步事件、日志、user_ctl、EID 转换和性能统计。UVS 的 `uvs_get_path_set` 属于独立 `libtpsa.so`，不纳入该表。部署库缺少其中任一符号时，加载整体失败。

`UrmaApi::Load()` 默认加载 `liburma.so`，也支持指定路径。全部符号解析成功才发布只读函数表；打开失败或缺符号时返回 `std::unexpected(LoadError)`，包含错误码和复制后的错误文本，允许重试。已加载时再次 Load 返回 `LoadErrorCode::kAlreadyLoaded`，不替换现有库。

```sh
USE_BAZEL_VERSION=9.2.0 bazel run //tools/capabilities:device_info -- --library /opt/urma/lib/liburma.so raw_device_name
```

加载标志与参考代码一致：`RTLD_NOW | RTLD_GLOBAL`。GLOBAL 允许 provider 解析核心符号；Unload 清空函数表并释放加载引用。

生命周期顺序是 loader → URMA init → catalog/硬件资源，清理时反向执行。loader 不自动调用 init/uninit，也不管理进程级 URMA 会话引用；控制线程需协调所有使用者。Load/Unload 不得与调用并发，退出会话前必须释放所有相关资源，不能在 Unload 后使用复制出的函数指针。

工具即使没有 liburma.so 也可编译和启动；实际查询需要运行时存在该库，缺库时明确返回加载失败。动态加载不能替代库本身，也不能保证任意版本 ABI 兼容。加载层测试通过 gMock 与测试二进制的链接包装拦截 dlopen/dlsym/dlclose/dlerror，覆盖完整加载、最后一个符号缺失时的回滚与重试、卸载清空函数表。不构建测试共享库；这些测试不证明真实 provider 的行为或 ABI 兼容性。

## 进程级调用入口与 gMock

业务使用 `UrmaApi::CreateJfr(ctx, cfg)` 等静态接口，全部 50 个 URMA 函数都有对应入口。生产启动顺序为 `UrmaApi::Load()` → `UrmaApi::Init()` → 创建资源/业务运行；退出时先停止调用并释放资源，再 `Uninit()` 和 `Unload()`。UrmaApi 直接持有进程级库句柄和唯一的常驻函数表；Load 使用局部临时表解析，全部成功后才发布。

`UrmaApi` 不自动计数业务资源或多 runtime 引用；进程的控制线程负责协调初始化和退出。没有加载或测试未填某个符号时，入口设置 errno=ENOSYS，并按签名返回 nullptr、URMA_FAIL 或 -1；void 接口只设置 errno。

测试依赖 testonly 的 `//kbsocket/transport/raw:urma_test_support`，包含 `testing/scoped_urma_override.hpp`。示例：

```cpp
UrmaFunctions functions;
functions.create_jfr = [](urma_context_t* ctx, urma_jfr_cfg_t* cfg) {
    return active_mock->CreateJfr(ctx, cfg);
};
test_support::ScopedUrmaOverride scope(functions);
EXPECT_CALL(mock, CreateJfr(&context, &config)).WillOnce(Return(&jfr));
auto* result = UrmaApi::CreateJfr(&context, &config);
```

`active_mock` 由 fixture 管理；完整例子见 `urma_api_test.cpp`。替换复制函数表并在析构时恢复，支持按栈顺序嵌套和异常退出。安装前及恢复前必须停止所有调用；owner 线程在安装后启动、在作用域结束前 join。mock 对象在此期间保持存活，相关资源在恢复前通过对应函数表释放。不同测试不能并发安装不同表。

替换期间 Load/Unload 返回 `LoadErrorCode::kInUse`，防止恢复到已失效的真实表。函数表在生产调用期间只读，不引入锁或内存分配。`--wrap` 仍只用于测试加载器本身，业务 gMock 测试不需要该链接选项。

## 进程退出与未停止的 worker

`UrmaApi` 的库句柄、函数表和测试替换计数均为 `constinit` 静态成员，保持平凡析构，不在进程退出时自动 dlclose。显式 Unload 前必须停止所有 URMA 调用。加载过程中尚未发布的句柄由局部 RAII 对象负责失败清理。

若上层不能确认 worker 已停止调用 URMA，不得从静态析构或 atexit 中调用 `UrmaApi::Uninit/Unload`。进程终止时由操作系统回收进程资源。需要受控退出时，先停止新操作、等待所有 URMA 调用退出、终结资源，再显式 Uninit/Unload；不要求所有 bRPC worker 都退出，但必须证明它们不会再进入 URMA。

这项修复只消除 kbsocket 加载器的自动清理。provider 自身的静态状态、退出回调或承载 kbsocket 的 DSO 被卸载，仍需上层协调。不能据此承诺任意退出阶段的 URMA 调用都安全。


## 加载结果

`UrmaApi::Load/Unload` 返回 `std::expected<void, LoadError>`。成功返回 `{}`；失败通过 `std::unexpected(LoadError{...})` 返回。`LoadError` 仅含失败码和固定长度错误文本，不包含成功状态或布尔转换。调用方先检查结果，再读取 `.error().code/message`。测试替换期间 Load 和 Unload 均返回 `LoadErrorCode::kInUse`。

## 裸设备 context 生命周期

`UrmaContext` 独占一个 context，`RawRuntime` 用 `unique_ptr` 持有各设备的封装，无需在收发路径维护 `shared_ptr` 引用计数。配置采用 `DeviceConfig{device_name, eid_index}` 数组，四个裸设备对应四项；数量不硬编码，每个设备暂时选择一个 EID。`eid_index` 是 provider 返回的实际索引，不是 EID 数组下标。

宿主先调用并检查 `UrmaApi::Load()`，再调用 `runtime.Initialize(configs)`。Runtime 独占本次 `Init/Uninit`，枚举指定设备并缓存目录，先验证所有端点，再依次创建 context 并校验实际 EID。全部成功后才允许通过 `runtime.context(index)` 借用句柄，索引对应配置顺序。当前只管理资源生命周期，不负责四设备调度、队列池或故障迁移。

初始化失败会逆序回滚。`RuntimeError` 保留启动失败信息，`cleanup_error` 额外报告回滚失败。删除 context 失败时保留句柄，不提前 `Uninit`；调用方处理残留引用后重试 `Close()`。已成功删除的 context 不会重复删除。URMA core 的 `Uninit()` 即使返回 provider 错误，也已经释放全局设备和卸载 provider，因此这类错误仍会上报，但不会重试 `Uninit()`。

退出顺序：停止新操作，确认 worker 不再访问这些 context，完成在途操作并释放队列和内存注册，然后显式 `runtime.Close()`，最后由宿主 `UrmaApi::Unload()`。`Close()` 不负责停止 worker，也不能与借用句柄的操作并发。context 删除失败时必须保留 runtime 对象并处理错误；若拖到析构仍删除失败，只能记录日志并保留残留资源及会话到进程退出，不能继续卸载库。

同一进程只允许一个活跃 `RawRuntime`，外部不得另行 Init/Uninit 或在借用资源存活期间重新枚举设备。该限制不构成对外部直接调用 URMA 的拦截，须由宿主遵守。无法保证 worker 停止的进程级实例应由宿主保持到进程退出，避免进入自动析构清理。

`raw_runtime_test` 使用 gMock 验证四设备发布、稀疏 EID 索引、创建失败回滚与重试、原始错误和清理错误同时保留、关闭失败保留资源及 Uninit 错误语义，不依赖本机 liburma.so 或硬件。

### bRPC 等进程级常驻使用方式

生产入口使用 `auto& runtime = GetRawRuntime();`，内部通过 `NoDestructor<RawRuntime>` 保存唯一实例。该入口不执行 Load 或 Initialize，启动阶段仍需由控制线程完成并检查结果。正常进程退出时不会自动析构 runtime，因此不会由它删除 context 或调用 Uninit；宿主也不得从 atexit 或其他静态析构中调用 Close/Uninit/Unload。

常驻实例支持显式 Close 和重新 Initialize，但必须先阻止新操作进入，并确认正在执行的调用、延迟回调和在途操作都已停止访问，释放子资源。不能把 DeleteContext 返回失败当作并发访问检测或同步机制。局部 RawRuntime 保留 RAII 清理能力，仅适用于能够满足这些前置条件的作用域。

该策略只避免本层静态析构导致的提前清理，不负责停止 bRPC worker，也不保证 provider 自身退出回调的安全；承载 kbsocket 的动态库必须在所有使用者停止后才能卸载。单测通过子进程执行 std::exit 验证不自动 DeleteContext/Uninit，同时覆盖显式关闭后再次初始化。

## device_info 能力输出

工具使用 gflags 解析 `--library`，其余位置参数为裸设备名；`--help` 在加载 URMA 前处理，无需本机安装 liburma.so。支持 `--library PATH` 和 `--library=PATH`。例如：

```sh
bazel run //tools/capabilities:device_info -- --library=/opt/urma/lib/liburma.so raw0 raw1
```

每台设备分组输出 GUID、传输模式及 TP 类型、资源数量、队列深度、消息/READ/WRITE/inline 大小、三类 SGE 限制、EID 容量、页大小能力、端口状态/速率/宽度/MTU、原子操作支持及大小、RM 顺序能力、乱序接收与乱序完成能力。大小输出为字节；端口使用属性数组索引标识，不推断物理端口编号。未知枚举保留数值，能力位图保留原始十六进制值。

输出代表 provider 上报能力，不代表当前剩余资源或实际可分配容量；零值不解释为无限制。工具继续使用 DeviceCatalog 的裸设备/RM_CTP 门禁，不用于枚举被门禁拒绝的设备。输出测试使用构造的设备属性，不需要 liburma.so 或真实硬件。

## 单设备 JettyPool

`JettyPool` 独占发送 JFC、接收 JFC、一个 JFR 和共享该 JFR 的多个 jetty。这里的共享指 URMA jetty 引用独立创建的 JFR，每池只有一个 JFR；不创建独立 JFS。`Open(ctx, cap, config)` 借用已打开的 context，`cap` 必须来自同一设备的目录快照。先检查 `RM_CTP`、队列深度、SGE 和 inline 限制，再创建资源，全部成功才允许通过 `Reserve()` / `jfr()` 借用。

使用 `//kbsocket/transport/raw:jetty_pool` Bazel 目标。配置默认发送/接收深度各 128，两个 JFC 深度各 256，SGE 各 1；不满足设备限制时显式失败，不自动裁剪。inline 为 0 遵循 URMA 的设备默认值语义。采用无 JFCE 的纯轮询模式，队列配置 lock_free，所有投递和轮询必须遵守同一 owner 约束。没有事件唤醒、远端导入或建链；RM_CTP 能力门禁不等于已完成 CTP 连接，远端导入时仍须指定正确的 TP 类型。JFR 暂用默认无 token 策略，后续建链认证需另行配置。

`PollSend` / `PollRecv` 接收调用方预分配的 `std::span<urma_cr_t>`，返回本批完成数量（0 为无完成），不分配内存或加锁。调用方继续检查每条完成的状态并回收对应请求，不能把轮询成功等同于每条 WR 成功。

创建失败自动逆序回滚，`JettyPoolError.failure` 保存原始错误，`cleanup_error` 保存清理失败。关闭顺序为 jetty → JFR → 接收 JFC → 发送 JFC；任一删除失败即停止，保留其依赖资源，再次 Close 只处理尚未释放部分。Open/Close 必须在使用者停止时执行，ready 标志不是并发停止协议。

池当前独立于 RawRuntime，宿主必须在关闭 context 前成功关闭所有借用它的池。退出时若 bRPC worker 仍在访问，整个池、已注册内存及 context 都必须常驻，例如由 NoDestructor 持有上层资源容器；不能仅让 RawRuntime 常驻而让池自动析构。析构只作受控作用域的兜底，失败会记录错误并保留底层依赖资源，不替代显式停止与清理。

### JettyPool 的共享 SQ 与逐 WR 额度

`jetty_count` 控制预创建数量。所有 jetty 共享 TX JFC 和 JFR/RX JFC，容量要求为 `jetty_count * (tx_depth + 1) <= tx_cq_depth`，另一个 CQE 用于每个 jetty 的 FLUSH_ERR_DONE 边界。池不在线扩容、不跨设备或 owner 迁移。

RM_CTP 每个 WR 自行指定 `tjetty`，连接不独占 jetty。`Reserve()` 轮转选择有容量的健康 SQ，为一个 WR 返回 `JettyTicket`；`ReserveOn(lane)` 可以在指定 SQ 上预留后续 WR，用于同队列批量投递。`Get(lane)` 返回物理队列，不代表独占权。同一 SQ 的多个票据可以属于不同连接和目标，同时处于在途状态。

票据遵循 `Reserve → Commit → Complete` 或 `Reserve → Cancel`。批量 post 部分成功时，仅 Commit 已接受前缀，Cancel 未接受后缀。预留已经扣除额度，确认不重复扣减；已提交票据不能 Cancel，未提交票据不能 Complete。票据序号防止迟到或重复完成误释放复用后的额度，lane epoch 则仅随物理队列重建更新。票据不跨越池对象销毁重建有效。所有票据槽在 Open 预分配，热路径不分配内存。

池当前不包装 post，不解析 CQE，也不拥有连接或 DMA buffer。调用方须在发送账本中关联票据、ConnId/OpId、目标和 buffer/grant 租约；批量 post、登记和 poll 不得重入。聚合完成须由上层识别实际终结的 WR 并逐条退休，不能按 CQE 条数释放额度，`FLUSH_ERR_DONE` 不对应用户票据。逐 WR 身份与状态校验由池和下述 AttemptLedger 协作完成；账本仅记录逻辑 WR，不暴露 provider 的硬件队列布局。

`MarkFaulted(lane)` 阻止整个 SQ 的新预留及 Get，但允许确认先前已接受的 WR、取消未提交票据及退休终结 WR，不重放请求。只要仍有预留或在途票据，Close 返回 kInUse 并保持轮询可用；全部退休后按依赖逆序销毁。关闭连接不销毁共享队列，也不取消其已提交票据；共享 SQ 上其他连接可继续发送。接收 WR、导入对象及 DMA 内存的关闭顺序仍由上层管理。

### 通过 gflags 设置队列深度

宿主在创建池前调用 `gflags::ParseCommandLineFlags`，可传入 `--kbsocket_tx_depth=128 --kbsocket_rx_depth=128`，两者默认均为 128。`tx_depth` 是每个 jetty 的发送 WR 深度，rx_depth 是整个池共享 JFR 的深度。

JettyPoolConfig 的 `tx_depth`/`rx_depth` 为可选配置，显式指定优先，否则在 Open 时读取 flags。已创建的池不会随 flag 修改而调整硬件或在途额度。零值、超过设备上限以及不满足 JFC 容量约束的配置，通过 Open 返回错误；库不自行解析参数或终止进程。`tx_cq_depth`/`rx_cq_depth` 保持独立显式配置，增大 WR 深度时须同步保证完成队列容量足够。

TX JFC 的额外容量用于每个 jetty 的 FLUSH_ERR_DONE 边界 CQE；用户 tx_depth、实际 JFS 深度和在途 WR 额度保持不变。加法和乘法使用 64 位计算，避免深度边界值溢出。

### 链路优先级和固定 SGE

`--kbsocket_link_priority=4` 控制创建 jetty 时的 `jfs_cfg.priority`，合法范围为 0–15，在 Open 时读取，已创建的 jetty 不随 flag 变化。非法值在创建任何硬件资源前返回参数错误。实际调度效果依赖 provider 和网络 QoS 配置。

远端 SGE（`jfs_cfg.max_rsge`）与接收 SGE（`jfr_cfg.max_sge`）固定为 1；Open 检查设备至少支持一个，不再暴露 `remote_sge`/`recv_sge` 配置。`rnr_retry=6`、`err_timeout=2` 保持当前配置；min_rnr_timer 使用 `URMA_TYPICAL_MIN_RNR_TIMER`（12），与当前 UMQ 的 19 不同，属于明确的重试等待策略选择，仍需在真实设备上验证。


### AttemptLedger 发送账本

Bazel 目标为 `//kbsocket/transport/raw:attempt_ledger`。账本绑定一个 JettyPool，由相同 owner 推进。`Open(pool, capacity)` 预分配固定数量记录；容量不足在 post 前失败，不动态扩容。通常按池内发送额度总量设置容量。

使用顺序：

1. `Prepare(metadata, optional_lane)` 预留账本和 SQ 额度，返回可写入 WR `user_ctx` 的 `AttemptId`。通过 `Lookup(id)` 读取 ticket，使用 `pool.Get(ticket.lane)` 取得队列。Prepare 失败不接管目标和资源引用。
2. 提交层将 metadata 对应目标、opcode 写入 WR，确保启用完成通知，执行 post；已接受部分调用 `Commit(id, submission_sequence)`，未接受后缀调用 `Cancel(id)`。预留顺序不等于实际提交顺序，提交位置由提交层明确传入。
3. 完成层确认某条 WR 已终结且 DMA 不再访问其资源后调用 `Complete(id)`，取得原始记录，再分发结果并处理 buffer/grant 租约。SQ 额度归还不代表协议 grant 自动撤销；远端授权生命周期仍由上层协议决定。

记录保存连接、操作及其代次、远端目标、opcode、buffer/grant 租约标识，以及 lane epoch、票据、逻辑提交位置。尚无统一资源租约实现，因此账本保存的是引用标识，不会自动释放内存或 unimport 目标。上层资源表必须保持引用直到返回退休记录，且根据协议判断是否可以真正回收。

AttemptId 在同一账本对象存活期间单调增加且不回绕，Close/Open 不重置，按容量取模直接定位槽位；预留跳过仍占用的槽，最坏扫描 capacity 次，查询和退休为 O(1)。不同账本的 id 不保证唯一，CQ 路由必须先确定所属账本；票据仅经账本流转，不能再直接调用池的 Commit/Cancel/Complete。

首版只接受全 signal 记录，不解析 CQE 状态、不自动 post、不实现 selective signaling 或累计成功推导。连接关闭、队列故障、`FLUSH_ERR_DONE` 本身都不能触发账本整体清空。Close 在有预留或在途记录时拒绝关闭；进程退出仍有工作线程时，账本、队列及资源表必须一起常驻。对象销毁后不得再使用旧 id。
