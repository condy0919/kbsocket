# URMA 加载与运行时生命周期

[文档入口](README.md) · [当前代码流程](raw-transport-flow.md)

本页对应 [UrmaApi](../kbsocket/transport/raw/urma_api.hpp)、[UrmaContext](../kbsocket/transport/raw/urma_context.hpp) 和 [RawRuntime](../kbsocket/transport/raw/raw_runtime.hpp)。设备快照细节见 [DeviceCatalog](device-catalog-reference.md)。

## 动态加载

`urma_api.hpp/.cpp` 参考 ubsocket 的函数表加载方式，使用 `decltype` 从 URMA 头文件取得准确签名。当前与 UMQ 的 URMA 符号表对齐，共加载 50 个必需符号，覆盖设备/context、JFC/JFCE/JFR、jetty 与收发、内存段、异步事件、日志、user_ctl、EID 转换和性能统计。UVS 的 `uvs_get_path_set` 属于独立 `libtpsa.so`，不纳入该表。部署库缺少其中任一符号时，加载整体失败。

`UrmaApi::Load()` 默认加载 `liburma.so`，也支持指定路径。全部符号解析成功才发布只读函数表；打开失败或缺符号时返回 `std::unexpected(LoadError)`，包含失败码和复制到固定长度数组中的错误文本，允许重试。已加载时再次 Load 返回 `LoadErrorCode::kAlreadyLoaded`，不替换现有库。

```sh
USE_BAZEL_VERSION=9.2.0 bazel run //tools/capabilities:device_info -- --library /opt/urma/lib/liburma.so raw_device_name
```

加载标志与参考代码一致：`RTLD_NOW | RTLD_GLOBAL`。GLOBAL 允许 provider 解析核心符号；`Load/Unload` 均返回 `std::expected<void, LoadError>`；Unload 清空函数表并释放加载引用。

生命周期顺序是 loader → URMA init → catalog/硬件资源，清理时反向执行。loader 不自动调用 init/uninit，也不管理进程级 URMA 会话引用；控制线程需协调所有使用者。Load/Unload 不得与调用并发，退出会话前必须释放所有相关资源，不能在 Unload 后使用复制出的函数指针。

工具即使没有 liburma.so 也可编译和启动；实际查询需要运行时存在该库，缺库时明确返回加载失败。动态加载不能替代库本身，也不能保证任意版本 ABI 兼容。加载层测试通过 gMock 与测试二进制的链接包装拦截 dlopen/dlsym/dlclose/dlerror，覆盖完整加载、最后一个符号缺失时的回滚与重试、卸载清空函数表。不构建测试共享库；这些测试不证明真实 provider 的行为或 ABI 兼容性。

## 进程级调用入口与 gMock

业务使用 `UrmaApi::CreateJfr(ctx, cfg)` 等静态接口，全部 50 个 URMA 函数都有对应入口。生产启动顺序为 `UrmaApi::Load()` → `UrmaApi::Init()` → 创建资源/业务运行；退出时先停止调用并释放资源，再 `Uninit()` 和 `Unload()`。UrmaApi 直接持有进程级库句柄和唯一的常驻函数表；Load 使用局部临时表解析，全部成功后才发布。

`UrmaApi` 不自动计数业务资源或多 runtime 引用；进程的控制线程负责协调初始化和退出。没有加载或测试未填某个符号时，入口设置 errno=ENOSYS，并按签名返回 nullptr、URMA_FAIL 或 -1；void 接口只设置 errno。

测试依赖 testonly 的 `//kbsocket/transport/raw:urma_test_support`，包含 `testing/scoped_urma_override.hpp`。示例：

```cpp
UrmaFunctions functions{};
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
