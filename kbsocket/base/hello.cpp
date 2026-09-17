#include <urma_types.h>

#include <iostream>

int main() {
    // 验证能够正确引用 urma_headers 导出的类型
    urma_status_t status = URMA_SUCCESS;
    std::cout << "Hello, World! (urma status: " << status << ")\n";
    return 0;
}
