// SPDX-License-Identifier: MulanPSL-2.0
#include "tools/import_jetty_test/api_timings.hpp"

#include <algorithm>
#include <numeric>
#include <print>
#include <string>
#include <vector>

namespace kbsocket {
namespace tools {
void ApiTimings::Record(ControlApi api, std::uint64_t ns, bool success, int code) noexcept {
    auto& row = rows_[static_cast<std::size_t>(api)];
    if (row.count == row.samples.size()) {
        ++row.dropped;
        return;
    }
    row.samples[row.count++] = {ns, success, code};
}
void ApiTimings::Skip(ControlApi api, const char* reason) noexcept {
    rows_[static_cast<std::size_t>(api)].skipped = reason;
}
const ApiMeasurements& ApiTimings::measurements(ControlApi api) const noexcept {
    return rows_[static_cast<std::size_t>(api)];
}
bool ApiTimings::HasFailures() const noexcept {
    for (const auto& row : rows_) {
        for (std::size_t i = 0; i < row.count; ++i) {
            if (!row.samples[i].success) {
                return true;
            }
        }
        if (row.dropped) {
            return true;
        }
    }
    return false;
}
void ApiTimings::Print(FILE* output, bool all_samples) const {
    std::println(output, "api,attempts,success,failed,first_ns,first_ok,call100_ns,call100_ok,min_ok_ns,mean_ok_ns,"
                         "p50_ok_ns,p95_ok_ns,p99_ok_ns,max_ok_ns,last_error,dropped,note");
    for (std::size_t i = 0; i < rows_.size(); ++i) {
        const auto& row = rows_[i];
        std::vector<std::uint64_t> good;
        int last_error = 0;
        for (std::size_t j = 0; j < row.count; ++j) {
            if (row.samples[j].success) {
                good.push_back(row.samples[j].ns);
            } else {
                last_error = row.samples[j].code;
            }
        }
        std::sort(good.begin(), good.end());
        auto value = [](auto v) { return std::to_string(v); };
        auto percentile = [&](std::size_t p) {
            return good.empty() ? "NA" : value(good[(good.size() * p + 99) / 100 - 1]);
        };
        std::println(
            output, "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}", kControlApiNames[i], row.count, good.size(),
            row.count - good.size(), row.count ? value(row.samples[0].ns) : "NA",
            row.count ? value(row.samples[0].success) : "NA", row.count >= 100 ? value(row.samples[99].ns) : "NA",
            row.count >= 100 ? value(row.samples[99].success) : "NA", good.empty() ? "NA" : value(good.front()),
            good.empty() ? "NA" : value(std::accumulate(good.begin(), good.end(), 0.0) / good.size()), percentile(50),
            percentile(95), percentile(99), good.empty() ? "NA" : value(good.back()), last_error, row.dropped,
            row.count ? "measured" : row.skipped);
    }
    if (all_samples) {
        std::println(output, "sample_api,call_index,elapsed_ns,success,code");
        for (std::size_t i = 0; i < rows_.size(); ++i) {
            for (std::size_t j = 0; j < rows_[i].count; ++j) {
                const auto& sample = rows_[i].samples[j];
                std::println(output, "{},{},{},{},{}", kControlApiNames[i], j + 1, sample.ns, sample.success,
                             sample.code);
            }
        }
    }
}
} // namespace tools
} // namespace kbsocket
