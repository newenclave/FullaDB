/*
 * File: stats.hpp
 * Author: newenclave
 * GitHub: https://github.com/newenclave
 * Created: 2025-10-25
 * License: MIT
 */

 #pragma once
#include <cstdint>

namespace fulla::storage {

struct stats {
    std::uint64_t hits = 0, misses = 0, evictions = 0, pinned_fail = 0;
    std::uint64_t reads = 0, writes = 0, writebacks = 0, forced_flushes = 0;
    std::uint64_t alloc_pages = 0, created_pages = 0;
    std::uint64_t clock_scans = 0, refbit_clears = 0;
    void reset() { *this = {}; }
};

struct null_stats {
    // Same API, but no counters
    void reset() {}
};

} // namespace fulla::storage
