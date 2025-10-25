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


template <typename T = int>
struct null_field {
    // pre-increment: ++x
    constexpr null_field& operator++() noexcept { return *this; }
    // pre-decrement: --x
    constexpr null_field& operator--() noexcept { return *this; }

    // post-increment: x++
    constexpr T operator++(int) noexcept { return T{}; }
    // post-decrement: x--
    constexpr T operator--(int) noexcept { return T{}; }

    // compound updates: x += n / x -= n
    constexpr null_field& operator+=(T) noexcept { return *this; }
    constexpr null_field& operator-=(T) noexcept { return *this; }

    // plain assignment: x = n
    constexpr null_field& operator=(T) noexcept { return *this; }

    // implicit read as zero: T v = x; std::cout << x;
    constexpr operator T() const noexcept { return T{}; }
};

struct null_stats {
    null_field<> hits, misses, evictions, pinned_fail;
    null_field<> reads, writes, writebacks, forced_flushes;
    null_field<> alloc_pages, created_pages;
    null_field<> clock_scans, refbit_clears;
    void reset() {}
};

} // namespace fulla::storage
