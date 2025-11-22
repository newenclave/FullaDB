/*
 * File: device.hpp
 * Author: newenclave
 * GitHub: https://github.com/newenclave
 * Created: 2025-10-25
 * License: MIT
 */

#pragma once

#include <cstdint>
#include <concepts>
#include "fulla/core/bytes.hpp"

namespace fulla::storage {

    using position_type = std::uint64_t;

    // A generic random-access device for page/file IO.
    template <class D>
    concept RandomAccessDevice = requires(
        D dev,
        position_type off,
        fulla::core::byte* dst,
        const fulla::core::byte* src,
        std::size_t n
    ) {
        typename D::offset_type;
        { dev.block_size() } -> std::convertible_to<std::size_t>;
        { dev.is_open() }    -> std::convertible_to<bool>;

        // Return bool for success/failure (recommended)
        { dev.read_at_offset(off, dst, n) }  -> std::same_as<bool>;
        { dev.write_at_offset(off, src, n) } -> std::same_as<bool>;

        { dev.append(src, n) }   -> std::convertible_to<position_type>;
        { dev.allocate_block() } -> std::convertible_to<position_type>;
        { dev.get_file_size() }  -> std::convertible_to<position_type>;
    };

} // namespace fulla::storage