/*
 * File: device.hpp
 * Author: newenclave
 * GitHub: https://github.com/newenclave
 * Created: 2025-12-08
 * License: MIT
 */

#pragma once

#include <cstdint>
#include <concepts>
#include "fulla/core/bytes.hpp"

namespace fulla::storage {

    // A generic random-access block device for page/file IO.
    template <class D>
    concept RandomAccessBlockDevice = requires(
        D dev,
        typename D::block_id_type block_id,
        fulla::core::byte* dst,
        const fulla::core::byte* src,
        std::size_t n
    ) {
        typename D::block_id_type;
        
        // Require invalid_block_id constant
        { D::invalid_block_id } -> std::convertible_to<typename D::block_id_type>;

        { dev.block_size() } -> std::convertible_to<std::size_t>;
        { dev.blocks_count() }  -> std::convertible_to<std::size_t>;
        
        { dev.is_open() }    -> std::convertible_to<bool>;

        // Return bool for success/failure (recommended)
        { dev.read_block(block_id, dst, n) }  -> std::same_as<bool>;
        { dev.write_block(block_id, src, n) } -> std::same_as<bool>;

        { dev.append(src, n) }   -> std::convertible_to<typename D::block_id_type>;
        { dev.allocate_block() } -> std::convertible_to<typename D::block_id_type>;
    };

} // namespace fulla::storage
