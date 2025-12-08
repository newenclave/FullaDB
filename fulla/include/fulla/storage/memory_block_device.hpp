/*
 * File: buffer_manager.hpp
 * Author: newenclave
 * GitHub: https://github.com/newenclave
 * Created: 2025-12-08
 * License: MIT
 */

#pragma once
#include <cstdint>
#include <vector>

#include "fulla/core/bytes.hpp"
#include "fulla/storage/block_device.hpp"

namespace fulla::storage {
    class memory_block_device {
    public:
        using block_id_type = std::size_t;
        using offset_type = std::size_t;
		constexpr static block_id_type invalid_block_id = std::numeric_limits<block_id_type>::max();

        memory_block_device(std::size_t block_size) : block_size_(block_size) {};

        bool is_open() const noexcept { return true; }
        std::size_t block_size() const noexcept { return block_size_; }

        block_id_type allocate_block() {
            const offset_type pos = data_.size();
            data_.resize(data_.size() + block_size_);
            return pos / block_size_;
        }

        std::size_t blocks_count() const noexcept {
            return data_.size() / block_size_;
        }

        block_id_type append(const core::byte* src, std::size_t n) {
            const offset_type pos = data_.size();
            data_.insert(data_.end(), src, src + n);
            return pos / block_size_;
        }

        bool read_block(block_id_type bid, core::byte* dst, std::size_t n) {
            const auto off = bid * block_size_;
            if (off + n > data_.size()) {
                return false;
            }
            std::memcpy(dst, data_.data() + off, n);
            return true;
        }

        bool write_block(block_id_type bid, const core::byte* src, std::size_t n) {
            const auto off = bid * block_size_;
            if (off + n > data_.size()) {
                return false;
            }
            std::memcpy(data_.data() + off, src, n);
            return true;
        }

    private:
        std::size_t block_size_ = 1024;
        std::vector<core::byte> data_;
    };
    static_assert(RandomAccessBlockDevice<memory_block_device>);
}
