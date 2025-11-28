/*
 * File: buffer_manager.hpp
 * Author: newenclave
 * GitHub: https://github.com/newenclave
 * Created: 2025-11-26
 * License: MIT
 */

#pragma once
#include <cstdint>
#include <vector>

#include "fulla/core/bytes.hpp"
#include "fulla/storage/device.hpp"

namespace fulla::storage {
    class memory_device {
    public:
        using position_type = std::size_t;
        using offset_type = std::size_t;
        memory_device(std::size_t block_size) : block_size_(block_size) {};

        bool is_open() const noexcept { return true; }
        std::size_t block_size() const noexcept { return block_size_; }

        offset_type allocate_block() {
            const offset_type pos = data_.size();
            data_.resize(data_.size() + block_size());
            return pos;
        }

        offset_type get_file_size() const noexcept {
            return data_.size();
        }

        offset_type append(const core::byte* src, std::size_t n) {
            const offset_type pos = data_.size();
            data_.insert(data_.end(), src, src + n);
            return pos;
        }

        bool read_at_offset(offset_type off, core::byte* dst, std::size_t n) {
            if (off + n > data_.size()) {
                return false;
            }
            std::memcpy(dst, data_.data() + off, n);
            return true;
        }

        bool write_at_offset(offset_type off, const core::byte* src, std::size_t n) {
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
    static_assert(RandomAccessDevice<memory_device>);
}
