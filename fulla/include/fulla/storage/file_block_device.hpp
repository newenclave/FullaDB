/*
 * File: file_device.hpp
 * Author: newenclave
 * GitHub: https://github.com/newenclave
 * Created: 2025-10-25
 * License: MIT
 */

#pragma once
#include <cstdint>
#include <fstream>
#include <filesystem>

#include "fulla/core/bytes.hpp"
#include "fulla/storage/block_device.hpp"
#include "fulla/core/debug.hpp"

namespace fulla::storage {

// Simple file-backed random-access block device (not thread-safe).
class file_block_device {
public:

    using block_id_type = std::uint64_t;
    using position_type = std::streamoff;
	constexpr static block_id_type invalid_block_id = std::numeric_limits<block_id_type>::max();

    file_block_device() = default;

    explicit file_block_device(const std::filesystem::path& filename,
                         std::size_t block_size = 4096)
        : block_size_(block_size) {
        open_or_create_(filename);
    }

    std::size_t block_size() const noexcept {
        return block_size_;
    }

    bool is_open() const noexcept {
        return file_.is_open();
    }

    // Write n bytes at offset.
    bool write_block(block_id_type bid,
                    const fulla::core::byte* data,
                    std::size_t n) {
        if (bid > std::numeric_limits<position_type>::max() / block_size_) {
            return false;  // Overflow would occur
        }
        const auto offset = static_cast<position_type>(bid) * static_cast<position_type>(block_size_);
        if (!is_open()) {
            return false;
        }
        file_.clear();
        file_.seekp(static_cast<std::streamoff>(offset), std::ios::beg);
        if (!file_) {
            return false;
        }
        file_.write(reinterpret_cast<const char*>(data),
                    static_cast<std::streamsize>(n));
        if (!file_) {
            return false;
        }
        return true;
    }

    // Read n bytes from a block.
    bool read_block(block_id_type bid,
                fulla::core::byte* dst,
                std::size_t n) {
        if (bid > std::numeric_limits<position_type>::max() / block_size_) {
            return false;  // Overflow would occur
        }
        const auto offset = static_cast<position_type>(bid) * static_cast<position_type>(block_size_);
        if (!is_open()) {
            return false;
        }
        file_.clear();
        file_.seekg(static_cast<std::streamoff>(offset), std::ios::beg);
        if (!file_) {
            return false;
        }
        file_.read(reinterpret_cast<char*>(dst),
                   static_cast<std::streamsize>(n));
        return static_cast<std::size_t>(file_.gcount()) == n;
    }

    // Append block and n bytes at file end; returns position where data begins.
    block_id_type append(const fulla::core::byte* data, std::size_t n) {
        if (!is_open()) {
            return invalid_block_id;
        }
        file_.seekp(0, std::ios::end);
        std::streamoff pos = file_.tellp();
        if (pos < 0) {
            return invalid_block_id;
        }
        DB_ASSERT(static_cast<std::size_t>(pos) % block_size_ == 0,
                  "Append position is not aligned to block size");
        DB_ASSERT(static_cast<std::size_t>(n) <= block_size_,
                  "'n' must be less than or equal to block size");
                  
        const auto tail = static_cast<std::streamoff>(block_size_ - n);

        file_.write(reinterpret_cast<const char*>(data),
                    static_cast<std::streamsize>(n));
        if (!file_) {
            return invalid_block_id;
        }
        
        // Extend file to end of block (sparse)
        if (tail > 0) {
            file_.seekp(pos + block_size_ - 1, std::ios::beg);
            file_.put('\0');
            if (!file_) {
                return invalid_block_id;
            }
        }
        
        file_.flush();
        return static_cast<block_id_type>(pos) / static_cast<block_id_type>(block_size_);
    }

    // Allocate and return aligned start of a fresh block at end of file.
    block_id_type allocate_block() {
        if (!is_open()) {
            return invalid_block_id;
        }
        file_.seekp(0, std::ios::end);
        std::streamoff endp = file_.tellp();
        if (endp < 0) {
            return invalid_block_id;
        }
        const std::streamoff bs  = static_cast<std::streamoff>(block_size_);
        const std::streamoff aligned = ((endp + (bs - 1)) / bs) * bs;
 
        // Extend file by 1 byte to set size at least to aligned + (bs - 1)
        // NOTE: This only ensures size; it does not zero-out the new block.
        file_.seekp(aligned + (bs - 1), std::ios::beg);
        file_.put('\0');
        if (!file_) {
            return invalid_block_id;
        }
        file_.flush();

        return static_cast<block_id_type>(aligned) / static_cast<block_id_type>(block_size_);
    }

    std::size_t blocks_count() {
        if (!is_open()) {
            return 0;
        }
        auto cur_g = file_.tellg();
        auto cur_p = file_.tellp();
        file_.seekg(0, std::ios::end);
        std::streamoff endg = file_.tellg();

        // restore positions (best-effort)
        if (cur_g >= 0) {
            file_.seekg(cur_g, std::ios::beg);
        }
        if (cur_p >= 0) {
            file_.seekp(cur_p, std::ios::beg);
        }
        DB_ASSERT(static_cast<std::size_t>(endg) % block_size_ == 0,
                  "File size is not aligned to block size");
        return (endg >= 0) ? (static_cast<std::size_t>(endg) / block_size_) : 0;
    }

private:

    void open_or_create_(const std::filesystem::path& filename) {
        file_.open(filename, std::ios::in | std::ios::out | std::ios::binary);
        if (!file_.is_open()) {
            file_.open(filename, std::ios::out | std::ios::binary | std::ios::trunc);
            file_.close();
            file_.open(filename, std::ios::in | std::ios::out | std::ios::binary);
        }
    }

private:
    std::size_t block_size_{4096};
    std::fstream file_{};
};

static_assert(RandomAccessBlockDevice<file_block_device>);

} // namespace fulla::storage
