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
#include "fulla/storage/device.hpp"

namespace fulla::storage {

// Simple file-backed random-access device (not thread-safe).
class file_device {
public:
    using position_type = storage::position_type;
    using offset_type = storage::position_type;

    file_device() = default;

    explicit file_device(const std::filesystem::path& filename,
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

    // Write n bytes at file start.
    bool write_at_start(const void* data, std::size_t n) {
        return write_at_offset(0, static_cast<const fulla::core::byte*>(data), n);
    }

    // Write n bytes at offset.
    bool write_at_offset(position_type offset,
                         const fulla::core::byte* data,
                         std::size_t n) {
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

    // Read n bytes at offset.
    bool read_at_offset(position_type offset,
                        fulla::core::byte* dst,
                        std::size_t n) {
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

    // Append n bytes at file end; returns position where data begins.
    position_type append(const fulla::core::byte* data, std::size_t n) {
        if (!is_open()) {
            return 0;
        }
        file_.seekp(0, std::ios::end);
        std::streamoff pos = file_.tellp();
        if (pos < 0) {
            return 0;
        }
        file_.write(reinterpret_cast<const char*>(data),
                    static_cast<std::streamsize>(n));
        file_.flush();
        return static_cast<position_type>(pos);
    }

    // Allocate and return aligned start of a fresh block at end of file.
    position_type allocate_block() {
        if (!is_open()) {
            return 0;
        }
        file_.seekp(0, std::ios::end);
        std::streamoff endp = file_.tellp();
        if (endp < 0) {
            return 0;
        }
        const std::streamoff bs  = static_cast<std::streamoff>(block_size_);
        const std::streamoff aligned = ((endp + (bs - 1)) / bs) * bs;

        // Extend file by 1 byte to set size at least to aligned + (bs - 1)
        // NOTE: This only ensures size; it does not zero-out the new block.
        file_.seekp(aligned + (bs - 1), std::ios::beg);
        file_.put('\0');
        file_.flush();

        return static_cast<position_type>(aligned);
    }

    position_type get_file_size() {
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
        return (endg >= 0) ? static_cast<position_type>(endg) : 0;
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

static_assert(RandomAccessDevice<file_device>);

} // namespace fulla::storage
