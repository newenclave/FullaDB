/*
 * File: header.hpp
 * Author: newenclave
 * GitHub: https://github.com/newenclave
 * Created: 2025-10-25
 * License: MIT
 */

#pragma once
#include <cstdint>
#include <compare>

#include "fulla/core/bytes.hpp"
#include "fulla/core/pack.hpp"
#include "fulla/core/types.hpp"

namespace fulla::page {

    using core::word_u16;
    using core::word_u32;
    using core::byte;

    // Logical kind of a page in the file.
    enum class page_kind : word_u16::word_type {
        undefined   = 0,
        sys_page    = 1,
        heap        = 2,
        bpt_root    = 3,
        bpt_leaf    = 4,
        bpt_inode   = 5,
        long_store_head  = 6,
        long_store_chunk = 7,
    };

    // Common page header that prefixes every page in the file.
    // The header is packed to guarantee on-disk layout stability.
    FULLA_PACKED_STRUCT_BEGIN
    struct page_header {
        word_u16 kind {0};         // page_kind
        word_u16 reserved {0};     //

        word_u16 subhdr_size{ 0 }; // size of type-specific subheader right after page_header
        word_u16 page_end{ 0 };    // full page size

        word_u32 self_pid {0};     // optional: page id (can be eliminated if manager tracks PID)
        word_u32 crc {0};          // reserved: page checksum

        // Initialize header fields for a fresh page buffer.
        void init(page_kind k, std::size_t page_size, std::uint32_t self, std::size_t subheader_size = 0) {
            kind = static_cast<word_u16::word_type>(k);
            self_pid = self;
            reserved = 0;
            page_end = static_cast<word_u16::word_type>(page_size);
            subhdr_size = static_cast<word_u16::word_type>(subheader_size);
            crc = 0xBAADF00D;
        }

        static page_header *to_header(void *ptr) {
            return static_cast<page_header *>(ptr);
        }

        static constexpr std::size_t header_size() noexcept {
            return sizeof(page_header);
        }

        byte* data() noexcept {
            return reinterpret_cast<byte*>(this);
        }
        const byte* data() const noexcept {
            return reinterpret_cast<const byte*>(this);
        }

        word_u16::word_type base() const {
            return static_cast<word_u16::word_type>(header_size() + subhdr_size);
        }

        word_u16::word_type capacity() const {
            return page_end - static_cast<word_u16::word_type>(header_size() + subhdr_size);
        }

    private:

    } FULLA_PACKED;
    FULLA_PACKED_STRUCT_END

    static_assert(sizeof(page_header) == 16, "page_header must be 16 bytes (packed).");

} // namespace fulla::page