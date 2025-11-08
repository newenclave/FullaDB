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
        undefined    = 0,
        heap         = 1,
        btree_leaf   = 2,
        btree_inode  = 3,
        long_store   = 4,
    };

    // Common page header that prefixes every page in the file.
    // The header is packed to guarantee on-disk layout stability.
    FULLA_PACKED_STRUCT_BEGIN
    struct page_header {
        word_u16 kind {0};         // page_kind
        word_u16 slots {0};        // number of occupied slot entries

        word_u16 free_beg {0};     // offset to the first free byte in payload area (grows upward)
        word_u16 free_end {0};     // offset to the last free byte+1 from the end (grows downward)

        word_u16 slots_offset {0}; // start offset of the slot array (from page begin)
        word_u16 subhdr_size {0};  // size of type-specific subheader right after page_header

        word_u32 self_pid {0};     // optional: page id (can be eliminated if manager tracks PID)
        word_u32 crc {0};          // reserved: page checksum

        // Initialize header fields for a fresh page buffer.
        void init(page_kind k, std::size_t page_size, std::uint32_t self, std::size_t subheader_size = 0) {
            kind = static_cast<word_u16::word_type>(k);
            self_pid = self;
            slots = 0;
            subhdr_size = static_cast<word_u16::word_type>(subheader_size);

            const auto base = static_cast<word_u16::word_type>(sizeof(page_header) + static_cast<std::size_t>(subhdr_size));
            slots_offset = base;
            free_beg = base;
            free_end = static_cast<word_u16::word_type>(page_size);
            crc = 0;
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
    } FULLA_PACKED;
    FULLA_PACKED_STRUCT_END

    static_assert(sizeof(page_header) == 20, "page_header must be 20 bytes (packed).");

} // namespace fulla::page