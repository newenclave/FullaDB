/*
 * File: slot_directory.hpp
 * Author: newenclave
 * GitHub: https://github.com/newenclave
 * Created: 2025-12-23
 * License: MIT
 */

 #pragma once

#include <cassert>
#include "fulla/core/types.hpp"
#include "fulla/core/pack.hpp"
#include "fulla/slots/concepts.hpp"

namespace fulla::page { 

    using core::word_u16;
    using core::byte;
    using core::byte_span;
    using core::byte_view;
    using core::byte_buffer;

    constexpr static const typename core::word_u16::word_type SLOT_INVALID = word_u16::max();
    constexpr static const auto SLOT_FREE = SLOT_INVALID;

    FULLA_PACKED_STRUCT_BEGIN
    struct slot_entry {
        word_u16 off;
        word_u16 len;
    };

    namespace slot_fixed {
        struct header {
            word_u16 size{ 0 };         // one slot size
            word_u16 free_beg{ 0 };     // offset to the first free byte in payload area (grows upward)
            word_u16 free_end{ 0 };     // offset to the last free byte+1 from the end (grows downward)
            word_u16 freed;             // first freed slot.
        } FULLA_PACKED;

        struct free_slot_type {
            word_u16 next;
            word_u16 slot;
        } FULLA_PACKED;
    }

    namespace slot_variadic {
        struct header {
            word_u16 slots{ 0 };        // number of occupied slot entries
            word_u16 free_beg{ 0 };     // offset to the first free byte in payload area (grows upward)
            word_u16 free_end{ 0 };     // offset to the last free byte+1 from the end (grows downward)
            word_u16 freed;             // first freed slot.
        } FULLA_PACKED;

        struct free_slot_type {
            word_u16 prev;
            word_u16 next;
            word_u16 len;
        } FULLA_PACKED;
    }
    FULLA_PACKED_STRUCT_END
} 