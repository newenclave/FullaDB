/*
 * File: bpt_leaf.hpp
 * Author: newenclave
 * GitHub: https://github.com/newenclave
 * Created: 2025-11-21
 * License: MIT
 */

 #pragma once

#include "fulla/core/pack.hpp"
#include "fulla/core/types.hpp"

namespace fulla::page {

    using core::word_u16;
    using core::word_u32;

FULLA_PACKED_STRUCT_BEGIN

    struct bpt_leaf_header {
        word_u32 parent{ 0 };
        word_u32 prev{ 0 };
        word_u32 next{ 0 };
        word_u32 reserved{ 0 };

        void init() {
            parent = 0;
            prev = 0;
            next = 0;
        }
    } FULLA_PACKED;

    struct bpt_leaf_slot {
        word_u16 key_len{ 0 };
        word_u16 value_off{ 0 };
        
        static typename word_u16::word_type key_offset() {
            return sizeof(bpt_leaf_slot);
        }
        
        word_u16::word_type value_offset() const {
            return value_off;
        }

        void update(std::size_t new_key_len) {
            key_len = static_cast<word_u16::word_type>(new_key_len);
            update_value_offset();
        }

        void update_value_offset() {
            value_off = static_cast<word_u16::word_type>(sizeof(bpt_leaf_slot) + static_cast<word_u16::word_type>(key_len));
        }

    } FULLA_PACKED;

FULLA_PACKED_STRUCT_END

}
