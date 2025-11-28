/*
 * File: bpt_inode.hpp
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
    
    struct bpt_inode_header {
        word_u32 parent {0};
        word_u32 rightmost_child {0};
        void init() {
            parent = 0;
            rightmost_child = 0;
        }
    } FULLA_PACKED;

    struct bpt_inode_slot {
        word_u32 child {0};
        static typename word_u16::word_type key_offset() {
            return sizeof(bpt_inode_slot);
        }
    } FULLA_PACKED;

FULLA_PACKED_STRUCT_END
}
