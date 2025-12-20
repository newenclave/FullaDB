/*
 * File: radix_level.hpp
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
    struct radix_value{
        word_u32 value { word_u32::max() };
        word_u32 gen {0};
        core::byte type{ 0 };
        core::byte reserved[3] { };
        void init() {
            value = word_u32::max();
            gen = 0;
            type = core::byte{ 0 };
            reserved[0] = core::byte{ 0 };
            reserved[1] = core::byte{ 0 };
            reserved[2] = core::byte{ 0 };
        }
    } FULLA_PACKED;
    
    struct radix_level_header {
        word_u32 parent { word_u32::max() };
        word_u16 level{ 0 };
        word_u16 factor{ 0x100 };
        void init(word_u32::word_type p, word_u16::word_type l) {
            parent = p;
            level = l;
        }
    } FULLA_PACKED;
    FULLA_PACKED_STRUCT_END
};
