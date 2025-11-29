/*
 * File: bpt_leaf.hpp
 * Author: newenclave
 * GitHub: https://github.com/newenclave
 * Created: 2025-11-29
 * License: MIT
 */

#pragma once

#include "fulla/core/pack.hpp"
#include "fulla/core/types.hpp"

namespace fulla::page {

    using core::word_u16;
    using core::word_u32;
    using pid_type = word_u32;

FULLA_PACKED_STRUCT_BEGIN

    struct freed {
        pid_type next{ pid_type::max() };
        void init() {
            next = pid_type::max();
        }
    } FULLA_PACKED;

FULLA_PACKED_STRUCT_END

}
