/*
 * File: page/slab_store.hpp
 * Author: newenclave
 * GitHub: https://github.com/newenclave
 * Created: 2025-12-23
 * License: MIT
 */

#pragma once

#include "fulla/core/pack.hpp"
#include "fulla/core/types.hpp"

namespace fulla::page {

    using fulla::core::word_u32;
    using pid_type = word_u32;

FULLA_PACKED_STRUCT_BEGIN

        struct slab_storage {
        pid_type prev{ pid_type::max() };
        pid_type next{ pid_type::max() };
        void init() {
            prev = pid_type::max();
            next = pid_type::max();
        }
    } FULLA_PACKED;

FULLA_PACKED_STRUCT_END

}