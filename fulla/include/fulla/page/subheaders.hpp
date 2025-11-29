/*
 * File: subheaders.hpp
 * Author: newenclave
 * GitHub: https://github.com/newenclave
 * Created: 2025-11-02
 * License: MIT
 */

#pragma once

#include "fulla/core/types.hpp"
#include "fulla/page/header.hpp"
#include "fulla/page/slot_page.hpp"

namespace fulla::page {

    using pid_type = core::word_u32;
    constexpr static const auto invalid_pid = static_cast<typename pid_type::word_type>(0xFFFFFFFFu);

    FULLA_PACKED_STRUCT_BEGIN
    struct btree_root_subheader {
        pid_type root{ invalid_pid };
        void set_root(typename pid_type::word_type pid) {
            root = pid;
        }
        typename pid_type::word_type get_root() const noexcept{
            return static_cast<typename pid_type::word_type>(root);
        }
    } FULLA_PACKED;
    FULLA_PACKED_STRUCT_END

    FULLA_PACKED_STRUCT_BEGIN
    struct free_page_subheader {
        pid_type prev{ invalid_pid };
        pid_type next{ invalid_pid };
    } FULLA_PACKED;
    FULLA_PACKED_STRUCT_END
}
