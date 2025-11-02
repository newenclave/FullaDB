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
    constexpr static const std::size_t invalid_pid = static_cast<typename pid_type::word_type>(0xFFFFFFFFu);

    FULLA_PACKED_STRUCT_BEGIN
    struct btree_inode_subheader {
        pid_type parent_pid { invalid_pid }; // Parent node page id
        pid_type leftmost_child { invalid_pid }; // Leftmost child page id
        void init(pid_type::word_type parent, pid_type::word_type leftmost) {
            parent_pid = parent;
            leftmost_child = leftmost;
        }

    } FULLA_PACKED;
    FULLA_PACKED_STRUCT_END

    FULLA_PACKED_STRUCT_BEGIN
    struct btree_leaf_subheader {
        pid_type parent_pid { invalid_pid }; // Parent node page id
        pid_type prev_pid { invalid_pid };   // Previous leaf page id
        pid_type next_pid { invalid_pid };   // Next leaf page id
        void init(pid_type::word_type parent, pid_type::word_type prev, pid_type::word_type next) {
            parent_pid = parent;
            prev_pid = prev;
            next_pid = next;
        }
    } FULLA_PACKED;
    FULLA_PACKED_STRUCT_END

    inline void init_btree_inode(page::page_view &pv, pid_type::word_type parent, pid_type::word_type leftmost) {
        auto &hdr = pv.header();
        hdr.kind = page_kind::btree_inode;
        auto sub_hdr = pv.subheader<btree_inode_subheader>();
        sub_hdr->init(parent, leftmost);
    }

    inline void init_btree_leaf(page::page_view &pv, pid_type::word_type parent, pid_type::word_type prev, pid_type::word_type next) {
        auto &hdr = pv.header();
        hdr.kind = page_kind::btree_leaf;
        auto sub_hdr = pv.subheader<btree_leaf_subheader>();
        sub_hdr->init(parent, prev, next);
    }
}
