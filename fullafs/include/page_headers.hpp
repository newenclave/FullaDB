#pragma once

#include "fulla/core/pack.hpp"
#include "fulla/core/types.hpp"

namespace fullafs::page {

    using fulla::core::word_u16;
    using fulla::core::word_u32;
    using pid_type = word_u32;

FULLA_PACKED_STRUCT_BEGIN

    struct entry_descriptor {
        word_u16 kind{ word_u16::max() };
        word_u16 reserved{ word_u16::max() };
        pid_type page{ pid_type::max() };
    } FULLA_PACKED;

    struct directory_header {
        pid_type parent{ pid_type::max() };
        pid_type entry_root { pid_type::max() };
        word_u32 total_entries{ 0 };
        void init(pid_type::word_type parent_pid) {
            parent = parent_pid;
            entry_root = pid_type::max();
            total_entries = 0;
        }
    } FULLA_PACKED;

    struct file_metadata {
        pid_type parent{ pid_type::max() };
        word_u32 reserved[31];
    } FULLA_PACKED;

    static_assert(sizeof(file_metadata) == 128, "Something is wrong; Check the file_metadata content");

    struct superblock {
        static constexpr std::size_t current_version = 1;
        word_u32 version{ current_version };
        pid_type first_freed_page{ pid_type::max() };
        entry_descriptor root;
        word_u32 total_pages{ 0 };

        void init() {
            version = current_version;
            root.page = pid_type::max();
            root.kind = word_u16::max();
            first_freed_page = pid_type::max();
            total_pages = 0;
        }
    } FULLA_PACKED;

    struct freed {
        pid_type next{ pid_type::max() };
        void init() {
            next = pid_type::max();
        }
    } FULLA_PACKED;

FULLA_PACKED_STRUCT_END
}