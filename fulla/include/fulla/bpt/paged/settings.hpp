/*
 * File: settings.hpp
 * Author: newenclave
 * GitHub: https://github.com/newenclave
 * Created: 2025-11-23
 * License: MIT
 */

#pragma once

#include <cstdint>
#include "fulla/page/header.hpp"

namespace fulla::bpt::paged {
    struct settings {
        std::size_t inode_minimum_slot_size = 4; 
        std::size_t inode_maximum_slot_size = 200; 
        std::size_t leaf_minimum_slot_size = 4; 
        std::size_t leaf_maximum_slot_size = 200; 
        std::uint16_t leaf_kind_value = static_cast<std::uint16_t>(page::page_kind::bpt_leaf);
        std::uint16_t inode_kind_value = static_cast<std::uint16_t>(page::page_kind::bpt_inode);
        std::uint16_t root_kind_value = static_cast<std::uint16_t>(page::page_kind::bpt_root);
    };
    static_assert(settings{}.leaf_kind_value != settings{}.inode_kind_value, "values must be different");
    static_assert(settings{}.leaf_kind_value != settings{}.root_kind_value, "values must be different");
    static_assert(settings{}.inode_kind_value != settings{}.root_kind_value, "values must be different");
}
