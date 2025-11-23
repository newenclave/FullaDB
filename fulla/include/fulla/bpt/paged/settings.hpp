/*
 * File: settings.hpp
 * Author: newenclave
 * GitHub: https://github.com/newenclave
 * Created: 2025-11-23
 * License: MIT
 */

#pragma once

#include <cstdint>

namespace fulla::bpt::paged {
    struct settings {
        std::size_t inode_minimum_slot_size = 5; 
        std::size_t inode_maximum_slot_size = 200; 
        std::size_t leaf_minimum_slot_size = 5; 
        std::size_t leaf_maximum_slot_size = 200; 
    };
}
