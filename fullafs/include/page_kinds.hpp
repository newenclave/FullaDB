#pragma once

#include <numeric>
#include "fulla/core/pack.hpp"
#include "fulla/core/types.hpp"


namespace fullafs::page {
    enum class kind : std::uint16_t {
        superblock = 1,
        directory_header = 0x10,
        directory_inode = 0x11,
        directory_leaf = 0x12,

        file_header = 0x20,
        file_chunk = 0x21,

        freed = std::numeric_limits<std::uint16_t>::max() - 1,
    };
}

