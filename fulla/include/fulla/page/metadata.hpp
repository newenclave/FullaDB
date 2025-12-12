/*
 * File: page/metadata.hpp
 * Author: newenclave
 * GitHub: https://github.com/newenclave
 * Created: 2025-12-1
 * License: MIT
 */

#pragma once

namespace fulla::page {

    template <typename T>
    concept EmptyMetadata = requires {
        typename T::empty_tag;
    };

    struct empty_metadata { struct empty_tag {}; };

    template <typename MdT>

    constexpr inline std::size_t metadata_size() noexcept {
        if constexpr (EmptyMetadata<MdT>) {
            return 0;
        }
        else{
            return sizeof(MdT);
        }
    }
}
