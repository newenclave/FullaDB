#pragma once
#include "fulla/core/types.hpp"

namespace fullafs::core {

    using fulla::core::byte_span;
    using fulla::core::byte_view;
    using fulla::core::byte;

    enum class name_type : std::uint8_t {
        file = 1,
        directory = 2,
    };

    inline static core::byte_view as_byte_view(const std::string& name) {
        return { reinterpret_cast<const core::byte *>(name.data()), name.size() };
    }

    inline static std::string make_directory_descriptor() {
        std::string data(sizeof(page::entry_descriptor), '\0');
        return data;
    }

    inline static auto make_directory_name(const std::string& name) {
        return name;
    }

    inline static auto make_file_name(const std::string &name) {
        return name;
    }

    struct path_string_less {
        bool operator ()(byte_view a, byte_view b) const noexcept {
            return std::is_lt(compare(a, b));
        }
        auto compare(byte_view a, byte_view b) const noexcept {
            return std::lexicographical_compare_three_way(
                a.begin(), a.end(),
                b.begin(), b.end()
            );
        }
    };
}