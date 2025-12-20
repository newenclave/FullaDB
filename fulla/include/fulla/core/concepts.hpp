/*
 * File: core/concepts.hpp
 * Author: newenclave
 * GitHub: https://github.com/newenclave
 * Created: 2025-12-12
 * License: MIT
 */

#pragma once
#include <concepts>
namespace fulla::core::concepts {
    template <typename T>
    concept HasInit = requires(T t) {
        { t.init() } -> std::same_as<void>;
    };

    template<typename T>
    concept RootManager = requires(T t, typename T::root_type id) {
        typename T::root_type;
        { t.get_root() } -> std::convertible_to<typename T::root_type>;
        { t.set_root(id) } -> std::same_as<void>;
        { t.has_root() } -> std::convertible_to<bool>;
    };

} // namespace fulla::core
