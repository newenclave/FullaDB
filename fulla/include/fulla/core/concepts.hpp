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

    template<typename T, typename NodeIdT>
    concept RootManager = requires(T t, NodeIdT id) {
        { t.get_root() } -> std::convertible_to<NodeIdT>;
        { t.set_root(id) } -> std::same_as<void>;
        { t.has_root() } -> std::convertible_to<bool>;
    };

} // namespace fulla::core
