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
} // namespace fulla::core
