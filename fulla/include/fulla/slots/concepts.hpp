/*
 * File: slot/concepts.hpp
 * Author: newenclave
 * GitHub: https://github.com/newenclave
 * Created: 2025-12-22
 * License: MIT
 */

 #pragma once

#include <cassert>
#include "fulla/core/types.hpp"
#include "fulla/core/pack.hpp"

namespace fulla::slots::concepts { 

    using core::word_u16;
    using core::byte;
    using core::byte_span;
    using core::byte_view;
    using core::byte_buffer;

    template <typename SdT>
    concept StableDirectory = requires(SdT sdt, std::size_t pos, byte_view bv, std::size_t len) {
        { sdt.size() } -> std::convertible_to<std::size_t>;
        { sdt.capacity() } -> std::convertible_to<std::size_t>;
        { sdt.erase(pos) } -> std::same_as<bool>;
        { sdt.set(pos, bv) } -> std::same_as<bool>;
        //{ sdt.get(pos) } -> std::same_as<byte_view>;
    };

    template <typename SdT>
    concept DenseDirectory = requires(SdT sdt, std::size_t pos, byte_view bv, std::size_t len) {
        typename SdT::slot_type;
        typename SdT::directory_header;

        { sdt.size() } -> std::convertible_to<std::size_t>;
        { sdt.capacity_for(std::size_t{}) } -> std::convertible_to<std::size_t>;
        { sdt.minumum_slot_size() } -> std::convertible_to<std::size_t>;
        { sdt.maximum_slot_size() } -> std::convertible_to<std::size_t>;

        { sdt.reserve_get(pos, len) } -> std::convertible_to<byte_span>;
        { sdt.update_get(pos, len) } -> std::convertible_to<byte_span>;

        { sdt.insert(pos, bv) } -> std::convertible_to<bool>;
        { sdt.update(pos, bv) } -> std::convertible_to<bool>;
        { sdt.erase(pos) } -> std::convertible_to<bool>;

        { sdt.can_insert(std::size_t{}) } -> std::convertible_to<bool>;
        { sdt.can_update(pos, std::size_t{}) } -> std::convertible_to<bool>;

        { sdt.available() } -> std::convertible_to<std::size_t>;
        { sdt.available_after_compact() } -> std::convertible_to<std::size_t>;

        { sdt.compact(std::span<typename SdT::slot_type*>{}) } -> std::convertible_to<bool>;

        { sdt.view() } -> std::convertible_to<std::span<const typename SdT::slot_type>>;
        { sdt.get_slot(std::size_t{}) } -> std::convertible_to<byte_span>;
    };

}