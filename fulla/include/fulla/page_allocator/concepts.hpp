/*
 * File: page_allocator/concept.hpp
 * Author: newenclave
 * GitHub: https://github.com/newenclave
 * Created: 2025-12-10
 * License: MIT
 */

#pragma once
#include <concepts>
#include "fulla/storage/block_device.hpp"

namespace fulla::page_allocator::concepts {

    template <typename PhT>
    concept PageHandle = requires (PhT ph, typename PhT::pid_type pid) {
        typename PhT::pid_type;
        { ph.is_valid() } -> std::convertible_to<bool>;
        { ph.pid() } -> std::convertible_to<typename PhT::pid_type>;
        { ph.mark_dirty() } -> std::same_as<void>;
        { ph.rw_span() } -> std::convertible_to<fulla::core::byte_span>;
        { ph.ro_span() } -> std::convertible_to<fulla::core::byte_view>;
    };

    template <typename T>
    concept PageAllocator = requires (T allocator,
                                      typename T::pid_type pid,
                                      std::size_t n) {
        typename T::pid_type;
        typename T::page_handle;
        typename T::underlying_device_type;

        requires PageHandle<typename T::page_handle>;

        { allocator.invalid_pid } -> std::convertible_to<typename T::pid_type>;
        { allocator.underlying_device() } -> std::convertible_to<typename T::underlying_device_type&>;

        { allocator.valid_id(pid) } -> std::convertible_to<bool>;
        { allocator.page_size() } -> std::convertible_to<std::size_t>;
        { allocator.allocate() } -> std::convertible_to<typename T::page_handle>;
        { allocator.fetch(pid) } -> std::convertible_to<typename T::page_handle>;
        { allocator.destroy(pid) } -> std::same_as<void>;
        { allocator.flush(pid) } -> std::same_as<void>;
        { allocator.flush_all() } -> std::same_as<void>;
    };
}
