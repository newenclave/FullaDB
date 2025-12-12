/*
 * File: page_allocator/base.hpp
 * Author: newenclave
 * GitHub: https://github.com/newenclave
 * Created: 2025-12-10
 * License: MIT
 */

#pragma once

#include <concepts>
#include <cstdint>

#include "fulla/storage/block_device.hpp"
#include "fulla/storage/buffer_manager.hpp"
#include "fulla/page_allocator/concepts.hpp"

namespace fulla::page_allocator {

    using namespace fulla::storage;

    template <RandomAccessBlockDevice RadT, typename PidT = std::uint32_t>
    class base {
    public:
        using pid_type = PidT;
        using underlying_device_type = RadT;
        using buffer_manager_type = storage::buffer_manager<RadT, PidT>;
        using page_handle = typename buffer_manager_type::page_handle;

        constexpr static const pid_type invalid_pid = std::numeric_limits<pid_type>::max();

        base(underlying_device_type &device, std::size_t maximum_pages)
            : mgr_(device, maximum_pages)
        {}

        virtual ~base() = default;

        underlying_device_type& underlying_device() { return *mgr_.device_; }
        std::size_t page_size() const noexcept { return mgr_.page_size(); }
        std::size_t pages_count() noexcept { return mgr_.pages_count(); }

        bool valid_id(pid_type pid) const noexcept { return mgr_.valid_id(pid); }
        auto fetch(pid_type pid) { return mgr_.fetch(pid); }
        void flush(pid_type pid) { return mgr_.flush(pid); }
        void flush_all() { return mgr_.flush_all(); }

        virtual page_handle allocate() { return mgr_.allocate(); }
        virtual void destroy(pid_type) {}

    private:
        buffer_manager_type mgr_;
    };
}
