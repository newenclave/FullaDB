#pragma once

#include "fulla/core/types.hpp"
#include "fulla/page/header.hpp"
#include "fulla/page/page_view.hpp"
#include "fulla/page_allocator/concepts.hpp"
#include "fulla/slots/directory.hpp"

namespace fullafs::storage {

    template <fulla::page_allocator::concepts::PageAllocator AllT, typename SubheaderT>
    struct handle_base {
        
        using subheader_type = SubheaderT;
        using pid_type = typename AllT::pid_type;
        using page_handle = AllT::page_handle;

        using empty_slot_directory = fulla::slots::empty_directory_view;
        using cpage_view_type = fulla::page::const_page_view<empty_slot_directory>;
        using page_view_type = fulla::page::page_view<empty_slot_directory>;

        handle_base() = default;
        handle_base(handle_base&&) = default;
        handle_base& operator=(handle_base&&) = default;
        handle_base(const handle_base&) = default;
        handle_base& operator=(const handle_base&) = default;

        handle_base(page_handle ph)
            : hdl_(std::move(ph))
        {
        }

        auto pid() const noexcept {
            return handle().pid();
        }

        page_handle handle() const noexcept {
            return hdl_;
        }
        auto get() const {
            cpage_view_type pv{ handle().ro_span()};
            return pv.subheader<subheader_type>();
        }

        auto get() {
            page_view_type pv{ handle().rw_span() };
            return pv.subheader<subheader_type>();
        }

        bool is_valid() const {
            return handle().is_valid();
        }

        void mark_dirty() {
            hdl_.mark_dirty();
        }

        operator bool() const noexcept {
            return is_valid();
        }

    protected:
        page_handle hdl_;
    };
}
