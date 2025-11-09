/*
 * File: ranges.hpp
 * Author: newenclave
 * GitHub: https://github.com/newenclave
 * Created: 2025-10-25
 * License: MIT
 */

#pragma once

#include <compare>
#include "fulla/core/bytes.hpp"
#include "fulla/page/slot_page.hpp"
#include "fulla/codec/data_view.hpp"

namespace fulla::page {

    using core::byte_view;

    // Strict-weak ordering for serialized records using data_view::compare.
    // Unordered -> not-less (defensive: malformed data won't break algorithms).
    struct record_less {
        bool operator()(byte_view a, byte_view b) const noexcept {
            const auto ord = fulla::codec::data_view::compare(a, b);
            return std::is_lt(ord);
        }
    };

    // Projection: slot_entry -> byte_view through a page_view.
    // Stores a pointer to page_view; safe as long as the page_view outlives this functor.
    template<slots::SlotDirectoryConcept SdT>
    struct slot_projection {
        const page_view<SdT>* pv{nullptr};

        explicit slot_projection(const page_view<SdT>& ref) noexcept : pv(&ref) {}

        byte_view operator()(const slots::slot_entry& se) const noexcept {
            auto const result = pv->get_slots_dir().get_slot(se);
            return result;
        }
    };

    // Helpers for convenience
    inline record_less make_record_less() noexcept {
        return record_less{};
    }

    template<slots::SlotDirectoryConcept SdT>
    inline slot_projection<SdT> make_slot_projection(const page_view<SdT>& pv) noexcept {
        return slot_projection{pv};
    }

} // namespace fulla::page
