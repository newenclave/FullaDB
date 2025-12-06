/*
 * File: slot_page.hpp
 * Author: newenclave
 * GitHub: https://github.com/newenclave
 * Created: 2025-10-25
 * License: MIT
 */

#pragma once

#include <cstdint>
#include <cstring>
#include <vector>
#include <span>
#include <algorithm>
#include <cassert>

#include "fulla/core/bytes.hpp"
#include "fulla/core/types.hpp"
#include "fulla/page/header.hpp"
#include "fulla/page/slot_directory.hpp"

namespace fulla::page {

    using core::word_u16;
    using core::byte;
    using core::byte_span;
    using core::byte_view;
    using core::byte_buffer;

    template<slots::SlotDirectoryConcept SdT, typename SpanT>
        requires(std::same_as<SpanT, byte_span> || std::same_as<SpanT, byte_view>)
    struct page_view_common {

        // Construct from page buffer
        explicit page_view_common(byte_buffer& page) : page_(page.data(), page.size()) {}
        explicit page_view_common(SpanT page) : page_(page) {}
        page_view_common() = default;
        page_view_common(const page_view_common&) = default;
        page_view_common& operator=(const page_view_common&) = default;

        SpanT get() const { return page_; }

        // Header access
        auto& header() { 
            if constexpr (std::same_as<SpanT, byte_span>) {
                return *reinterpret_cast<page_header*>(page_.data());
            }
            else {
                return *reinterpret_cast<const page_header*>(page_.data());
			}
        }
        const page_header& header() const { return *reinterpret_cast<const page_header*>(page_.data()); }

        // Optional typed subheader
        template <class SubHdrT>
        auto subheader() {
            if constexpr (std::same_as<SpanT, byte_span>) {
                return reinterpret_cast<SubHdrT*>(page_.data() + sizeof(page_header));
            }
            else {
                return reinterpret_cast<const SubHdrT*>(page_.data() + sizeof(page_header));
            }
        }
        template <class SubHdrT>
        const SubHdrT* subheader() const {
            return reinterpret_cast<const SubHdrT*>(page_.data() + sizeof(page_header));
        }

        std::size_t size() const noexcept {
            return page_.size();
        }

        word_u16::word_type base_off() const {
            return static_cast<word_u16::word_type>(headers_len());
        }

        auto* base_ptr() {
            return page_.data() + headers_len();
        }

        const byte* base_ptr() const {
            return page_.data() + headers_len();
        }

        std::size_t capacity() const {
            return header().page_end - headers_len();
        }

        SdT get_slots_dir() {
            return SdT(get_slot_directory());
        }

        SdT get_slots_dir() const {
            return SdT(get_slot_directory());
        }

    private:

        std::size_t headers_len() const {
            const auto& h = header();
            return sizeof(page_header) + h.subhdr_size;
        }

        SpanT get_slot_directory() const {
            return { page_.data() + headers_len(),  capacity() };
        }

        SpanT page_{};
    };

    template <slots::SlotDirectoryConcept SdT>
    using page_view = page_view_common<SdT, byte_span>;

    template <slots::SlotDirectoryConcept SdT>
    using const_page_view = page_view_common<SdT, byte_view>;

} // namespace fulla::page
