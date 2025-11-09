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

    template<slots::SlotDirectoryConcept SdT>
    struct page_view {

        // Construct from page buffer
        explicit page_view(byte_buffer& page) : page_(page.data(), page.size()) {}
        explicit page_view(byte_span page) : page_(page) {}
        page_view() = default;
        page_view(const page_view&) = default;
        page_view& operator=(const page_view&) = default;

        byte_span get() const { return page_; }

        // Header access
        page_header& header() { return *reinterpret_cast<page_header*>(page_.data()); }
        const page_header& header() const { return *reinterpret_cast<const page_header*>(page_.data()); }

        // Optional typed subheader
        template <class SubHdrT>
        SubHdrT* subheader() {
            return reinterpret_cast<SubHdrT*>(page_.data() + sizeof(page_header));
        }
        template <class SubHdrT>
        const SubHdrT* subheader() const {
            return reinterpret_cast<const SubHdrT*>(page_.data() + sizeof(page_header));
        }

        word_u16::word_type base_off() const {
            return static_cast<word_u16::word_type>(headers_len());
        }

        byte* base_ptr() {
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

        byte_span get_slot_directory() const {
            return { page_.data() + headers_len(),  capacity() };
        }

        byte_span page_{};
    };

} // namespace fulla::page
