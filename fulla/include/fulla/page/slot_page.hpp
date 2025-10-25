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

namespace fulla::page {

    using core::word_u16;
    using core::byte;
    using core::byte_span;
    using core::byte_view;
    using core::byte_buffer;

    struct page_view {
        struct slot_entry {
            word_u16 off{};
            word_u16 len{};
        };
        using slot_span = std::span<slot_entry>;
        using slot_view = std::span<const slot_entry>;

        // Construct from page buffer
        explicit page_view(byte_buffer& page) : page_(page.data(), page.size()) {}
        explicit page_view(byte_span page) : page_(page) {}
        page_view() = default;
        page_view(const page_view&) = default;
        page_view& operator=(const page_view&) = default;

        byte_span get() const { return page_; }

        static constexpr std::uint16_t SLOT_MAX  = 0xFFFF;
        static constexpr std::uint16_t SLOT_FREE = 0xFFFF;
        static constexpr std::uint16_t ALIGN_SIZE = 4;

        struct insert_result {
            bool ok{false};
            std::uint16_t slot_id{SLOT_FREE};
        };

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

        // Slot directory
        slot_entry* slot_dir() {
            return reinterpret_cast<slot_entry*>(page_.data() + header().slots_offset);
        }
        const slot_entry* slot_dir() const {
            return reinterpret_cast<const slot_entry*>(page_.data() + header().slots_offset);
        }

        slot_span slot_dir_view() {
            return { slot_dir(), slot_count() };
        }
        slot_view slot_dir_view() const {
            return { slot_dir(), slot_count() };
        }

        std::uint16_t slot_dir_size() const {
            return static_cast<std::uint16_t>(slot_count() * sizeof(slot_entry));
        }

        std::uint16_t slot_count() const { return header().slots; }
        std::uint16_t free_beg()   const { return header().free_beg; }
        std::uint16_t free_end()   const { return header().free_end; }

        std::uint16_t free_space() const {
            return static_cast<std::uint16_t>(free_end() - free_beg());
        }

        std::uint16_t free_space_if_compacted() const {
            std::uint32_t live = 0;
            const auto* dir = slot_dir();
            for (std::uint16_t i = 0; i < slot_count(); ++i) {
                live += core::align_up(static_cast<std::uint16_t>(dir[i].len), ALIGN_SIZE);
            }
            const std::uint16_t fbegin = static_cast<std::uint16_t>(
                sizeof(page_header) + header().subhdr_size + slot_dir_size()
            );
            const std::uint16_t fend = static_cast<std::uint16_t>(page_.size() - live);
            return (fend > fbegin) ? static_cast<std::uint16_t>(fend - fbegin) : 0;
        }

        int find_hole_slot() const {
            const auto* dir = slot_dir();
            for (std::uint16_t i = 0; i < slot_count(); ++i) {
                if ((dir[i].off == SLOT_FREE) && (dir[i].len == 0)) {
                    return static_cast<int>(i);
                }
            }
            return -1;
        }

        // Insert at the end (or reuse a hole)
        insert_result insert(byte_view rec) {
            auto& h = header();
            auto* dir = slot_dir();

            if (rec.size() > SLOT_MAX) {
                return { false, SLOT_MAX };
            }

            const std::uint16_t data_len = static_cast<std::uint16_t>(rec.size());
            const std::uint16_t data_size = core::align_up(data_len, ALIGN_SIZE);

            const int hole = find_hole_slot();
            const std::uint16_t need = required_space(data_len, hole >= 0);
            if (need == SLOT_MAX) {
                return { false, SLOT_MAX };
            }

            if (free_space() < need) {
                if (free_space_if_compacted() < need) {
                    return { false, SLOT_MAX };
                }
                compact();
            }

            h.free_end = static_cast<word_u16::word_type>(h.free_end - data_size);
            std::memcpy(page_.data() + h.free_end, rec.data(), rec.size());

            std::uint16_t sid;
            if (hole >= 0) {
                sid = static_cast<std::uint16_t>(hole);
            } else {
                sid = h.slots;
                h.slots = static_cast<word_u16::word_type>(h.slots + 1);
                h.free_beg = static_cast<word_u16::word_type>(h.free_beg + sizeof(slot_entry));
            }

            dir[sid].off = h.free_end;
            dir[sid].len = data_len;
            return { true, sid };
        }

        // Insert at specific index (dense shift)
        insert_result insert_at_dense(std::uint16_t i, byte_view rec) {
            auto& h = header();

            if (i > h.slots) {
                i = h.slots;
            }
            if (rec.size() > SLOT_MAX) {
                return { false, SLOT_MAX };
            }

            const std::uint16_t data_len = static_cast<std::uint16_t>(rec.size());
            const std::uint16_t data_size = core::align_up(data_len, ALIGN_SIZE);

            const std::uint16_t need = static_cast<std::uint16_t>(data_size + sizeof(slot_entry));
            if (free_space() < need) {
                if (free_space_if_compacted() < need) {
                    return { false, SLOT_MAX };
                }
                compact();
            }

            h.free_end = static_cast<word_u16::word_type>(h.free_end - data_size);
            std::memcpy(page_.data() + h.free_end, rec.data(), data_len);

            auto* dir = slot_dir();
            h.slots = static_cast<word_u16::word_type>(h.slots + 1);
            h.free_beg = static_cast<word_u16::word_type>(h.free_beg + sizeof(slot_entry));

            for (std::uint16_t j = static_cast<std::uint16_t>(h.slots - 1); j > i; --j) {
                dir[j] = dir[static_cast<std::uint16_t>(j - 1)];
            }
            dir[i].off = h.free_end;
            dir[i].len = data_len;

            return { true, i };
        }

        // Read slot content (validated)
        byte_view get_slot(const slot_entry& slot) const {
            const auto& h = header();
            const std::size_t page_sz = page_.size();

            if (slot.off == SLOT_FREE || slot.len == 0) {
                return {};
            }

            const std::size_t off = static_cast<std::size_t>(slot.off);
            const std::size_t len = static_cast<std::size_t>(slot.len);

            if (off < h.free_end) {
                return {};
            }
            if (off + len > page_sz) {
                return {};
            }
            const std::size_t low_limit =
                sizeof(page_header) + static_cast<std::size_t>(h.subhdr_size) + slot_dir_size();

            if (off < low_limit) {
                return {};
            }

            return { page_.data() + off, len };
        }

        byte_view get_slot(std::uint16_t sid) const {
            const auto* dir = slot_dir();
            if (sid >= slot_count()) {
                return {};
            }
            const auto off = dir[sid].off;
            const auto len = dir[sid].len;
            if (off == SLOT_FREE || len == 0) {
                return {};
            }
            return { page_.data() + off, len };
        }

        bool can_insert(std::uint16_t payload_len, bool has_hole_slot) const {
            const auto need = required_space(payload_len, has_hole_slot);
            return (need != SLOT_MAX) && (free_space() >= need);
        }

        std::uint16_t required_space(std::uint16_t payload_len, bool has_hole_slot) const {
            const std::uint32_t data_sz32 = core::align_up(payload_len, ALIGN_SIZE);
            if (data_sz32 > SLOT_MAX) {
                return SLOT_MAX;
            }
            const std::uint16_t data_sz = static_cast<std::uint16_t>(data_sz32);
            const std::uint16_t slot_sz = has_hole_slot ? 0 : static_cast<std::uint16_t>(sizeof(slot_entry));
            return static_cast<std::uint16_t>(data_sz + slot_sz);
        }

        // Remove free tail slots to keep directory dense at the end
        void shrink_slot_dir() {
            auto& h = header();
            auto* dir = slot_dir();

            while (h.slots > 0) {
                const std::uint16_t i = static_cast<std::uint16_t>(h.slots - 1);
                if (dir[i].off == SLOT_FREE && dir[i].len == 0) {
                    h.slots = static_cast<word_u16::word_type>(h.slots - 1);
                    h.free_beg = static_cast<word_u16::word_type>(h.free_beg - sizeof(slot_entry));
                } else {
                    break;
                }
            }
        }

        // Compact payload region (move live records to the end; update offsets)
        void compact() {
            auto& h = header();
            auto* dir = slot_dir();

            struct item { std::uint16_t sid; std::uint16_t off; std::uint16_t len; };
            std::vector<item> live;
            live.reserve(slot_count());

            for (std::uint16_t i = 0; i < slot_count(); ++i) {
                if ((dir[i].off != SLOT_FREE) && (dir[i].len != 0)) {
                    live.push_back({ i, dir[i].off, dir[i].len });
                }
            }

            if (live.empty()) {
                h.free_end = static_cast<word_u16::word_type>(page_.size());
                return;
            }

            std::ranges::sort(live, std::less<std::uint16_t>{}, &item::off);

            std::uint16_t cursor = static_cast<std::uint16_t>(page_.size());
            for (auto it = live.rbegin(); it != live.rend(); ++it) {
                const std::uint16_t padded = core::align_up(it->len, ALIGN_SIZE);
                cursor = static_cast<std::uint16_t>(cursor - padded);
                std::memmove(page_.data() + cursor, page_.data() + it->off, it->len);
                dir[it->sid].off = cursor;
                dir[it->sid].len = it->len;
            }

            h.free_end = cursor;
            assert(h.free_beg <= h.free_end);
        }

        // Validate structural invariants
        bool validate() const {
            const auto& h = header();
            if (!((sizeof(page_header) <= h.free_beg) &&
                  (h.free_beg <= h.free_end) &&
                  (h.free_end <= page_.size()))) {
                return false;
            }

            const auto* dir = slot_dir();
            for (std::uint16_t i = 0; i < slot_count(); ++i) {
                const std::uint16_t off = dir[i].off;
                const std::uint16_t len = dir[i].len;

                if ((off == SLOT_FREE) && (len == 0)) {
                    continue;
                }
                if (off < h.free_end) {
                    return false;
                }
                if (static_cast<std::size_t>(off) + static_cast<std::size_t>(len) > page_.size()) {
                    return false;
                }
            }

            const std::uint16_t expected_fb =
                static_cast<std::uint16_t>(sizeof(page_header) + h.subhdr_size + slot_dir_size());

            if (h.free_beg != expected_fb) {
                return false;
            }

            if (h.slots_offset != static_cast<std::uint16_t>(sizeof(page_header) + h.subhdr_size)) {
                return false;
            }

            if (h.free_beg < h.slots_offset) {
                return false;
            }

            return true;
        }

        // Erase slot i (dense: shift left)
        void erase_dense(std::uint16_t slot_id) {
            erase(slot_id);
        }

        void erase(std::uint16_t slot_id) {
            auto& h = header();
            if (slot_id >= h.slots) {
                return;
            }
            auto* dir = slot_dir();

            for (std::uint16_t j = static_cast<std::uint16_t>(slot_id + 1); j < h.slots; ++j) {
                dir[static_cast<std::uint16_t>(j - 1)] = dir[j];
            }
            h.slots = static_cast<word_u16::word_type>(h.slots - 1);
            h.free_beg = static_cast<word_u16::word_type>(h.free_beg - sizeof(slot_entry));
        }

        // Update: can re-place record at the end if needed
        bool update(std::uint16_t sid, byte_view rec) {
            if (sid >= slot_count()) {
                return false;
            }
            if (rec.size() > SLOT_MAX) {
                return false;
            }

            auto& h = header();
            auto* dir = slot_dir();

            const std::uint16_t data_len = static_cast<std::uint16_t>(rec.size());
            const std::uint16_t data_size = core::align_up(data_len, ALIGN_SIZE);

            if (static_cast<std::uint16_t>(h.free_end - h.free_beg) < data_size) {
                return false;
            }

            h.free_end = static_cast<word_u16::word_type>(h.free_end - data_size);
            std::memcpy(page_.data() + h.free_end, rec.data(), rec.size());

            dir[sid].off = h.free_end;
            dir[sid].len = data_len;
            return true;
        }

        // Try in-place update if new padded size fits into old padded size
        bool try_update_in_place(std::uint16_t i, byte_view rec) {
            auto& h = header();
            if (i >= h.slots) {
                return false;
            }
            auto* dir = slot_dir();

            const std::uint16_t old_len = dir[i].len;
            const std::uint16_t old_pad = core::align_up(old_len, ALIGN_SIZE);
            const std::uint16_t new_len = static_cast<std::uint16_t>(rec.size());
            const std::uint16_t new_pad = core::align_up(new_len, ALIGN_SIZE);
            if (new_pad > old_pad) {
                return false;
            }

            std::memcpy(page_.data() + dir[i].off, rec.data(), new_len);
            dir[i].len = new_len;
            return true;
        }

        // Dense update: fallback to append-at-end if in-place fails
        bool update_dense(std::uint16_t i, byte_view rec) {
            if (try_update_in_place(i, rec)) {
                return true;
            }

            auto& h = header();
            if (rec.size() > SLOT_MAX) {
                return false;
            }
            const std::uint16_t data_len = static_cast<std::uint16_t>(rec.size());
            const std::uint16_t data_size = core::align_up(data_len, ALIGN_SIZE);

            if (free_space() < data_size) {
                if (free_space_if_compacted() < data_size) {
                    return false;
                }
                compact();
            }
            h.free_end = static_cast<word_u16::word_type>(h.free_end - data_size);
            std::memcpy(page_.data() + h.free_end, rec.data(), data_len);

            auto* dir = slot_dir();
            dir[i].off = h.free_end;
            dir[i].len = data_len;
            return true;
        }

        void debug_print() const {
            const auto& h = header();
            // std::println("slots={} free_beg={} free_end={} size={}", (unsigned)h.slots, (unsigned)h.free_beg, (unsigned)h.free_end, page_.size());
            (void)h;
        }

    private:
        byte_span page_{};
    };

} // namespace fulla::page
