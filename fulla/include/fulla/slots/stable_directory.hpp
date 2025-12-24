/*
 * File: slots/stable_directory.hpp
 * Author: newenclave
 * GitHub: https://github.com/newenclave
 * Created: 2025-12-23
 * License: MIT
 */

 #pragma once

#include <cassert>
#include "fulla/core/types.hpp"
#include "fulla/core/pack.hpp"
#include "fulla/core/bitset.hpp"

#include "fulla/page/slots.hpp"
#include "fulla/slots/concepts.hpp"

namespace fulla::slots { 

    using core::word_u16;
    using core::byte;
    using core::byte_span;
    using core::byte_view;
    using core::byte_buffer;
    
    template <typename SpanT>
        requires(std::same_as<SpanT, core::byte_view> || std::same_as<SpanT, core::byte_span>)
    struct stable_directory_view {
        
        constexpr static bool is_const = std::same_as<SpanT, core::byte_view>;

        using span_type = SpanT;
        using bitset_type = core::bitset<core::word_u32, SpanT>;

        using header_ptr = std::conditional_t<
            is_const,
            const page::stable_header *,
            page::stable_header *
        >;

        stable_directory_view(span_type body) 
            : body_(body) 
        {}
        
        std::optional<std::uint16_t> find_available() const {
            if (const auto fa = get_bitset().find_zero_bit()) {
                return { static_cast<std::uint16_t>(*fa) };
            }
            return std::nullopt;
        }

        std::size_t size() const noexcept {
            return get_bitset().popcount();
        }

        std::size_t capacity() const noexcept {
            return header()->capacity.get();
        }

        bool erase(std::size_t id) {
            auto bs = get_bitset();
            if (id < bs.bits_count() && bs.test(id)) {
                bs.clear(id);
                return true;
            }
            return false;
        }

        bool test(std::size_t id) const noexcept {
            auto bs = get_bitset();
            if (id < bs.bits_count() ) {
                return bs.test(id);
            }
            return false;
        }

        bool set(std::size_t id, core::byte_view data) {
            auto bs = get_bitset();
            auto hdr = header();
            const auto obj_size = hdr->size.get();

            if (data.size() <= obj_size && id < bs.bits_count()) {
                const auto pos = (id * obj_size);
                auto values = get_slots();
                std::memcpy(values.data() + pos, data.data(), data.size());
                bs.set(id);
                return true;
            }
            return false;
        }

        core::byte_span get(std::size_t id) {

            auto bs = get_bitset();
            auto hdr = header();
            const auto obj_size = hdr->size.get();

            if (id < bs.bits_count() && bs.test(id)) {
                const auto pos = (id * obj_size);
                auto values = get_slots();
                return { values.data() + pos, obj_size };
            }
            return {};
        }

        core::byte_view get(std::size_t id) const {
            auto bs = get_bitset();
            auto hdr = header();
            const auto obj_size = hdr->size.get();

            if (id < bs.bits_count() && bs.test(id)) {
                const auto pos = (id * obj_size);
                auto values = get_slots();
                return { values.data() + pos, obj_size };
            }
            return {};
        }

        void init(std::size_t objsize) {

            const auto available_size = (body_.size() - sizeof(page::stable_header));

            auto [bitmap_words, cap] = core::max_objects_by_words<std::uint32_t>(available_size, objsize);
            auto hdr = header();
            hdr->size = static_cast<std::uint16_t>(objsize);
            hdr->capacity = static_cast<std::uint16_t>(cap);
            hdr->bitmask_words = static_cast<std::uint16_t>(bitmap_words);

            get_bitset().reset();
        }

    private:

        bitset_type get_bitset() noexcept {
            auto bss = bitset_size();
            return { body_.subspan(sizeof(page::stable_header), bss), header()->capacity.get()};
        }

        bitset_type get_bitset() const noexcept {
            auto bss = bitset_size();
            return { body_.subspan(sizeof(page::stable_header), bss), header()->capacity.get() };
        }

        page::stable_header* header() noexcept {
            return reinterpret_cast<page::stable_header*>(body_.data());
        }

        const page::stable_header* header() const noexcept {
            return reinterpret_cast<const page::stable_header*>(body_.data());
        }

        std::size_t bitset_size() const {
            return header()->bitmask_words * sizeof(std::uint32_t);
        }

        std::size_t total_header_size() const noexcept {
            return sizeof(page::stable_header) + bitset_size();
        }

        core::byte_view get_slots() const noexcept {
            return body_.subspan(total_header_size());
        }

        core::byte_span get_slots() noexcept {
            return body_.subspan(total_header_size());
        }

        span_type body_;
    };

};
