/*
 * File: slot_directory.hpp
 * Author: newenclave
 * GitHub: https://github.com/newenclave
 * Created: 2025-11-08
 * License: MIT
 */

 #pragma once

#include <cassert>
#include "fulla/core/types.hpp"
#include "fulla/core/pack.hpp"

namespace fulla::page::slots { 

    using core::word_u16;
    using core::byte;
    using core::byte_span;
    using core::byte_view;
    using core::byte_buffer;

    constexpr static const typename core::word_u16::word_type SLOT_INVALID = word_u16::max();
    constexpr static const auto SLOT_FREE = SLOT_INVALID;

    template <typename SdT>
    concept SlotDirectoryConcept = requires(SdT sdt, std::size_t pos, byte_view bv, std::size_t len) {
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

	enum class directory_type {
		variadic,
		fixed,
	};

    FULLA_PACKED_STRUCT_BEGIN
    struct slot_entry {
        word_u16 off;
        word_u16 len;
    };

    namespace fixed {
        struct header {
            word_u16 size{ 0 };         // one slot size
            word_u16 free_beg{ 0 };     // offset to the first free byte in payload area (grows upward)
            word_u16 free_end{ 0 };     // offset to the last free byte+1 from the end (grows downward)
            word_u16 freed;             // first freed slot.
        } FULLA_PACKED;

        struct free_slot_type {
            word_u16 next;
        } FULLA_PACKED;
    }

    namespace variadic {
        struct header {
            word_u16 slots{ 0 };        // number of occupied slot entries
            word_u16 free_beg{ 0 };     // offset to the first free byte in payload area (grows upward)
            word_u16 free_end{ 0 };     // offset to the last free byte+1 from the end (grows downward)
            word_u16 freed;             // first freed slot.
        } FULLA_PACKED;

        struct free_slot_type {
            word_u16 prev;
            word_u16 next;
            word_u16 len;
        } FULLA_PACKED;
    }
    FULLA_PACKED_STRUCT_END

    template <directory_type Type, std::size_t AlignV = 4>
    struct directory_view {

    public:

        using word16_type = word_u16::word_type;

        constexpr static const std::size_t align_val = AlignV;
        constexpr static const bool is_fixed = (Type == directory_type::fixed);

        using slot_type = slot_entry;

        using free_slot_type = std::conditional_t <is_fixed,
            fixed::free_slot_type,
            variadic::free_slot_type
        >;

        using directory_header = std::conditional_t<is_fixed,
            fixed::header,
            variadic::header
        >;

        static_assert(sizeof(slot_type) == 4, "Bad alignment");
        using slot_ptr = slot_type*;
        using cslot_ptr = const slot_type*;
        using free_slot_ptr = free_slot_type*;
        using cfree_slot_ptr = const free_slot_type*;

        using slot_span = std::span<slot_type>;
        using cslot_span = std::span<const slot_type>;

        directory_view(std::span<byte> data) : body_(data) {
            [[maybe_unused]] auto* p = body_.data();
            assert((reinterpret_cast<std::uintptr_t>(p) % alignof(directory_header)) == 0);
        }

        template <typename ...Args>
        void init(Args &&...args) {
            if constexpr (is_fixed) {
                fix_init(std::forward<Args>(args)...);
            }
            else {
                var_init();
            }
            header().free_beg = base_begin();
            header().free_end = base_end();
            header().freed = 0;
        }

        cslot_span view() const {
            return { slot_begin(), size() };
        }

        word16_type size() const {
            if constexpr (is_fixed) {
                return (header().free_beg - base_begin()) / sizeof(slot_type);
            }
            else {
                return header().slots;
            }
        }

        std::size_t capacity_for(std::size_t slot_size) const noexcept {
            const auto fixed_size = fixed_len(slot_size);
            const std::size_t max_available = static_cast<std::size_t>(base_end() - base_begin());
            return max_available / (fixed_size + sizeof(slot_type));
        }
        
        std::size_t minumum_slot_size() const noexcept {
            if constexpr (is_fixed) {
                return header().size;
            }
            else {
                return fix_slot_len(sizeof(free_slot_type));
            }
        }

        std::size_t maximum_slot_size() const noexcept {
            if constexpr (is_fixed) {
                return header().size;
            }
            else {
                const std::size_t max_available = static_cast<std::size_t>(base_end() - base_begin());
                return max_available - sizeof(slot_type);
            }
        }

        word16_type available_after_compact() const {
            const std::size_t max_available = static_cast<std::size_t>(base_end() - base_begin());
            const std::size_t total_size =
                static_cast<std::size_t>(stored_size()) +
                static_cast<std::size_t>(sizeof(slot_type)) * static_cast<std::size_t>(size());
            const std::size_t free = (max_available > total_size) ? (max_available - total_size) : 0u;
            return static_cast<word16_type>(free);
        }

        word16_type available() const {
            return header().free_end - header().free_beg;
        }

        word16_type maximum_available_slots() const {
            return available() / (minumum_slot_size() + sizeof(slot_type));
        }

        word16_type stored_size() const {
            if constexpr (is_fixed) {
                return size() * header().size;
            }
            else {
                auto all = view();
                word16_type result = 0;
                for (auto& s : all) {
                    result += fix_slot_len(s.len);
                }
                return result;
            }
        }

        bool can_insert(std::size_t len) const {
            if constexpr (is_fixed) {
                if (len > header().size) {
                    return false;
                }
            }
            len = fixed_len(len);
            const auto slot_overhead = sizeof(slot_type);
            if ((available() >= slot_overhead) && (find_free_slot(len) != nullptr)) {
                return true;
            }
            if (available_for(len, true)) {
                return true;
            }
            const auto after_compact = available_after_compact();
            return after_compact >= (len + slot_overhead);
        }

        bool insert(std::size_t pos, byte_view data) {
            if (available() >= sizeof(slot_type)) {
                byte_span mem;
                if (auto fs = pop_free_slot(data.size())) {
                    mem = free_slot_to_span(fs);
                }
                else if (available_for(data.size())) {
                    mem = allocate_space(data.size());
                }
                else if (available_after_compact() >= (sizeof(slot_type) + fixed_len(data.size()))) {
                    if (compact()) {
                        mem = allocate_space(data.size());
                    }
                }
                if (!mem.empty()) {
                    auto slots = allocate_slot();
                    expand_at(pos);
                    std::memcpy(mem.data(), data.data(), data.size());
                    slots[pos].len = static_cast<word16_type>(data.size());
                    slots[pos].off = offset_of(mem.data());
                    check_valid();
                    return true;
                }
            }
            return false;
        }

        bool reserve(std::size_t pos, std::size_t len) {
            return (reserve_get(pos, len).size() == len);
        }

        byte_span reserve_get(std::size_t pos, std::size_t len) {
            byte_span mem;
            if (available() >= sizeof(slot_type)) {
                if (auto fs = pop_free_slot(len)) {
                    mem = free_slot_to_span(fs);
                }
                else if (available_for(len)) {
                    mem = allocate_space(len);
                }
            }
            if (mem.empty()) {
                if (available_after_compact() >= (sizeof(slot_type) + fixed_len(len))) {
                    if (compact()) {
                        mem = allocate_space(len);
                    }
                }
            }
            if (!mem.empty()) {
                auto slots = allocate_slot();
                expand_at(pos);
                slots[pos].len = static_cast<word16_type>(len);
                slots[pos].off = offset_of(mem.data());
                check_valid();
                return { mem.data(), len };
            }
            return {};
        }

        bool can_update(std::size_t pos, std::size_t new_len) const {
            auto all_slots = view();
            if (pos >= all_slots.size()) {
                return false;
            }
            if constexpr (is_fixed) {
                if (new_len > header().size) {
                    return false;
                }
            } 
            else {
                auto& s = all_slots[pos];
                const auto cur_cap = fix_slot_len(s.len);
                const auto new_cap = fix_slot_len(new_len);
                if (new_len <= cur_cap) {
                    return true;
                }
                if (auto fs = find_free_slot(new_len)) {
                    return true;
                }
                if (available() >= new_cap) {
                    return true;
                }
                const auto size_after_compact = static_cast<std::size_t>(available_after_compact()) + cur_cap;
                if (size_after_compact >= new_cap) {
                    return true;
                }
            }
            return false;
        }

        bool update_reserve(std::size_t pos, std::size_t len) {
            return (update_get(pos, len).size() == len);
        }

        byte_span update_get(std::size_t pos, std::size_t len) {
            auto all_slots = span();
            if (pos >= all_slots.size()) {
                return {};
            }

            if constexpr (is_fixed) {
                if (len > header().size) {
                    return {};
                }
                auto mem = get_slot(all_slots[pos]);
                all_slots[pos].len = static_cast<word16_type>(len);
                check_valid();
                return { mem.data(), len };
            }
            else {
                auto& s = all_slots[pos];
                const auto cur_cap = fix_slot_len(s.len);
                const auto new_cap = fix_slot_len(len);

                // 1) fits into current allocated block (including padding)
                if (len <= cur_cap) {
                    auto mem = get_slot(s);
                    s.len = static_cast<word16_type>(len);
                    if constexpr (!is_fixed) {
                        if ((cur_cap - new_cap) > minumum_slot_size()) {
                            auto fs = reinterpret_cast<free_slot_type*>(mem.data() + new_cap);
                            push_free_slot(fs, cur_cap - new_cap);
                        }
                    }
                    check_valid();
                    return { mem.data(), len };
                }

                // 2) try allocate a new place without compact
                {
                    auto new_mem = allocate_space(len);
                    if (!new_mem.empty()) {
                        // free the old block
                        auto old_mem = get_slot(s);
                        auto fs = reinterpret_cast<free_slot_type*>(old_mem.data());
                        push_free_slot(fs, old_mem.size());

                        s.len = static_cast<word16_type>(len);
                        s.off = offset_of(new_mem.data());
                        check_valid();
                        return { new_mem.data(), len };
                    }
                }

                // 3) fallback: compact and try again
                // mark as "invalid" to reclaim its old space by compact()
                auto size_after_compact = static_cast<std::size_t>(available_after_compact()) + cur_cap;
                if (size_after_compact >= new_cap) {
                    s.off = SLOT_INVALID;
                    if (compact()) {
                        auto new_mem = allocate_space(len);
                        if (!new_mem.empty()) {
                            s.len = static_cast<word16_type>(len);
                            s.off = offset_of(new_mem.data());
                            check_valid();
                            return { new_mem.data(), len };
                        }
                    }
                }
                return {};
            }
        }

        bool update(std::size_t pos, byte_view data) {
            auto new_place = update_get(pos, data.size());
            if (new_place.size() >= data.size()) {
                std::memcpy(new_place.data(), data.data(), data.size());
                check_valid();
                return true;
            }
            return false;
        }

        bool erase(std::size_t pos) {
            auto mem = get_slot(pos);
            if (!mem.empty()) {
                auto fs = reinterpret_cast<free_slot_type*>(mem.data());
                shrink_at(pos);
                if constexpr (!is_fixed) {
                    header().slots = static_cast<word16_type>(header().slots) - 1;
                }
                push_free_slot(fs, mem.size());
                check_valid();
                return true;
            }
            return false;
        }

        bool compact() {
            std::vector<slot_ptr> ext(size());
            return compact(ext);
        }

        bool compact(std::span<slot_ptr> external) {
            auto all = span();
            std::size_t count = 0;
            if (all.size() <= external.size()) {
                for (auto &a: all) {
                    if (a.off != SLOT_INVALID) {
                        external[count++] = &a;
                    }
                }
                
                external = external.first(count);
                std::ranges::sort(external, 
                    [](const auto& lh, const auto& rh) { return rh->off < lh->off; });

                auto end = base_end();
                for (auto& ps : external) {
                    auto flen = fix_slot_len(ps->len);
                    end -= flen;
                    std::memmove(body_.data() + end, body_.data() + ps->off, ps->len);
                    ps->off = end;
                }
                header().free_end = end;
                header().freed = 0;
                check_valid();
                return true;
            }
            return false;
        }

        byte_span get_slot(std::size_t pos) {
            auto slots = view();
            if (pos < slots.size()) {
                return get_slot(slots[pos]);
            }
            return {};
        }

        byte_view get_slot(std::size_t pos) const {
            auto slots = view();
            if (pos < slots.size()) {
                return get_slot(slots[pos]);
            }
            return {};
        }

        byte_span get_slot(slot_type s) {
            if (validate_slot(s)) {
                return { body_.data() + s.off, s.len };
            }
            return {};
        }

        byte_view get_slot(slot_type s) const {
            if (validate_slot(s)) {
                return { body_.data() + s.off, s.len };
            }
            return {};
        }

        bool validate() const {
            const auto& h = header();
            if (!((sizeof(directory_header) <= h.free_beg) &&
                (h.free_beg <= h.free_end) &&
                (h.free_end <= body_.size()))) {
                return false;
            }

            const auto dir = view();
            for (std::uint16_t i = 0; i < size(); ++i) {
                const std::uint16_t off = dir[i].off;
                const std::uint16_t len = dir[i].len;

                if ((off == SLOT_FREE) || (len == 0)) {
                    continue;
                }
                if (off < h.free_end) {
                    return false;
                }
                if (static_cast<std::size_t>(off) + static_cast<std::size_t>(len) > body_.size()) {
                    return false;
                }
            }

            const std::uint16_t expected_fb = static_cast<std::uint16_t>(sizeof(directory_header) + slot_dir_size());

            if (h.free_beg != expected_fb) {
                return false;
            }

            return true;
        }

        word16_type fixed_len(std::size_t len) const {
            return fix_slot_len(len);
        }

        const directory_header& header() const {
            return *reinterpret_cast<const directory_header*>(body_.data());
        }

    private:

        void check_valid([[maybe_unused]] bool val = true) {
            //const auto fe = header().free_end;
            //const auto fb = header().free_beg;
            //if (val && !validate()) {
            //    std::cout << "";
            //}
            //if (!val && fe < fb) {
            //    std::cout << "";
            //}
        }

        bool validate_slot(slot_type s) const {
            const auto& h = header();
            const std::size_t page_sz = body_.size();

            if (s.off == SLOT_INVALID || s.len == 0) {
                return {};
            }
            const std::size_t off = static_cast<std::size_t>(s.off);
            const std::size_t len = static_cast<std::size_t>(s.len);

            if (off < h.free_end) {
                return false;
            }
            if (off + len > page_sz) {
                return false;
            }
            const std::size_t low_limit = sizeof(directory_header) + slot_dir_size();

            if (off < low_limit) {
                return false;
            }
            return true;
        }

        std::size_t slot_dir_size() const {
            return size() * sizeof(slot_type);
        }

        directory_header& header() {
            return *reinterpret_cast<directory_header*>(body_.data());
        }

        void push_free_slot(free_slot_ptr fs, std::size_t len) {
            fs->next = header().freed;
            if constexpr (!is_fixed) {
                fs->prev = 0;
                if (auto oldfs = get_free_slot_by_offset(header().freed)) {
                    oldfs->prev = offset_of(fs);
                }
                fs->len = fix_slot_len(len);
            }
            header().freed = offset_of(fs);
            check_valid();
        }

        bool available_for(std::size_t len, bool need_slot = true) const {
            const auto slot_overhead = need_slot ? sizeof(slot_type) : 0;
            if constexpr (!is_fixed) {
                const auto ava = available();
                const auto fsl = fix_slot_len(len);
                return ava >= (fsl + slot_overhead);
            }
            else {
                auto& h = header();
                return (len <= h.size) && available() >= (h.size + slot_overhead);
            }
        }

        void fix_init(word16_type slot_size) {
            slot_size = static_cast<word16_type>(core::align_up(slot_size, align_val));
            assert(slot_size >= sizeof(free_slot_type));
            header().size = slot_size;
        }

        void var_init() {
            header().slots = 0;
        }

        word16_type base_begin() const {
            return static_cast<word16_type>(core::align_up(sizeof(directory_header), align_val));
        }

        word16_type base_end() const {
            return static_cast<word16_type>(body_.size());
        }

        word16_type fix_slot_len(std::size_t len) const {
            if constexpr (is_fixed) {
                return header().size;
            }
            else {
                if (len < sizeof(free_slot_type)) {
                    len = sizeof(free_slot_type);
                }
                len = core::align_up(len, align_val);
                return static_cast<word16_type>(len);
            }
        }

        template <typename PtrT>
        word16_type offset_of(const PtrT* ptr) const {
            if (ptr) {
                const auto b = reinterpret_cast<std::uintptr_t>(body_.data());
                const auto e = reinterpret_cast<std::uintptr_t>(ptr);
                if ((e > b) && (e < (b + base_end()))) {
                    return static_cast<word16_type>(e - b);
                }
            }
            return 0;
        }

        slot_span span() {
            return { slot_begin(), size() };
        }

        void shrink_at(std::size_t pos) {
            auto all_slots = span();
            for (std::size_t i = pos + 1; i < all_slots.size(); ++i) {
                all_slots[i - 1] = std::move(all_slots[i]);
            }
            header().free_beg = header().free_beg - sizeof(slot_type);
            check_valid(false);
        }

        void expand_at(std::size_t pos) {
            auto all_slots = span();
            for (std::size_t i = all_slots.size() - 1; i > pos; --i) {
                all_slots[i] = std::move(all_slots[i - 1]);
            }
            check_valid(false);
        }

        slot_span allocate_slot() {
            if constexpr (!is_fixed) {
                header().slots = static_cast<word16_type>(header().slots) + 1;
            }
            header().free_beg = static_cast<word16_type>(header().free_beg) + sizeof(slot_type);
            check_valid(false);
            return span();
        }

        free_slot_ptr get_free_slot_by_offset(word16_type off) const {
            if ((off > 0) && (off < static_cast<word16_type>(body_.size()))) {
                return reinterpret_cast<free_slot_ptr>(body_.data() + off);
            }
            return nullptr;
        }

        void remove_free_slot(free_slot_ptr fs) {
            if constexpr (!is_fixed) {
                auto off = offset_of(fs);
                if (off == header().freed) {
                    header().freed = fs->next;
                    if (auto new_h = get_free_slot_by_offset(header().freed)) {
                        new_h->prev = 0;
                    }
                }
                else {
                    auto ps = get_free_slot_by_offset(fs->prev);
                    auto ns = get_free_slot_by_offset(fs->next);
                    if (ps) {
                        ps->next = offset_of(ns);
                    }
                    if (ns) {
                        ns->prev = offset_of(ps);
                    }
                }
            }
            check_valid();
        }

        free_slot_ptr find_free_slot(std::size_t len) const {
            const auto need = fix_slot_len(len);
            if constexpr (!is_fixed) {
                auto fs = get_free_slot_by_offset(header().freed);
                while (fs) {
                    if (fs->len >= need) {
                        return fs;
                    }
                    fs = get_free_slot_by_offset(fs->next);
                }
            }
            else {
                if (auto h = get_free_slot_by_offset(header().freed)) {
                    return h;
                }
            }
            return nullptr;
        }

        free_slot_ptr pop_free_slot(std::size_t len) {
            if (auto fs = find_free_slot(len)) {
                if constexpr (is_fixed) {
                    header().freed = fs->next;
                }
                else {
                    remove_free_slot(fs);
                }
                check_valid();
                return fs;
            }
            return nullptr;
        }

        byte_span free_slot_to_span(free_slot_ptr fs) const {
            if constexpr (is_fixed) {
                return { reinterpret_cast<byte*>(fs), header().size };
            }
            else {
                return { reinterpret_cast<byte*>(fs), fs->len };
            }
        }

        byte_span allocate_space(std::size_t len) {
            if constexpr (is_fixed) {
                if (header().size < len) {
                    check_valid();
                    return {};
                }
            }
            auto fix_len = fix_slot_len(len);
            if (auto fs = pop_free_slot(fix_len)) {
                check_valid();
                return free_slot_to_span(fs);
            }
            else {
                if (available() >= fix_len) {
                    header().free_end = static_cast<word16_type>(header().free_end) - static_cast<word16_type>(fix_len);
                    check_valid();
                    return { body_.data() + header().free_end, fix_len };
                }
            }
            check_valid();
            return {};
        }

        slot_ptr slot_begin() {
            return reinterpret_cast<slot_ptr>(body_.data() + base_begin());
        }

        slot_ptr slot_end() {
            return slot_begin() + size();
        }

        cslot_ptr slot_begin() const {
            return reinterpret_cast<cslot_ptr>(body_.data() + base_begin());
        }

        cslot_ptr slot_end() const {
            return slot_begin() + size();
        }

        std::span<byte> body_;
    };

	struct empty_directory_view {
        struct slot_type {};
        struct directory_header {};

        empty_directory_view() = default;
        constexpr empty_directory_view(core::byte_span) {}

        constexpr static void init() noexcept { }
        constexpr static std::size_t size() noexcept { return 0; }
        constexpr static std::size_t capacity_for(std::size_t) noexcept { return 0; }
        constexpr static std::size_t minumum_slot_size() noexcept { return 0; }
        constexpr static std::size_t maximum_slot_size() noexcept { return 0; }
        constexpr static byte_span reserve_get(std::size_t, std::size_t) noexcept { return {}; }
        constexpr static byte_span update_get(std::size_t, std::size_t) noexcept { return {}; }
		constexpr static bool insert(std::size_t, byte_view) noexcept { return false; }
		constexpr static bool update(std::size_t, byte_view) noexcept { return false; }
		constexpr static bool erase(std::size_t) noexcept { return false; }
		constexpr static bool can_insert(std::size_t) noexcept { return false; }
		constexpr static bool can_update(std::size_t, std::size_t) noexcept { return false; }
        constexpr static std::size_t available() noexcept { return 0; }
		constexpr static std::size_t available_after_compact() noexcept { return 0; }
		constexpr static bool compact(auto) noexcept { return false; }
		constexpr static std::span<const slot_type> view() noexcept { return {}; }
		constexpr static byte_span get_slot(std::size_t) noexcept { return {}; }
    };

    template <std::size_t AlignV = 4>
    using fixed_directory_view = directory_view<directory_type::fixed, AlignV>;

    template <std::size_t AlignV = 4>
    using variadic_directory_view = directory_view<directory_type::variadic, AlignV>;

    static_assert(SlotDirectoryConcept<fixed_directory_view<>>);
    static_assert(SlotDirectoryConcept<variadic_directory_view<>>);
    static_assert(SlotDirectoryConcept<empty_directory_view>);

    template <typename DirDst, typename DirSrc>
    inline std::size_t merge_need_bytes(const DirDst& dst, const DirSrc& src) {
        std::size_t payload = 0;
        for (const auto& s : src.view()) {
            payload += static_cast<std::size_t>(dst.fixed_len(s.len));
        }
        const std::size_t slots = static_cast<std::size_t>(src.size())
            * static_cast<std::size_t>(sizeof(typename DirDst::slot_type));
        return payload + slots;
    }

    template <typename DirDst, typename DirSrc>
    inline bool can_merge(const DirDst& dst, const DirSrc& src) {

        //static_assert(std::is_same_v<typename DirDst::slot_type, typename DirSrc::slot_type>,
        //    "Slot entry layout must match");

        if constexpr (DirDst::is_fixed) {
            auto sview = src.view();
            for (const auto& s : sview) {
                if (s.len > dst.header().size) {
                    return false;
                }
            }
        }

        std::size_t payload_need = merge_need_bytes(dst, src);
        const std::size_t available = dst.available_after_compact();

        return available >= payload_need;
    }

}