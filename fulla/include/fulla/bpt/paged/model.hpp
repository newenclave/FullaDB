/*
 * File: model.hpp
 * Author: newenclave
 * GitHub: https://github.com/newenclave
 * Created: 2025-11-02
 * License: MIT
 */


#pragma once

#include <optional>
#include <ranges>
#include <algorithm>
#include <functional>

#include "fulla/core/debug.hpp"
#include "fulla/bpt/concepts.hpp"
#include "fulla/bpt/paged/settings.hpp"

#include "fulla/page/header.hpp"
#include "fulla/page/page_view.hpp"
#include "fulla/page/bpt_inode.hpp"
#include "fulla/page/bpt_leaf.hpp"
#include "fulla/page/bpt_root.hpp"

#include "fulla/page/freed.hpp"

#include "fulla/page/slot_directory.hpp"
#include "fulla/page/ranges.hpp"

#include "fulla/storage/device.hpp"
#include "fulla/storage/buffer_manager.hpp"


namespace fulla::bpt::paged {

    using core::byte_view;
    using core::byte_buffer;

    namespace model_common {
        struct key_like_type {
            byte_view key;
        };

        struct key_out_type {
            byte_view key;
        };

        struct key_borrow_type {
            byte_buffer key;
        };

        struct value_in_type {
            byte_view val;
        };

        struct value_out_type {
            byte_view val;
        };

        struct value_borrow_type {
            byte_buffer val;
        };

        using slot_directory_type = page::slots::variadic_directory_view<>;
        using page_view_type = page::page_view<slot_directory_type>;
        using cpage_view_type = page::const_page_view<slot_directory_type>;
    }

    template <typename KeyLessT>
    concept ModelKeyLessConcept = requires (const KeyLessT klt, byte_view a, byte_view b) {
        { klt.compare(a, b) } -> std::convertible_to<std::partial_ordering>;
        { klt.operator ()(a, b) } -> std::convertible_to<bool>;
    };

    template <storage::RandomAccessBlockDevice DeviceT, 
        typename PidT = std::uint32_t, 
        ModelKeyLessConcept KeyLessT = page::record_less>
    struct model {

        using buffer_manager_type = storage::buffer_manager<DeviceT, PidT>;
        using slot_directory_type = model_common::slot_directory_type;
        using page_view_type = model_common::page_view_type;
        using cpage_view_type = model_common::cpage_view_type;

        using less_type = KeyLessT;

        using node_id_type = PidT;
        constexpr static const node_id_type invalid_node_value = std::numeric_limits<node_id_type>::max();

        constexpr static const std::size_t maximum_inode_slot_size = 200;
        constexpr static const std::size_t minumum_inode_slot_size = 5;

        constexpr static const std::size_t maximum_leaf_slot_size = 200;
        constexpr static const std::size_t minumum_leaf_slot_size = 5;

        using key_like_type = model_common::key_like_type;
        using key_out_type = model_common::key_out_type;
        using key_borrow_type = model_common::key_borrow_type;
        using value_in_type = model_common::value_in_type;
        using value_out_type = model_common::value_out_type;
        using value_borrow_type = model_common::value_borrow_type;

        struct inode_key_extractor {
            byte_view operator ()(byte_view value) const noexcept {
                const auto* slot_hdr = reinterpret_cast<const page::bpt_inode_slot*>(value.data());
                return { value.begin() + slot_hdr->key_offset(), value.end() };
            }
        };

        struct leaf_key_extractor {
            byte_view operator ()(byte_view value) const noexcept {
                const auto* slot_hdr = reinterpret_cast<const page::bpt_leaf_slot*>(value.data());
                return { value.begin() + slot_hdr->key_offset(), slot_hdr->key_len };
            }
        };

        struct leaf_value_extractor {
            byte_view operator ()(byte_view value) const noexcept {
                const auto* slot_hdr = reinterpret_cast<const page::bpt_leaf_slot*>(value.data());
                return { value.begin() + slot_hdr->value_offset(), value.end() };
            }
        };

        inline static auto make_inode_key_projector(page_view_type& pv) {
            return page::make_slot_projection_with_extracor<inode_key_extractor>(pv);
        }

        inline static auto make_value_key_projector(page_view_type& pv) {
            return page::make_slot_projection_with_extracor<leaf_key_extractor>(pv);
        }

        inline static auto make_key_less() {
            return less_type{};
        }

        struct node_base {

            using node_id_type = node_id_type;

            node_base(page_view_type page, node_id_type self_id, std::size_t min_l, std::size_t max_l, typename buffer_manager_type::page_handle hdl)
                : page_(page)
                , id_(self_id) 
                , minimum_len(min_l)
                , maximum_len(max_l)
                , hdl_(std::move(hdl))
            {}

            node_base() = default;

            virtual ~node_base() = default;

            std::size_t capacity() const {
                return get_slots().capacity_for(maximum_len);
            }

            bool is_full() const noexcept {
                return !get_slots().can_insert(maximum_len);
            }

            bool is_underflow() const noexcept {
                const auto slots = this->get_slots();
                return slots.stored_size() < slots.available();
            }

            bool keys_eq(key_like_type a, key_like_type b) const noexcept {
                const auto key_cmp = make_key_less();
                return std::is_eq(key_cmp.compare(a.key, b.key));
            }

            node_id_type self() const noexcept {
                return id_;
            }

            bool can_update_key(std::size_t pos, key_like_type k) const {
                const auto slots = this->get_slots();
                const auto old_slot_value = slots.get_slot(pos);
                const auto old_key_value = extract_key(old_slot_value);
                const auto old_size_without_key = old_slot_value.size() - old_key_value.size();
                return slots.can_update(pos, old_size_without_key + k.key.size());
            }

            key_out_type get_key(std::size_t pos) const {
                auto pv = get_page();
                auto slots = pv.get_slots_dir();
                if (pos < slots.size()) {
                    auto res = key_out_type{ extract_key(slots.get_slot(pos)) };
                    return res;
                }
                return {};
            }

            key_borrow_type borrow_key(std::size_t pos) const {
                auto pv = get_page();
                auto slots = get_slots();

                if (pos < slots.size()) {
                    auto key_result = extract_key(slots.get_slot(pos));
                    key_borrow_type result{ .key = byte_buffer {key_result.begin(), key_result.end()} };
                    return result;
                }
                return {};
            }

            bool check_mark_dirty(bool ok) {
                if (ok) {
                    hdl_.mark_dirty();
                }
                return ok;
            }

            bool erase(std::size_t pos) {
                auto slots = this->get_slots();
                return check_mark_dirty(slots.erase(pos));
            }

            bool is_valid() const {
                return id_ != invalid_node_value;
            }

            std::size_t size() const {
                return get_slots().size();
            }

            slot_directory_type get_slots() const noexcept {
                return get_page().get_slots_dir();
            }

            page_view_type get_page() const noexcept {
                return page_;
            }

            bool check_length(std::size_t len) const noexcept {
                return (len >= minimum_len) && (len <= maximum_len);
            }

            virtual byte_view extract_key(byte_view val) const = 0;

            page_view_type page_;
            node_id_type id_ = invalid_node_value;
            std::size_t minimum_len = 0;
            std::size_t maximum_len = 0;
            typename buffer_manager_type::page_handle hdl_;
        };

        struct leaf_type : public node_base {

            leaf_type(page_view_type page, node_id_type self_id, 
                typename buffer_manager_type::page_handle hdl, 
                std::size_t min_slot_sz = minumum_leaf_slot_size,
                std::size_t max_slot_sz = maximum_leaf_slot_size)
                : node_base(page, self_id, min_slot_sz, max_slot_sz, std::move(hdl))
            {}

            leaf_type() = default;

            virtual byte_view extract_key(byte_view val) const {
                const auto key_proj = leaf_key_extractor{};
                return key_proj(val);
            }

            std::size_t key_position(key_like_type k) const {
                auto pv = this->get_page();
                auto slots = pv.get_slots_dir();
                auto slots_view = slots.view();

                const auto key_proj = make_value_key_projector(pv);
                const auto key_cmp = make_key_less();

                auto it = std::ranges::lower_bound(slots_view, k.key, key_cmp, key_proj);
                return std::distance(slots_view.begin(), it);
            }

            bool update_key(std::size_t pos, key_like_type k) {
                auto slots = this->get_slots();
                leaf_value_extractor lve;
                auto old_slot = slots.get_slot(pos);
                auto old_value = lve(old_slot);
                auto new_full_len = sizeof(page::bpt_leaf_slot) + k.key.size() + old_value.size();

                if (slots.can_update(pos, new_full_len)) {
                    if (!this->check_length(new_full_len)) {
                        DB_ASSERT(false, "something went wrong");
                        return false;
                    }
                    byte_buffer new_value(new_full_len);
                    auto* slot_hdr = reinterpret_cast<page::bpt_leaf_slot*>(new_value.data());
                    slot_hdr->update(k.key.size());

                    std::memcpy(new_value.data() + slot_hdr->key_offset(), k.key.data(), k.key.size());
                    std::memcpy(new_value.data() + slot_hdr->value_offset(), old_value.data(), old_value.size());

                    if (!slots.update(pos, { new_value })) {
                        DB_ASSERT(false, "something went wrong");
                        return false;
                    }
                    return this->check_mark_dirty(true);
                }
                DB_ASSERT(false, "something went wrong");
                return false; // ?
            }

            bool insert_value(std::size_t pos, key_like_type k, value_in_type v) {
                auto slots = this->get_slots();
                auto new_full_len = sizeof(page::bpt_leaf_slot) + k.key.size() + v.val.size();
                if (!this->check_length(new_full_len)) {
                    DB_ASSERT(false, "maximum_leaf_slot_size reached");
                    return false;
                }

                if (slots.reserve(pos, new_full_len)) {
                    auto data = slots.get_slot(pos);
                    auto hdr = reinterpret_cast<page::bpt_leaf_slot*>(data.data());
                    hdr->update(k.key.size());
                    std::memcpy(data.data() + hdr->key_offset(), k.key.data(), k.key.size());
                    std::memcpy(data.data() + hdr->value_offset(), v.val.data(), v.val.size());
                    return this->check_mark_dirty(true);
                }
                DB_ASSERT(false, "something went wrong");
                return false;
            }

            bool update_value(std::size_t pos, value_in_type v) {
                auto slots = this->get_slots();
                const auto old_data = slots.get_slot(pos);
                const auto old_value = leaf_value_extractor{}(old_data);
                const auto old_key = leaf_key_extractor{}(old_data);
                const auto new_size = sizeof(page::bpt_leaf_slot) + old_key.size() + v.val.size();
                if (!this->check_length(new_size)) {
                    DB_ASSERT(false, "something went wrong");
                    return false;
                }
                if (slots.can_update(pos, new_size)) {
                    byte_buffer new_data(new_size);
                    auto new_hdr = reinterpret_cast<page::bpt_leaf_slot*>(new_data.data());
                    new_hdr->update(old_key.size());
                    std::memcpy(new_data.data() + new_hdr->key_offset(), old_key.data(), old_key.size());
                    std::memcpy(new_data.data() + new_hdr->value_offset(), v.val.data(), v.val.size());
                    if (!slots.update(pos, { new_data })) {
                        DB_ASSERT(false, "something went wrong");
                        return false;
                    }
                    return this->check_mark_dirty(true);
                }
                DB_ASSERT(false, "something went wrong");
                return false;
            }

            bool can_insert_value(std::size_t, key_like_type k, value_in_type v) {
                const auto slots = this->get_slots();
                const auto new_full_len = sizeof(page::bpt_leaf_slot) + k.key.size() + v.val.size();
                [[maybe_unused]] const bool size_ok = this->check_length(new_full_len);
                DB_ASSERT(size_ok, "Something went wrong");
                return slots.can_insert(new_full_len);
            }

            bool can_update_value(std::size_t pos, value_in_type v) {
                const auto slots = this->get_slots();
                const auto old_value = slots.get_slot(pos);
                auto k = leaf_key_extractor{}(old_value);
                const auto new_full_len = sizeof(page::bpt_leaf_slot) + k.size() + v.val.size();
                [[maybe_unused]] const bool size_ok = this->check_length(new_full_len);
                DB_ASSERT(size_ok, "Something went wrong");
                return slots.can_update(pos, new_full_len);
            }

            value_out_type get_value(std::size_t pos) const {
                auto slots = this->get_slots();
                if (pos < slots.size()) {
                    leaf_value_extractor lve;
                    return { lve(slots.get_slot(pos)) };
                }
                return {};
            }

            value_borrow_type borrow_value(std::size_t pos) const {
                auto slots = this->get_slots();
                if (pos < slots.size()) {
                    leaf_value_extractor lve;
                    value_borrow_type res;
                    const auto value = lve(slots.get_slot(pos));
                    res.val.insert(res.val.end(), value.begin(), value.end());
                    return res;
                }
                return {};
            }

            void set_next(node_id_type nv) {
                auto pv = this->get_page();
                auto hdr = pv.subheader<page::bpt_leaf_header>();
                hdr->next = nv;
                this->check_mark_dirty(true);
            }
            
            node_id_type get_next() const {
                auto pv = this->get_page();
                auto hdr = pv.subheader<const page::bpt_leaf_header>();
                return hdr->next;
            }

            void set_prev(node_id_type nv) {
                auto pv = this->get_page();
                auto hdr = pv.subheader<page::bpt_leaf_header>();
                hdr->prev = nv;
                this->check_mark_dirty(true);
            }

            node_id_type get_prev() const {
                auto pv = this->get_page();
                auto hdr = pv.subheader<const page::bpt_leaf_header>();
                return hdr->prev;
            }

            void set_parent(node_id_type new_value) {
                page_view_type pv = this->get_page();
                auto* inode_hdr = pv.subheader<page::bpt_leaf_header>();
                inode_hdr->parent = new_value;
                this->check_mark_dirty(true);
            }

            node_id_type get_parent() const {
                page_view_type pv = this->get_page();
                auto* inode_hdr = pv.subheader<page::bpt_leaf_header>();
                return inode_hdr->parent;
            }

        };

        struct inode_type: public node_base {
            using node_id_type = node_id_type;

            inode_type(page_view_type page, node_id_type self_id, 
                typename buffer_manager_type::page_handle hdl, 
                std::size_t min_slot_sz = minumum_inode_slot_size, 
                std::size_t max_slot_sz = maximum_inode_slot_size)
                : node_base(page, self_id, min_slot_sz, max_slot_sz, std::move(hdl))
            {}

            inode_type() = default;

            virtual byte_view extract_key(byte_view val) const {
                const auto key_proj = inode_key_extractor{};
                return key_proj(val);
            }

            std::size_t key_position(key_like_type k) const {
                auto pv = this->get_page();
                auto slots = pv.get_slots_dir();
                auto slots_view = slots.view();

                const auto key_proj = make_inode_key_projector(pv);
                const auto key_cmp = make_key_less();

                auto it = std::ranges::upper_bound(slots_view, k.key, key_cmp, key_proj);
                return std::distance(slots_view.begin(), it);
            }

            bool update_key(std::size_t pos, key_like_type k) {
                auto slots = this->get_slots();
                const auto old_data = slots.get_slot(pos);
                const auto* old_slot_hdr = reinterpret_cast<const page::bpt_inode_slot*>(old_data.data());
                const auto old_child_value = old_slot_hdr->child;
                const auto new_len = sizeof(page::bpt_inode_slot) + k.key.size();
                if (new_len > maximum_inode_slot_size) {
                    DB_ASSERT(false, "something went wrong");
                    return false;
                }
                if (slots.update_reserve(pos, new_len)) {
                    auto new_value = slots.get_slot(pos);
                    auto* slot_hdr = reinterpret_cast<page::bpt_inode_slot*>(new_value.data());
                    slot_hdr->child = old_child_value;
                    std::memcpy(new_value.data() + slot_hdr->key_offset(), k.key.data(), k.key.size());
                    return this->check_mark_dirty(true);
                }
                DB_ASSERT(false, "something went wrong");
                return false; // ?
            }

            void set_parent(node_id_type new_value) {
                page_view_type pv = this->get_page();
                auto* inode_hdr = pv.subheader<page::bpt_inode_header>();
                inode_hdr->parent = new_value;
                this->check_mark_dirty(true);
            }

            node_id_type get_parent() {
                page_view_type pv = this->get_page();
                auto* inode_hdr = pv.subheader<page::bpt_inode_header>();
                return inode_hdr->parent;
            }

            node_id_type get_child(std::size_t pos) const {
                if (auto c_ptr = get_child_ptr(pos)) {
                    return static_cast<node_id_type>(*c_ptr);
                }
                return invalid_node_value;
            }

            bool can_insert_child(std::size_t, key_like_type k, node_id_type) const {
                const auto slots = this->get_slots();
                const auto full_slot_size = (k.key.size() + sizeof(page::bpt_inode_slot));

                return (full_slot_size >= this->minimum_len) 
                    && (full_slot_size <= this->maximum_len)
                    && slots.can_insert(full_slot_size);
            }

            bool can_update_child(std::size_t, node_id_type) const noexcept {
                return true;
            }

            bool insert_child(std::size_t pos, key_like_type k, node_id_type c) {
                auto slots = this->get_slots();
                const auto full_len = k.key.size() + sizeof(page::bpt_inode_slot);
                if (full_len > maximum_inode_slot_size) {
                    return false;
                }
                if (slots.reserve(pos, full_len)) {
                    auto new_slot = slots.get_slot(pos);
                    auto slot_hdr = reinterpret_cast<page::bpt_inode_slot*>(new_slot.data());
                    slot_hdr->child = c;
                    std::memcpy(new_slot.data() + slot_hdr->key_offset(), k.key.data(), k.key.size());
                    return this->check_mark_dirty(true);
                }
                return false;
            }

            bool update_child(std::size_t pos, node_id_type c) {
                if (auto c_ptr = get_child_ptr(pos)) {
                    *c_ptr = c;
                    return this->check_mark_dirty(true);
                }
                return false;
            }

        private:
            auto get_child_ptr(std::size_t pos) const -> decltype(page::bpt_inode_slot::child) * {
                auto slots = this->get_slots();
                const auto slot_size = slots.size();
                if (pos < slot_size) {
                    auto value = slots.get_slot(pos);
                    auto slot_hdr = reinterpret_cast<page::bpt_inode_slot*>(value.data());
                    return &slot_hdr->child;
                }
                else if(pos == slot_size) {
                    auto pv = this->get_page();
                    auto sub_hdr = pv.subheader<page::bpt_inode_header>();
                    return &sub_hdr->rightmost_child;
                }
                return nullptr;
            }
        };

        static_assert(concepts::INode<inode_type, key_out_type, key_like_type, key_borrow_type>);
        static_assert(concepts::LeafNode<leaf_type, key_out_type, key_like_type, key_borrow_type, 
                value_out_type, value_in_type, value_borrow_type>);

        model(buffer_manager_type& mgr, settings sett)
            : accessor_(mgr, std::move(sett))
        {}

        model(buffer_manager_type& mgr)
            : model(mgr, {})
        {}

        struct accessor_type {

            using leaf_type = model::leaf_type;
            using inode_type = model::inode_type;

            accessor_type(buffer_manager_type& mgr, settings sett)
                : mgr_(mgr)
                , sett_(std::move(sett))
            {
                //check_create_root_node();
            }

            accessor_type(buffer_manager_type& mgr)
                : accessor_type(mgr, {})
            {}

            leaf_type create_leaf() {
                typename buffer_manager_type::page_handle new_page = pop_free_page();

                if (!new_page.is_valid()) {
                    new_page = mgr_.create();
                }
                if (new_page.is_valid()) {
                    auto pv = page_view_type{ new_page.rw_span() };
                    const auto page_id = new_page.pid();
                    pv.header().init(sett_.leaf_kind_value, mgr_.page_size(), page_id, sizeof(page::bpt_leaf_header));
                    pv.get_slots_dir().init();
                    auto subhdr = pv.subheader<page::bpt_leaf_header>();
                    subhdr->init();
                    subhdr->parent = invalid_node_value;
                    subhdr->next = invalid_node_value;
                    subhdr->prev = invalid_node_value;
                    new_page.mark_dirty();

                    return { 
                        pv, page_id, std::move(new_page), 
                        sett_.leaf_minimum_slot_size, 
                        sett_.leaf_maximum_slot_size 
                    };
                }
                return {};
            }
            
            inode_type create_inode() {
                typename buffer_manager_type::page_handle new_page = pop_free_page();
                if (!new_page.is_valid()) {
                    new_page = mgr_.create();
                }
                if (new_page.is_valid()) {
                    auto pv = page_view_type{ new_page.rw_span() };
                    const auto page_id = new_page.pid();
                    pv.header().init(sett_.inode_kind_value, mgr_.page_size(), page_id, sizeof(page::bpt_inode_header));
                    pv.get_slots_dir().init();
                    auto subhdr = pv.subheader<page::bpt_inode_header>();
                    subhdr->init();
                    subhdr->parent = invalid_node_value;
                    new_page.mark_dirty();

                    return { pv, page_id, std::move(new_page),
                        sett_.inode_minimum_slot_size, 
                        sett_.inode_maximum_slot_size 
                    };
                }
                return { };
            }

            bool destroy(node_id_type id) {
                if (destroy_hook_) {
                    destroy_hook_(id);
                }
                push_free_page(id);
                return true;
            }

            leaf_type load_leaf(node_id_type id) {
                auto new_page = mgr_.fetch(id);
                if (new_page.is_valid()) {
                    const auto page_id = new_page.pid();
                    auto data = new_page.rw_span();
                    auto pv = page_view_type{ data };
                    const auto kind = pv.header().kind.get();
                    if (kind == static_cast<std::uint16_t>(sett_.leaf_kind_value)) {
                        return leaf_type{ pv, page_id, 
                            std::move(new_page),
                            sett_.leaf_minimum_slot_size,
                            sett_.leaf_maximum_slot_size,
                        };
                    }
                }
                return {};
            }
            
            inode_type load_inode(node_id_type id) {
                auto new_page = mgr_.fetch(id);
                if (new_page.is_valid()) {
                    const auto page_id = new_page.pid();
                    auto data = new_page.rw_span();
                    auto pv = page_view_type{ data };
                    const auto kind = pv.header().kind.get();
                    if (kind == static_cast<std::uint16_t>(sett_.inode_kind_value)) {
                        return inode_type{ pv, page_id, 
                            std::move(new_page),
                            sett_.inode_minimum_slot_size,
                            sett_.inode_maximum_slot_size
                        };
                    }
                }
                return {};
            }

            bool can_merge_leafs(const leaf_type& dst, const leaf_type& src) const {
                return page::slots::can_merge(dst.get_page().get_slots_dir(), src.get_page().get_slots_dir());
            }

            bool can_merge_inodes(const inode_type& dst, const inode_type& src) {
                const auto dst_slots = dst.get_page().get_slots_dir();
                const auto src_slots = src.get_page().get_slots_dir();
                const auto dst_available = dst_slots.available_after_compact();
 
                const auto need_size = page::slots::merge_need_bytes(dst_slots, src_slots) + maximum_inode_slot_size;

                if (dst_available < need_size) {
                    return false;
                }
                return page::slots::can_merge(dst.get_page().get_slots_dir(), src.get_page().get_slots_dir());
            }

            std::tuple<node_id_type, bool> load_root() {
                if(root_) {
                    return { *root_, true };
                }
                return { invalid_node_value, false };
            }

            void set_root(node_id_type id) {
                if (set_root_hook_) {
                    set_root_hook_(id);
                }
                if (id != invalid_node_value) {
                    root_ = { id };
                }
                else {
                    root_ = {};
                }
            }

            auto pop_free_page() {
                if (first_freed_ != invalid_node_value) {
                    auto page = mgr_.fetch(first_freed_);
                    if (page.is_valid()) {
                        const auto pv = cpage_view_type{ page.ro_span() };
                        const auto *fh = pv.subheader<page::freed>();
                        first_freed_ = fh->next;
                        return page;
                    }
                }
                return typename buffer_manager_type::page_handle{};
            }

            void push_free_page(node_id_type id) {
                auto page = mgr_.fetch(id);
                if (page.is_valid()) {
                    auto pv = page_view_type{ page.rw_span() };
                    pv.header().init(core::word_u16::max(), pv.size(), id, sizeof(page::freed));
                    auto *fh = pv.subheader<page::freed>();
                    fh->init();
                    fh->next = first_freed_;
                    first_freed_ = id;
                    page.mark_dirty();
                }
            }

            void check_create_root_node() {
                auto first_node = mgr_.fetch(0);
                if (!first_node.is_valid()) {
                    first_node = mgr_.create();
                    auto pv = page_view_type{ first_node.rw_span() };
                    pv.header().init(sett_.root_kind_value, mgr_.page_size(), 0, sizeof(page::bpt_root));
                    auto rh = pv.subheader<page::bpt_root>();
                    rh->root = invalid_node_value;
                    first_node.mark_dirty();
                }
                else {
                    const auto pv = cpage_view_type{ first_node.ro_span() };
                    const auto rh = pv.subheader<page::bpt_root>();
                    root_ = { rh->root.get() };
                }
            }

            std::optional<node_id_type> root_ {};
            buffer_manager_type &mgr_;
            settings sett_{};
            std::function<void(node_id_type)> destroy_hook_;
            std::function<void(node_id_type)> set_root_hook_;
            node_id_type first_freed_ = invalid_node_value;
        };

        static_assert(concepts::NodeAccessor<accessor_type, node_id_type, inode_type, leaf_type>);

        static key_like_type key_out_as_like(key_out_type kout) {
            const key_like_type res = { kout.key };
            return res;
        }

        static key_like_type key_borrow_as_like(const key_borrow_type &kbor) {
            return { kbor.key };
        }

        static value_in_type value_out_as_in(value_out_type vout) {
            return { vout.val };
        }

        static value_in_type value_borrow_as_in(const value_borrow_type &vbor) {
            return { vbor.val };
        }

        bool is_valid_id(node_id_type id) {
            return (id != invalid_node_value) && (accessor_.mgr_.valid_pid(id));
        }

        bool is_leaf_id(node_id_type id) {
            auto p = accessor_.mgr_.fetch(id);
            if (p.is_valid()) {
                return reinterpret_cast<const page::page_header*>(p.ro_span().data())->kind 
                    == static_cast<std::uint16_t>(page::page_kind::bpt_leaf);
            }
            return false;
        }

        static node_id_type get_invalid_node_id() noexcept {
            return invalid_node_value;
        }

        template <typename IasT, typename KasT, typename VasT>
        void set_stringifier_callbacks(IasT ias, KasT kas, VasT vas) {
            id_as_string_ = std::move(ias);
            key_as_string_ = std::move(kas);
            value_as_string_ = std::move(vas);
        }

        std::string id_as_string(node_id_type id) const {
            if (id_as_string_) {
                return id_as_string_(id);
            }
            return {};
        }

        std::string key_as_string(const key_out_type &kout) const {
            if (key_as_string_) {
                return key_as_string_(kout);
            }
            return {};
        }

        std::string value_as_string(const value_out_type vout) const {
            if (value_as_string_) {
                return value_as_string_(vout);
            }
            return {};
        }

        accessor_type &get_accessor() noexcept {
            return accessor_;
        }
        
        const accessor_type &get_accessor() const noexcept {
            return accessor_;
        }

        settings& get_settings() noexcept {
            return get_accessor().sett_;
        }

        const settings& get_settings() const noexcept {
            return get_accessor().sett_;
        }

    private:
        std::function<std::string(const key_out_type&)> key_as_string_;
        std::function<std::string(const value_out_type&)> value_as_string_;
        std::function<std::string(node_id_type)> id_as_string_;
        accessor_type accessor_;
    };

}