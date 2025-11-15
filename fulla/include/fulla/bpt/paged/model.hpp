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

#include "fulla/bpt/concepts.hpp"

#include "fulla/page/header.hpp"
#include "fulla/page/page_view.hpp"
#include "fulla/page/bpt_inode.hpp"
#include "fulla/page/slot_directory.hpp"
#include "fulla/page/ranges.hpp"

#include "fulla/storage/device.hpp"
#include "fulla/storage/buffer_manager.hpp"

namespace fulla::bpt::paged {

    using core::byte_view;
    using core::byte_buffer;

    template <storage::RandomAccessDevice DeviceT, typename PidT = std::uint32_t>
    struct model {

        using buffer_manager_type = storage::buffer_manager<DeviceT, PidT>;
        using slot_directory_type = page::slots::variadic_directory_view<>;
        using page_view_type = page::page_view<slot_directory_type>;

        using node_id_type = PidT;
        constexpr static const node_id_type invalid_node_type = std::numeric_limits<node_id_type>::max();

        constexpr static const std::size_t maximum_slot_size = 200;
        constexpr static const std::size_t minumum_slot_size = 40;

        struct key_like_type {
            byte_view key;
        };

        struct key_out_type {
            byte_view key;
        };

        struct key_borrow_type {
            byte_buffer key;
        };

        struct inode_key_extractor {
            byte_view operator ()(byte_view value) const noexcept {
                const auto* slot_hdr = reinterpret_cast<const page::bpt_inode_slot*>(value.data());
                return { value.begin() + slot_hdr->key_offset(), value.end() };
            }
        };
        
        inline static auto make_inode_key_projector(page_view_type &pv) {
            return page::make_slot_projection_with_extracor<inode_key_extractor>(pv);
        }
        
        inline static auto make_key_less() {
            return page::make_record_less();
        }

        struct node_base {
            virtual ~node_base() = default;

        };

        struct leaf_type {};

        struct inode_type {
            using node_id_type = node_id_type;
            inode_type(page_view_type page, node_id_type self_id) : page_(page), id_(self_id) {}

            node_id_type self() const noexcept {
                return id_;
            }

            std::size_t capacity() const {
                return get_slots().capacity_for(maximum_slot_size);
            }

            std::size_t size() const {
                return get_slots().size();
            }

            bool is_full() const noexcept {
                return !get_slots().can_insert(maximum_slot_size);
            }

            bool is_underflow() const noexcept {
                const auto slots = get_slots();
                return slots.stored_size() < slots.available();
            }

            std::size_t key_position(key_like_type k) const {
                auto pv = get_page();
                auto slots = pv.get_slots_dir();
                auto slots_view = slots.view();

                const auto key_proj = make_inode_key_projector(pv);
                const auto key_cmp = make_key_less();

                auto it = std::ranges::upper_bound(slots_view, k.key, key_cmp, key_proj);
                return std::distance(slots_view.begin(), it);
            }

            bool keys_eq(key_like_type a, key_like_type b) const noexcept {
                const auto key_cmp = make_key_less();
                return std::is_eq(key_cmp.compare(a.key, b.key));
            }

            key_out_type get_key(std::size_t pos) const {
                auto pv = get_page();
                auto slots = pv.get_slots_dir();
                const auto key_proj = inode_key_extractor{};

                if (pos < slots.size()) {
                    return { key_proj(slots.get_slot(pos)) };
                }
                return {};
            }

            key_borrow_type borrow_key(std::size_t pos) const {
                auto pv = get_page();
                auto slots = get_slots();
                const auto key_proj = make_inode_key_projector(pv);

                if (pos < slots.size()) {
                    auto key_result = key_proj(slots.get_slot(pos));
                    key_borrow_type result{ .key = byte_buffer {key_result.begin(), key_result.end()}};
                    return result;
                }
                return {};
            }

            bool erase(std::size_t pos) const {
                auto slots = get_slots();
                return slots.erase(pos);
            }

            bool update_key(std::size_t pos, key_like_type k) const {
                auto slots = get_slots();
                if (slots.can_update(pos, k.key.size() + sizeof(page::bpt_inode_slot))) {
                    byte_buffer new_value(sizeof(page::bpt_inode_slot) + k.key.size());
                    if (new_value.size() > maximum_slot_size) {
                        return false;
                    }
                    auto old_slot = slots.get_slot(pos);
                    auto* slot_hdr = reinterpret_cast<page::bpt_inode_slot*>(new_value.data());
                    auto* old_slot_hdr = reinterpret_cast<const page::bpt_inode_slot*>(old_slot.data());
                    slot_hdr->child = old_slot_hdr->child;
                    std::memcpy(new_value.data() + slot_hdr->key_offset(), old_slot.data() + old_slot_hdr->key_offset(), k.key.size());
                    slots.update(pos, { new_value });
                    return true;
                }
                return false; // ?
            }

            bool is_valid() const {
                return id_ != invalid_node_type;
            }

            void set_parent(node_id_type new_value) {
                page_view_type pv = get_page();
                auto* inode_hdr = pv.subheader<page::bpt_inode_header>();
                inode_hdr->parent = new_value;
            }

            node_id_type get_parent() {
                page_view_type pv = get_page();
                auto* inode_hdr = pv.subheader<page::bpt_inode_header>();
                return inode_hdr->parent;
            }

            node_id_type get_child(std::size_t pos) const {
                auto slots = get_slots();
                if (pos < slots.size()) {
                    auto slot_data = slots.get_slot(pos);
                    auto slot_hdr = reinterpret_cast<const page::bpt_inode_slot*>(slot_data.data());
                    return slot_hdr->child;
                }
                else if(pos == slots.size()) {
                    auto pv = get_page();
                    auto sub_hdr = pv.subheader<page::bpt_inode_header>();
                    return sub_hdr->rightmost_child;
                }
                return invalid_node_type;
            }

            bool can_insert_child(std::size_t, key_like_type k, node_id_type) const {
                const auto slots = get_slots();
                const auto full_slot_size = (k.key.size() + sizeof(page::bpt_inode_slot));
                return (full_slot_size < maximum_slot_size) && slots.can_insert(full_slot_size);
            }

            bool can_update_child(std::size_t, node_id_type) const noexcept {
                return true;
            }

            bool insert_child(std::size_t pos, key_like_type k, node_id_type c) const {
                auto slots = get_slots();
                byte_buffer new_value(k.key.size() + sizeof(page::bpt_inode_slot));
                if (new_value.size() > maximum_slot_size) {
                    return false;
                }
                auto slot_hdr = reinterpret_cast<page::bpt_inode_slot*>(new_value.data());
                slot_hdr->child = c;
                std::memcpy(new_value.data() + slot_hdr->key_offset(), k.key.data(), k.key.size());
                return slots.insert(pos, { new_value });
            }

            bool update_child(std::size_t pos, node_id_type c) const {
                auto slots = get_slots();
                if (pos < slots.size()) {
                    auto value = slots.get_slot(pos);
                    auto slot_hdr = reinterpret_cast<page::bpt_inode_slot*>(value.data());
                    slot_hdr->child = c;
                    return true;
                }
                else if(pos == slots.size()) {
                    auto pv = get_page();
                    auto sub_hdr = pv.subheader<page::bpt_inode_header>();
                    sub_hdr->rightmost_child = c;
                    return true;
                }
                return false;
            }

            slot_directory_type get_slots() const noexcept {
                return get_page().get_slots_dir();
            }

            page_view_type get_page() const noexcept {
                return page_;
            }

            page_view_type page_;
            node_id_type id_{};
        };

        static_assert(concepts::INode<inode_type, key_out_type, key_like_type, key_borrow_type>);

        model(buffer_manager_type& mgr)
            : allocator_(mgr) 
        {}

        struct allocator_type {

            allocator_type(buffer_manager_type& mgr)
                : mgr_(mgr) 
            {}

            leaf_type create_leaf() {
                return {};
            }
            
            inode_type create_inode() {
                return {};
            }

            void destroy(node_id_type id) {
                // no-op
            }

            leaf_type load_leaf(node_id_type id) {
                return {};
            }
            
            inode_type load_inode(node_id_type id) {
                return {};
            }

            std::tuple<node_id_type, bool> load_root() {
                if(root_) {
                    return { *root_, true };
                }
                return { {}, false };
            }

            void set_root(node_id_type id) {
                root_ = {id};
            }

            std::optional<node_id_type> root_ {};
            buffer_manager_type mgr_;
        };

        allocator_type &get_allocator() {
            return allocator_;
        }
        
        const allocator_type &get_allocator() const {
            return allocator_;
        }

    private:
        allocator_type allocator_;
    };

}