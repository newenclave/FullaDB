/*
 * File: model.hpp
 * Author: newenclave
 * GitHub: https://github.com/newenclave
 * Created: 2025-11-01
 * License: MIT
 */

#pragma once

#include "fulla/bpt/concepts.hpp"
#include "fulla/bpt/memory/containter.hpp"

namespace fulla::bpt::memory {

    template <typename KeyT, typename ValueInT, std::size_t KeysMax = 5, typename LessT = std::less<KeyT>>
    struct model {

        using key_type = KeyT;
        using value_type = ValueInT;
        using less_in_type = LessT;

        struct key_like_type {
            explicit key_like_type(const key_type& val) : v(&val) {};
            const key_type& get() const {
                return *v;
            }
        private:
            const key_type* v = nullptr;
        };

        struct key_out_type {
            key_out_type() = default;
            explicit key_out_type(const key_type& val) : v(&val) {};
            const key_type& get() const {
                return *v;
            }
        private:
            const key_type* v = nullptr;
        };

        struct key_borrow_type {
            key_borrow_type() = default;
            explicit key_borrow_type(key_type val) : v(std::move(val)) {};
            const key_type& get() const {
                return v;
            }
        private:
            key_type v{};
        };

        struct value_in_type {
            explicit value_in_type(value_type& val) : v(&val) {}
            const value_type& get() const {
                return *v;
            }
            value_type& get() {
                return *v;
            }
        public:
            value_type *v = nullptr;
        };

        struct value_out_type {
            value_out_type() = default;
            explicit value_out_type(value_type& val) : v(&val) {}
            const value_type& get() const {
                return *v;
            }
            value_type& get() {
                return &v;
            }
        public:
            value_type* v = nullptr;
        };

        struct value_borrow_type {
            value_borrow_type() = default;
            explicit value_borrow_type(value_type &val) : v(std::move(val)) {}
            const value_type& get() const {
                return v;
            }
            value_type& get() {
                return v;
            }
        public:
            value_type v{};
        };

        //using node_id_type = std::size_t;
        struct node_id_type {
            constexpr static const std::size_t npos = std::numeric_limits<std::size_t>::max();
            std::size_t id = npos;
            explicit node_id_type(std::size_t i) : id(i) {}
            node_id_type() = default;
            auto operator == (const node_id_type& other) const noexcept {
                return id == other.id;
            };
        };

        struct cmp {
            constexpr static bool less(const key_type& l, const key_type& r) {
                return LessT{}(l, r);
            }
            constexpr static bool eq(const key_type& l, const key_type& r) {
                return (!less(l, r) && !less(r, l));
            }
            bool operator()(const key_type& l, const key_type& r) const {
                return less(l, r);
            }

            bool operator()(const key_like_type& l, const key_like_type& r) const {
                return less(l.get(), r.get());
            }
        };

        using less_type = cmp;

        using base_container = container::base<node_id_type, key_type, KeysMax>;
        using inode_container = container::inode<node_id_type, key_type, KeysMax>;
        using leaf_container = container::leaf<node_id_type, key_type, value_type, KeysMax>;

        using container_uptr = std::unique_ptr<base_container>;

        struct node_base {

            using node_id_type = model::node_id_type;

            ~node_base() = default;

            node_base() = default;
            node_base(base_container* base, node_id_type self_id) : base_(base), self_(self_id) {};

            bool is_valid() const noexcept {
                return base_ != nullptr;
            }

            virtual std::size_t key_position(const key_like_type &key) const noexcept { // inode impl
                const auto itr = std::ranges::upper_bound(base_->keys_, key.get(), cmp{});
                return std::distance(base_->keys_.begin(), itr);
            }

            static std::size_t keys_maximum() noexcept { return KeysMax; }

            std::size_t keys_count() const noexcept {
                return base_->keys_.size();
            }

            bool keys_eq(const key_like_type& lhs, const key_like_type& rhs) const noexcept {
                return cmp{}.eq(lhs.get(), rhs.get());
            }

            const key_out_type get_key(std::size_t pos) const noexcept {
                return key_out_type{ base_->keys_[pos] };
            };

            key_borrow_type borrow_key(std::size_t pos) noexcept {
                return key_borrow_type{ std::move(base_->keys_[pos]) };
            };

            bool insert_key(std::size_t pos, const key_like_type &key) {
                base_->keys_.emplace(base_->keys_.begin() + pos, key.get());
                return true;
            }

            bool update_key(std::size_t pos, const key_like_type& key) {
                base_->keys_[pos] = key_type{ key.get() };
                return true;
            }

            bool erase_key(std::size_t pos) {
                base_->keys_.erase(base_->keys_.begin() + pos);
                return true;
            }

            void set_parent(node_id_type id) noexcept {
                base_->parent_ = id;
            }

            node_id_type get_parent() const noexcept {
                return base_->parent_;
            }

            node_id_type self() const noexcept {
                return self_;
            }

            base_container* base_ = nullptr;
            node_id_type self_ = {};
        };

        struct inode : public node_base {

            using node_id_type = node_base::node_id_type;

            inode() = default;
            inode(base_container* base, node_id_type self_id)
                : node_base(base, self_id)
            {
            }

            inode_container* impl() {
                return this->base_->as<inode_container>();
            }

            const inode_container* impl() const {
                return this->base_->as<const inode_container>();
            }

            std::size_t children_count() const {
                return impl()->children_.size();
            }

            node_id_type get_child(std::size_t pos) const {
                return impl()->children_[pos];
            }

            bool insert_child(std::size_t pos, node_id_type id) {
                impl()->children_.emplace(impl()->children_.begin() + pos, id);
                return true;
            }

            bool update_child(std::size_t pos, node_id_type id) {
                impl()->children_[pos] = id;
                return true;
            }

            bool erase_child(std::size_t pos) {
                impl()->children_.erase(impl()->children_.begin() + pos);
                return true;
            }
        };

        struct leaf_node : public node_base {
            using node_id_type = node_base::node_id_type;

            leaf_node() = default;
            leaf_node(base_container* base, node_id_type self_id)
                : node_base(base, self_id)
            {
            }

            std::size_t key_position(const key_like_type& key) const noexcept override { // leaf impl
                const auto itr = std::ranges::lower_bound(impl()->keys_, key.get(), cmp{});
                return std::distance(impl()->keys_.begin(), itr);
            }

            std::size_t values_count() const noexcept {
                return impl()->values_.size();
            }

            leaf_container* impl() noexcept {
                return this->base_->as<leaf_container>();
            }

            const leaf_container* impl() const noexcept {
                return this->base_->as<const leaf_container>();
            }

            value_out_type get_value(std::size_t pos) noexcept {
                return value_out_type(impl()->values_[pos]);
            }

            value_borrow_type borrow_value(std::size_t pos) noexcept {
                return value_borrow_type(impl()->values_[pos]);
            }

            bool insert_value(std::size_t pos, value_in_type val) {
                impl()->values_.emplace(impl()->values_.begin() + pos, std::move(val.get()));
                return true;
            }

            bool update_value(std::size_t pos, value_in_type val) {
                impl()->values_[pos] = std::move(val.get());
                return true;
            }

            bool erase_value(std::size_t pos) {
                impl()->values_.erase(impl()->values_.begin() + pos);
                return true;
            }

            void set_next(node_id_type id) noexcept {
                impl()->next_ = id;
            }

            void set_prev(node_id_type id) noexcept {
                impl()->prev_ = id;
            }

            node_id_type get_next() const noexcept {
                return impl()->next_;
            }

            node_id_type get_prev() const noexcept {
                return impl()->prev_;
            }
        };

        struct accessor_type {

            using node_id_type = model::node_id_type;
            using leaf_type = leaf_node;
            using inode_type = inode;

            constexpr const static std::size_t invalid_id = std::numeric_limits<std::size_t>::max();

            accessor_type() = default;
            accessor_type(const accessor_type&) = delete;
            accessor_type& operator = (const accessor_type&) = delete;

            leaf_node create_leaf() {
                nodes_.emplace_back(std::make_unique<leaf_container>());
                return leaf_node{ nodes_.back().get(), node_id_type(nodes_.size()) };
            }

            inode create_inode() {
                nodes_.emplace_back(std::make_unique<inode_container>());
                return inode{ nodes_.back().get(), node_id_type(nodes_.size()) };
            }

            bool destroy(node_id_type id) {
                if (valid_id(id.id)) {
                    nodes_[id.id - 1].reset();
                    return true;
                }
                return false;
            }

            leaf_node load_leaf(node_id_type id) {
                if (valid_id(id.id) && nodes_[id.id - 1]->is_leaf()) {
                    return leaf_node{ nodes_[id.id - 1].get(), id };
                }
                return {};
            }

            const leaf_node load_leaf(node_id_type id) const {
                if (valid_id(id.id) && nodes_[id.id - 1]->is_leaf()) {
                    return leaf_node{ nodes_[id.id - 1].get(), id };
                }
                return {};
            }

            inode load_inode(node_id_type id) {
                if (valid_id(id.id) && (!nodes_[id.id - 1]->is_leaf())) {
                    return inode{ nodes_[id.id - 1].get(), id };
                }
                return {};
            }

            const inode load_inode(node_id_type id) const {
                if (valid_id(id.id) && (!nodes_[id.id - 1]->is_leaf())) {
                    return inode{ nodes_[id.id - 1].get(), id };
                }
                return {};
            }

            bool is_leaf_id(node_id_type id) const {
                return valid_id(id.id) && nodes_[id.id - 1]->is_leaf();
            }

            std::tuple<node_id_type, bool> load_root() const {
                if (root_.id != invalid_id) {
                    return { root_, true };
                }
                return { {}, false };
            }

            void set_root(node_id_type id) {
                root_ = id;
            }

            bool valid_id(std::size_t id) const {
                return (id > 0) && (id <= nodes_.size() && nodes_[id - 1]);
            }

            std::vector<container_uptr> nodes_;
            node_id_type root_ = {};
        };

        using leaf_type = leaf_node;
        using inode_type = inode;

        static_assert(concepts::INode<inode, key_out_type, key_like_type, key_borrow_type>);
        static_assert(concepts::LeafNode<leaf_node, key_out_type, key_like_type, key_borrow_type, value_out_type, value_in_type, value_borrow_type>);
        static_assert(concepts::NodeAccessor<accessor_type, node_id_type, inode, leaf_node>);

        static key_like_type key_out_as_like(key_out_type k) {
            return key_like_type(k.get());
        }

        static key_like_type key_borrow_as_like(key_borrow_type& k) {
            return key_like_type(k.get());
        }

        static value_in_type value_out_as_in(value_out_type k) {
            return value_in_type(k.get());
        }

        static value_in_type value_borrow_as_in(value_borrow_type &k) {
            return value_in_type(k.get());
        }

        static bool is_valid_id(const node_id_type& id) {
            return id.id != node_id_type::npos;
        }

        bool is_leaf_id(const node_id_type& id) const {
            return get_accessor().is_leaf_id(id);
        }

        accessor_type& get_accessor() {
            return accessor_;
        }

        const accessor_type& get_accessor() const {
            return accessor_;
        }

    private:
        accessor_type accessor_;
    };

} // namespace fulla::bpt