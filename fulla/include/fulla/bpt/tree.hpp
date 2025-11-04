/*
 * File: bpt.hpp
 * Author: newenclave
 * GitHub: https://github.com/newenclave
 * Created: 2025-11-01
 * License: MIT
 */

#pragma once

#include <optional>
#include <format>

#include "fulla/bpt/concepts.hpp"
#include "fulla/bpt/policies.hpp"
#include "fulla/bpt/cursor.hpp"

namespace fulla::bpt {

    template <concepts::BptModel ModelT>
    class tree {

    public:
        using model_type = ModelT;
        using accessor_type = typename model_type::accessor_type;
        
        using key_out_type = typename model_type::key_out_type;
        using key_like_type = typename model_type::key_like_type;
        using key_borrow_type = typename model_type::key_borrow_type;
        using value_in_type = typename model_type::value_in_type;
        using value_out_type = typename model_type::value_out_type;
        using value_borrow_type = typename model_type::value_borrow_type;

        using node_id_type = typename ModelT::node_id_type;
        using leaf_type = typename ModelT::leaf_type;
        using inode_type = typename ModelT::inode_type;

        constexpr static const std::size_t npos = std::numeric_limits<std::size_t>::max();

        tree() = default;

        template <typename ...Args>
        tree(Args&&...args) : model_(std::forward<Args>(args)...) {}

        class iterator {
        public:
            using difference_type = std::ptrdiff_t;
            using value_type = std::pair<key_out_type, value_out_type>;
            using iterator_category = std::bidirectional_iterator_tag;
            using reference = const value_type&;
            using pointer = const value_type*;
            using iterator_category = std::bidirectional_iterator_tag;
            using iterator_concept = std::bidirectional_iterator_tag;
        public:

            friend class tree;

            iterator() = default;

            iterator(tree* t, node_id_type leaf, std::size_t idx)
                : tree_(t)
                , leaf_(leaf)
                , idx_(idx)
            {
            }

            reference operator*() const {
                return deref();
            }

            auto operator -> () const {
                struct proxy {
                    const value_type* p;
                    const value_type* operator->() const {
                        return p;
                    }
                };
                return proxy{ &deref() };
            }

            iterator& operator++() {
                invalidate_cache();
                auto leaf = tree_->model_.get_accessor().load_leaf(leaf_);
                if (idx_ + 1 < leaf.size()) {
                    ++idx_;
                }
                else {
                    auto nxt = leaf.get_next();
                    if (!is_end()) {
                        leaf_ = nxt;
                        idx_ = 0;
                    }
                }
                return *this;
            }

            iterator operator++(int) {
                auto tmp = *this;
                ++(*this);
                return tmp;
            }

            iterator& operator--() {
                invalidate_cache();
                if (is_end()) {
                    // end() -> last element
                    auto [root, exists] = tree_->model_.get_accessor().load_root();
                    if (exists) {
                        leaf_ = tree_->get_rightmost_leaf(root);
                        auto leaf = tree_->model_.get_accessor().load_leaf(leaf_);
                        idx_ = leaf.size() - 1;
                    }
                    return *this;
                }
                if (idx_ > 0) {
                    --idx_;
                }
                else {
                    auto leaf = tree_->model_.get_accessor().load_leaf(leaf_);
                    auto prv = leaf.get_prev();
                    auto pnode = tree_->model_.get_accessor().load_leaf(prv);
                    leaf_ = prv;
                    idx_ = pnode.size() - 1;
                }
                return *this;
            }

            iterator operator--(int) {
                auto tmp = *this;
                --(*this);
                return tmp;
            }

            iterator& operator+=(difference_type n) {
                if (n >= 0) {
                    while (n--) {
                        ++(*this);
                    }
                }
                else {
                    while (n++) {
                        --(*this);
                    }
                }
                return *this;
            }

            iterator operator+(difference_type n) const {
                auto c = *this;
                c += n;
                return c;
            }

            friend bool operator==(const iterator& a, const iterator& b) {
                return (a.tree_ == b.tree_) && (a.leaf_ == b.leaf_) && (a.idx_ == b.idx_);
            }

        private:

            const value_type& deref() const {
                if (!cache_) {
                    auto leaf = tree_->model_.get_accessor().load_leaf(leaf_);
                    cache_.emplace(leaf.get_key(idx_), leaf.get_value(idx_));
                }
                return *cache_;
            }

            void invalidate_cache() {
                cache_.reset();
            }

            bool is_end() const {
                return !tree_->model_.is_valid_id(leaf_);
            }

            tree* tree_{};
            node_id_type leaf_{};
            std::size_t idx_{ 0 };
            mutable std::optional<value_type> cache_;
        };

        struct search_result {
            node_id_type node = {};
            std::size_t pos = 0;
            bool found = false;
        };

        iterator begin() {
            auto [root, exists] = get_accessor().load_root();
            if (exists) {
                auto leftmost = get_leftmost_leaf(root);
                return iterator(this, leftmost, 0);
            }
            else {
                return end();
            }
        }

        iterator end() {
            return iterator(this, {}, 0);
        }

        bool insert(const key_like_type& key, value_in_type value, 
            policies::insert ip, 
            policies::rebalance rp = policies::rebalance::force_split) {
            auto& accessor = get_accessor();
            auto [root, exists] = accessor.load_root();
            if (!exists) {
                auto new_leaf = accessor.create_leaf();
                new_leaf.insert_value(0, key, std::move(value));
                accessor.set_root(new_leaf.self());
                return true;
            }
            else {
                auto [node_id, pos, found] = find_node_with_(key, root);
                auto leaf = accessor.load_leaf(node_id);
                if (!found) {
                    if (is_full(leaf)) {
                        if (rp == policies::rebalance::force_split) {
                            handle_leaf_overflow_default(leaf, key, std::move(value), pos, rp);
                        }
                        else {
                            if (!try_leaf_neighbor_share(leaf, key, value, pos, rp)) {
                                handle_leaf_overflow_default(leaf, key, std::move(value), pos, rp);
                            }
                        }
                    }
                    else {
                        leaf.insert_value(pos, key, std::move(value));
                    }
                    return true;
                }
                else if (ip == policies::insert::upsert) {
                    leaf.update_value(pos, std::move(value));
                    return true;
                }
            }
            return false;
        }

        bool remove(const key_like_type &key) {
            auto& accessor = get_accessor();
            auto [nodeid, pos, found] = find_node_with(key);
            if (found) {
                auto node = accessor.load_leaf(nodeid);
                return remove_impl(key, node, pos);
            }
            return false;
        }

        iterator erase(iterator where) {
            if (where == end()) {
                return where;
            }

            auto after = std::next(where);
            auto where_node = model_.get_accessor().load_leaf(where.leaf_);

            if (after != end()) {
                auto k_after_like = model_.key_out_as_like(after->first);
                remove_impl(model_.key_out_as_like(where->first), where_node, where.idx_);
                return find(k_after_like);
            }
            else {
                remove_impl(model_.key_out_as_like(where->first), where_node, where.idx_);
                return end();
            }
        }

        iterator find(key_like_type key) {
            auto [nodeid, pos, found] = find_node_with(key);
            if (found) {
                return iterator(this, nodeid, pos);
            }
            else {
                return end();
            }
        }

        void dump() {
            auto [root, exists] = get_accessor().load_root();
            if (exists) {
                dump_node(root, 0);
            }
            else {
                std::cout << "<Empty>\n";
            }
        }

        void dump_node(node_id_type node, int level) const {
            for (int i = 0; i < level; ++i) {
                std::cout << "  ";
            }

            const auto& accessor = get_accessor();

            std::ostringstream leaf_info;
            const bool is_leaf = model_.is_leaf_id(node);
            if (is_leaf) {
                leaf_info << "* "
                    ;
            }

            auto leaf = accessor.load_leaf(node);
            auto inode = accessor.load_inode(node);

            //std::cout << std::format("<{} p:{}>", leaf ? leaf.self() : inode.self(), leaf ? leaf.get_parent() : inode.get_parent());
            if (leaf.is_valid()) {
                std::cout << std::format("<{} p:{} cap:{}>", leaf.self().id, leaf.get_parent().id, leaf.capacity());
            }
            else {
                std::cout << std::format("<{} p:{} cap:{}>", inode.self().id, inode.get_parent().id, inode.capacity());
            }
            std::cout
                << std::dec << " "
                << leaf_info.str()
                << std::dec << " [";

            const auto access = [](auto value) {
                for (size_t i = 0; i < value.size(); ++i) {
                    if (i > 0) {
                        std::cout << ", ";
                    }
                    std::cout << *value.get_key(i).get();

                }
                };

            if (leaf.is_valid()) {
                access(leaf);
            }
            else if (inode.is_valid()) {
                access(inode);
            }

            std::cout << "]";


            if (!is_leaf) {
                std::cout << " children: " << inode.size() + 1;
            }

            std::cout << "\n";

            if (!is_leaf) {
                for (std::size_t id = 0; id < inode.size() + 1; ++id) {
                    dump_node(inode.get_child(id), level + 1);
                }
            }
        }

        model_type& get_model() {
            return model_;
        }

        const model_type& get_model() const {
            return model_;
        }

        //private:

        bool remove_impl(const key_like_type& key, leaf_type &node, std::size_t pos) {
            auto& accessor = get_accessor();
            node.erase(pos);
            if (pos == 0 && (node.size() > 0)) {
                fix_parent_index(node);
            }
            handle_leaf_underflow(node, key);
            auto [root, _] = accessor.load_root();
            const auto root_size = visit_node([](auto& r) { return r.size(); }, root);
            if (root_size == 0) {
                accessor.set_root({});
                accessor.destroy(root);
            }
            return true;
        }

        struct split_leaf_result {
            leaf_type right;
            key_out_type key;
            operator bool() const {
                return right.is_valid();
            }
        };

        struct split_inode_result {
            inode_type right;
            key_borrow_type key;
            operator bool() const {
                return right.is_valid();
            }
        };

        split_inode_result split_inode(inode_type& node) {
            const auto maximum = node.capacity();
            const auto middle_element = maximum / 2;
            const auto reduce_size = (maximum - middle_element);

            auto right = get_accessor().create_inode();
            if (right.is_valid()) {
                auto key = node.borrow_key(middle_element);

                right.set_parent(node.get_parent());

                for (std::size_t id = middle_element + 1; id < node.size(); ++id) {
                    auto borrow_key = node.borrow_key(id);
                    auto next_child = node.get_child(id);
                    visit_node([&](auto& cnode) { cnode.set_parent(right.self()); }, next_child);
                    right.insert_child(right.size(), model_.key_borrow_as_like(borrow_key), next_child);
                }

                // latest child 
                auto last_child = node.get_child(node.size());
                visit_node([&](auto& cnode) { cnode.set_parent(right.self()); }, last_child);
                right.update_child(right.size(), last_child);

                for (std::size_t i = 0; i < reduce_size; ++i) {
                    const auto last_key = node.size() - 1;
                    swap_children(node, last_key, last_key + 1);
                    node.erase(last_key);
                }

                /// check if the children are leafs..?
                return { std::move(right), std::move(key) };
            }
            return {};
        }

        split_leaf_result split_leaf(leaf_type& node) {
            const auto maximum = node.capacity();
            const auto middle_element = maximum / 2;
            const auto reduce_size = (maximum - middle_element);

            const auto node_id = node.self();
            auto& accessor = get_accessor();

            auto right = accessor.create_leaf();
            if (right.is_valid()) {
                for (std::size_t id = middle_element; id < node.size(); ++id) {
                    const auto last_element = right.size();

                    auto borrow_key = node.borrow_key(id);
                    auto borrow_val = node.borrow_value(id);

                    auto id_like = model_.key_borrow_as_like(borrow_key);
                    auto val_in = model_.value_borrow_as_in(borrow_val);

                    right.insert_value(last_element, id_like, std::move(val_in));
                }
                right.set_parent(node.get_parent());

                right.set_prev(node_id);
                right.set_next(node.get_next());
                node.set_next(right.self());

                {
                    auto right_next_id = right.get_next();
                    if (model_.is_valid_id(right_next_id)) {
                        auto next = accessor.load_leaf(right_next_id);
                        next.set_prev(right.self());
                    }
                }

                for (std::size_t i = 0; i < reduce_size; ++i) {
                    const auto last_key = (node.size() - 1);
                    node.erase(last_key);
                }

                auto key = right.get_key(0);
                return { std::move(right), std::move(key) };
            }

            return {};
        }

        void handle_leaf_overflow_default(leaf_type& node, const key_like_type& key, 
            value_in_type value, std::size_t pos, policies::rebalance rp) {
            auto res_node = handle_leaf_overflow(node, rp);
            if (node.size() < pos) {
                const auto insert_pos = pos - node.size();
                res_node.insert_value(insert_pos, key, std::move(value));
            }
            else {
                node.insert_value(pos, key, std::move(value));
            }
        }

        void handle_inode_overflow(inode_type& node, node_id_type leftmost, policies::rebalance rp) {

            auto& accessor = get_accessor();
            inode_type new_root;
            if (!model_.is_valid_id(node.get_parent())) { // is node root_?
                new_root = accessor.create_inode();
            }
            auto [root_id, exists] = accessor.load_root();
            auto [right, key] = split_inode(node);
            if (right.is_valid()) { 

                if (new_root.is_valid()) { // node is root
                    new_root.insert_child(0, model_.key_borrow_as_like(key), root_id);
                    new_root.update_child(1, right.self());

                    visit_node([&](auto& c) { c.set_parent(new_root.self()); }, new_root.get_child(0));
                    visit_node([&](auto& c) { c.set_parent(new_root.self()); }, new_root.get_child(1));

                    accessor.set_root(new_root.self());
                }
                else {
                    auto parent = node.get_parent();
                    auto pos = find_child_index_in_parent(parent, node.self());
                    auto pnode = accessor.load_inode(parent);

                    if (is_full(pnode)) {
                        if (!(give_to_right(pnode, 1, rp) || give_to_left(pnode, 1, rp))) {
                            handle_inode_overflow(pnode, leftmost, rp);
                        }
                    }
                    parent = node.get_parent();
                    right.set_parent(parent);
                    pos = find_child_index_in_parent(parent, node.self());
                    pnode = accessor.load_inode(parent);

                    const auto pos_child = pnode.get_child(pos);
                    pnode.insert_child(pos, model_.key_borrow_as_like(key), pos_child);
                    pnode.update_child(pos + 1, right.self());
                }
            }
        }

        leaf_type handle_leaf_overflow(leaf_type& node, policies::rebalance rp) {

            const auto node_id = node.self();
            auto& accessor = get_accessor();

            inode_type new_root{};
            if (!model_.is_valid_id(node.get_parent())) { // node is root_?
                new_root = accessor.create_inode();
            }
            if (auto split_right = split_leaf(node)) {
                auto&& [right, key] = split_right;

                if (new_root.is_valid()) { // node is root_;
                    const auto first_like = model_.key_out_as_like(right.get_key(0));
                    right.set_parent(new_root.self());

                    auto [current_root, exists] = accessor.load_root();
                    visit_node([&](auto& c) { c.set_parent(new_root.self()); }, current_root);

                    new_root.insert_child(0, first_like, current_root);
                    new_root.update_child(1, right.self());

                    accessor.set_root(new_root.self());

                    return right;
                }
                else {
                    auto parent_id = node.get_parent();
                    auto pos = find_child_index_in_parent(parent_id, node_id);
                    auto parent = accessor.load_inode(parent_id);

                    if (is_full(parent)) {
                        if (!(give_to_right(parent, 1, rp) || give_to_left(parent, 1, rp))) {
                            handle_inode_overflow(parent, node.self(), rp);
                        }
                    }
                    parent_id = node.get_parent();
                    pos = find_child_index_in_parent(parent_id, node_id);

                    if (pos == npos) {
                        std::cout << "";
                    }

                    parent = accessor.load_inode(parent_id);

                    right.set_parent(parent_id);

                    const auto first_like = model_.key_out_as_like(right.get_key(0));
                    const auto pos_child = parent.get_child(pos);

                    parent.insert_child(pos, first_like, pos_child); // insert the same child
                    parent.update_child(pos + 1, right.self()); // update shifted

                    return right;
                }
            }
            return {};
        }

        void handle_inode_underflow(inode_type& node, const key_like_type& key) {

            auto& accessor = get_accessor();

            const auto maximum = node.capacity();
            const auto middle_element = maximum / 2;

            const auto pos = node.key_position(key);
            if (pos > 0 && (pos <= node.size())) {
                const auto out_like = model_.key_out_as_like(node.get_key(pos - 1));
                if (node.keys_eq(key, out_like)) {
                    const auto first_leaf = accessor.load_leaf(get_leftmost_leaf(node.get_child(pos)));
                    const auto first_like = model_.key_out_as_like(first_leaf.get_key(0));
                    node.update_key(pos - 1, first_like);
                }
            }
            auto tmp = try_merge_inode(node);
            if (tmp.is_valid()) {
                node = tmp;
            }
            else if (node.size() < middle_element) {
                borrow_from_right(node, 0) || borrow_from_left(node, 0);
            }

            auto parent = accessor.load_inode(node.get_parent());
            if (parent.is_valid()) {
                handle_inode_underflow(parent, key);
                fix_zero_root(parent);
            }
        }

        void handle_leaf_underflow(leaf_type& node, const key_like_type& key) {

            const auto maximum = node.capacity();
            const auto middle_element = maximum / 2;

            auto tmp = try_merge_leaf(node);
            if (tmp.is_valid()) {
                node = tmp;
            }
            else if (node.size() < middle_element) {
                borrow_from_right(node, 0) || borrow_from_left(node, 0);
            }
            auto parent = get_accessor().load_inode(node.get_parent());
            if (parent.is_valid()) {
                handle_inode_underflow(parent, key);
                fix_zero_root(parent);
            }
        }

        void fix_zero_root(inode_type& node) {
            auto& accessor = get_accessor();
            if (node.size() == 0) {
                if (!model_.is_valid_id(node.get_parent())) {
                    auto [root, _] = accessor.load_root();
                    auto proot = accessor.load_inode(root);
                    auto next_child = proot.get_child(0);
                    accessor.destroy(root);
                    accessor.set_root(next_child);
                    if (model_.is_valid_id(next_child)) {
                        visit_node([](auto& c) { c.set_parent({}); }, next_child);
                    }
                }
            }
        }

#pragma region "borrowing and giving"
        //region borrowing and giving
        //      LEAF
        bool leaf_borrow_from_left(leaf_type& node, std::size_t additional_elements) {
            auto left = get_accessor().load_leaf(find_left_sibling(node));
            if (left.is_valid()) {
                return leaf_borrow_from_left_impl(node, left, additional_elements);
            }
            return false;
        }

        bool leaf_borrow_from_left_impl(leaf_type& node, leaf_type& left, std::size_t additional_elements) {

            const auto max_elements = node.capacity();
            const auto min_elements = (max_elements + 1) / 2 - 1;

            if (left.size() > (min_elements + additional_elements)) {

                auto borrowed_key = left.borrow_key(left.size() - 1);
                auto borrowed_val = left.borrow_value(left.size() - 1);
                auto key = model_.key_borrow_as_like(borrowed_key);
                auto value = model_.value_borrow_as_in(borrowed_val);

                node.insert_value(0, key, value);

                const auto last_key = left.size() - 1;
                left.erase(last_key);

                auto parent = get_accessor().load_inode(node.get_parent());
                auto pos = find_child_index_in_parent(parent, node.self());
                parent.update_key(pos - 1, model_.key_out_as_like(node.get_key(0)));

                return true;
            }
            return false;
        }

        bool leaf_borrow_from_right(leaf_type& node, std::size_t additional_elements) {
            auto right = get_accessor().load_leaf(find_right_sibling(node));
            if (right.is_valid()) {
                return leaf_borrow_from_right_impl(node, right, additional_elements);
            }
            return false;
        }

        bool leaf_borrow_from_right_impl(leaf_type& node, leaf_type& right, std::size_t additional_elements) {

            const auto max_elements = node.capacity();
            const auto min_elements = (max_elements + 1) / 2 - 1;

            if (right.size() > (min_elements + additional_elements)) {

                auto borrowed_key = right.borrow_key(0);
                auto borrowed_value = right.borrow_value(0);

                auto key = model_.key_borrow_as_like(borrowed_key);
                auto value = model_.value_borrow_as_in(borrowed_value);

                node.insert_value(node.size(), std::move(key), std::move(value));

                right.erase(0);

                auto parent = get_accessor().load_inode(node.get_parent());
                auto pos = find_child_index_in_parent(parent, node.self());
                parent.update_key(pos, model_.key_out_as_like(right.get_key(0)));

                return true;
            }
            return false;
        }

        bool try_leaf_neighbor_share(leaf_type& node, const key_like_type& key, value_in_type& value, std::size_t pos, policies::rebalance rp) {
            auto& accessor = get_accessor();

            const bool first = (pos == 0);
            const bool last = (pos == node.size());

            if (give_to_left<leaf_type>(node, first ? 1 : 0, rp)) {
                if (first) {
                    auto left_sibling = accessor.load_leaf(find_left_sibling(node));
                    left_sibling.insert_value(left_sibling.size(), key, std::move(value));
                }
                else {
                    pos--;
                    node.insert_value(pos, key, std::move(value));
                    if (pos == 0) {
                        fix_parent_index(node);
                    }
                }
                return true;
            }
            else if (give_to_right<leaf_type>(node, last ? 1 : 0, rp)) {
                if (last) {
                    auto right_sibling = accessor.load_leaf(find_right_sibling(node));
                    pos = right_sibling.key_position(key);
                    right_sibling.insert_value(pos, key, std::move(value));
                    fix_parent_index(right_sibling);
                }
                else {
                    node.insert_value(pos, key, std::move(value));
                    if (pos == 0) {
                        fix_parent_index(node);
                    }
                }
                return true;
            }
            return false;
        }

        //      INODE
        bool inode_borrow_from_left(inode_type& node, std::size_t additional_elements) {
            auto left = get_accessor().load_inode(find_left_sibling(node));
            if (left.is_valid()) {
                return inode_borrow_from_left_impl(node, left, additional_elements);
            }
            return false;
        }

        bool inode_borrow_from_left_impl(inode_type& node, inode_type& left, std::size_t additional_elements) {
            const auto max_elements = node.capacity();
            const auto min_elements = (max_elements + 1) / 2 - 1;

            if (left.size() > (min_elements + additional_elements)) {

                auto parent = get_accessor().load_inode(node.get_parent());
                const auto pos = find_child_index_in_parent(parent, node.self());

                auto borrow_parent_key = parent.borrow_key(pos - 1);
                auto borrow_key = left.borrow_key(left.size() - 1);

                auto parent_key = model_.key_borrow_as_like(borrow_parent_key);
                auto key = model_.key_borrow_as_like(borrow_key);
                auto child = std::move(left.get_child(left.size()));

                visit_node([&](auto& c) { c.set_parent(node.self()); }, child);

                parent.update_key(pos - 1, std::move(key));
                node.insert_child(0, parent_key, child);

                const auto last_key = left.size() - 1;
                swap_children(left, left.size() - 1, left.size());
                left.erase(last_key);

                return true;
            }
            return false;
        }

        bool inode_borrow_from_right(inode_type& node, std::size_t additional_elements) {
            auto right = get_accessor().load_inode(find_right_sibling(node));
            if (right.is_valid()) {
                return inode_borrow_from_right_impl(node, right, additional_elements);
            }
            return false;
        }

        bool inode_borrow_from_right_impl(inode_type& node, inode_type& right, std::size_t additional_elements) {

            const auto max_elements = node.capacity();
            const auto min_elements = (max_elements + 1) / 2 - 1;

            if (right.size() > (min_elements + additional_elements)) {

                auto parent = get_accessor().load_inode(node.get_parent());
                const auto pos = find_child_index_in_parent(parent, node.self());

                auto borrow_parent_key = parent.borrow_key(pos);
                auto borrow_key = right.borrow_key(0);

                auto parent_key = model_.key_borrow_as_like(borrow_parent_key);
                auto key = model_.key_borrow_as_like(borrow_key);
                auto child = std::move(right.get_child(0));

                visit_node([&](auto& c) { c.set_parent(node.self()); }, child);

                parent.update_key(pos, key);
                const auto last_child = node.get_child(node.size());

                node.insert_child(node.size(), parent_key, last_child); // insert the same
                node.update_child(node.size(), child); // update the latest

                right.erase(0);

                return true;
            }
            return false;
        }

        //      common 
        template <typename NodeT>
        bool borrow_from_left_impl(NodeT& node, NodeT& left, std::size_t additional_elements) {
            if constexpr (std::is_same_v<NodeT, inode_type>) {
                return inode_borrow_from_left_impl(node, left, additional_elements);
            }
            else if constexpr (std::is_same_v<NodeT, leaf_type>) {
                return leaf_borrow_from_left_impl(node, left, additional_elements);
            }
        }

        template <typename NodeT>
        bool borrow_from_left(NodeT& node, std::size_t additional_elements) {
            if constexpr (std::is_same_v<NodeT, inode_type>) {
                return inode_borrow_from_left(node, additional_elements);
            }
            else if constexpr (std::is_same_v<NodeT, leaf_type>) {
                return leaf_borrow_from_left(node, additional_elements);
            }
        }

        template <typename NodeT>
        bool borrow_from_right_impl(NodeT& node, NodeT& right, std::size_t additional_elements) {
            if constexpr (std::is_same_v<NodeT, inode_type>) {
                return inode_borrow_from_right_impl(node, right, additional_elements);
            }
            else if constexpr (std::is_same_v<NodeT, leaf_type>) {
                return leaf_borrow_from_right_impl(node, right, additional_elements);
            }
        }

        template <typename NodeT>
        bool borrow_from_right(NodeT& node, std::size_t additional_elements) {
            if constexpr (std::is_same_v<NodeT, inode_type>) {
                return inode_borrow_from_right(node, additional_elements);
            }
            else if constexpr (std::is_same_v<NodeT, leaf_type>) {
                return leaf_borrow_from_right(node, additional_elements);
            }
        }

        template <typename NodeT>
        bool give_to_left(NodeT& node, std::size_t additional_elements, policies::rebalance rp) {

            const auto load_call = [this](auto id) { return load<NodeT>(id); };
            const auto borrow_impl_call = [this](auto& n, auto& r, auto ae) { return borrow_from_right_impl<NodeT>(n, r, ae); };
            const auto borrow_call = [this](auto& n, auto ae) { return borrow_from_right<NodeT>(n, ae); };

            if (rp == policies::rebalance::local_rebalance) {

                auto parent = get_accessor().load_inode(node.get_parent());
                const auto pos = find_child_index_in_parent(parent, node.self());

                bool res = false;
                if ((pos != npos) && (pos > 0)) {

                    auto start_pos = pos;

                    while (start_pos > 1 && (is_full(load_call(parent.get_child(start_pos))))) {
                        --start_pos;
                    }

                    for (std::size_t id = start_pos; id <= pos; ++id) {

                        auto left = load_call(parent.get_child(id - 1));
                        auto right = load_call(parent.get_child(id));

                        const bool is_last = (right.self() == node.self());

                        if (!is_last) {
                            if (!is_full(left)) {
                                borrow_impl_call(left, right, 0);
                            }
                        }
                        else {
                            if (left.size() < (left.capacity() - additional_elements)) {
                                res = borrow_impl_call(left, right, 0);
                            }
                        }
                    }
                }
                return res;
            }
            else if (rp == policies::rebalance::neighbor_share) {
                auto left = load_call(find_left_sibling(node));
                if (left.is_valid()) {
                    if (left.size() < (left.capacity() - additional_elements)) {
                        return borrow_call(left, 0);
                    }
                }
            }
            return false;
        }

        template <typename NodeT>
        bool give_to_right(NodeT& node, std::size_t additional_elements, policies::rebalance rp) {

            const auto load_call = [this](auto id) { return load<NodeT>(id); };
            const auto borrow_impl_call = [this](auto& n, auto& l, auto ae) { return borrow_from_left_impl<NodeT>(n, l, ae); };
            const auto borrow_call = [this](auto& n, auto ae) { return borrow_from_left<NodeT>(n, ae); };

            if (rp == policies::rebalance::local_rebalance) {

                auto parent = get_accessor().load_inode(node.get_parent());
                auto pos = find_child_index_in_parent(parent, node.self());
                bool res = false;

                if ((pos != npos) && (pos < (parent.size() - 1))) {
                    const auto parent_size = parent.size();

                    auto start_pos = pos;

                    while (start_pos < parent_size && (is_full(load_call(parent.get_child(start_pos))))) {
                        ++start_pos;
                    }

                    for (std::size_t id = start_pos; id > pos; --id) {

                        auto left = load_call(parent.get_child(id - 1));
                        auto right = load_call(parent.get_child(id));

                        const bool is_last = (left.self() == node.self());

                        if (is_last) {
                            if (right.size() < (right.capacity() - additional_elements)) {
                                res = borrow_impl_call(right, left, 0);
                            }
                        }
                        else {
                            if (!is_full(right)) {
                                borrow_impl_call(right, left, 0);
                            }
                        }
                    }
                }
                return res;
            }
            else if (rp == policies::rebalance::neighbor_share) {
                auto right = load_call(find_right_sibling(node));
                if (right.is_valid()) {
                    if (right.size() < (right.capacity() - additional_elements)) {
                        return borrow_call(right, 0);
                    }
                }
            }
            return false;
        }

        //endregion borrowing and giving
#pragma endregion "borrowing and giving"


#pragma region "merging"
        //region merging

        leaf_type merge_leaf_with_right(leaf_type& node) {
            auto& accessor = get_accessor();
            auto right = accessor.load_leaf(find_right_sibling(node));
            if (right.is_valid()) {
                if ((right.size() + node.size()) <= node.capacity()) {

                    auto parent = accessor.load_inode(node.get_parent());
                    const auto right_pos = find_child_index_in_parent(parent, right.self());

                    for (std::size_t id = 0; id < right.size(); ++id) {

                        auto borrow_key = right.borrow_key(id);
                        auto borrow_val = right.borrow_value(id);

                        auto id_like = model_.key_borrow_as_like(borrow_key);
                        auto val_in = model_.value_borrow_as_in(borrow_val);

                        node.insert_value(node.size(), std::move(id_like), std::move(val_in));
                    }

                    node.set_next(right.get_next());
                    {
                        auto next = accessor.load_leaf(node.get_next());
                        if (next.is_valid()) {
                            next.set_prev(node.self());
                        }
                    }

                    swap_children(parent, right_pos - 1, right_pos);

                    accessor.destroy(parent.get_child(right_pos - 1));
                    parent.erase(right_pos - 1);
                    return node;
                }
            }
            return {};
        }

        leaf_type merge_leaf_with_left(leaf_type& node) {
            auto left = get_accessor().load_leaf(find_left_sibling(node));
            if (left.is_valid()) {
                return merge_leaf_with_right(left);
            }
            return {};
        }

        leaf_type try_merge_leaf(leaf_type& node) {
            auto tmp = merge_leaf_with_right(node);
            if (tmp.is_valid()) {
                return tmp;
            }
            return merge_leaf_with_left(node);
        }

        inode_type merge_inode_with_right(inode_type& node) {
            auto& accessor = get_accessor();

            auto right = accessor.load_inode(find_right_sibling(node));
            if (right.is_valid()) {
                if ((right.size() + node.size() + 1) <= node.capacity()) {

                    auto parent = accessor.load_inode(node.get_parent());
                    auto right_pos = find_child_index_in_parent(parent, right.self());

                    auto borrow_separator = parent.borrow_key(right_pos - 1);
                    auto separator_key = model_.key_borrow_as_like(borrow_separator);
                    const auto last_node_child = node.get_child(node.size());

                    // move the latest child
                    node.insert_child(node.size(), separator_key, last_node_child);

                    for (std::size_t id = 0; id < right.size(); ++id) {

                        auto borrow_key = right.borrow_key(id);
                        auto id_like = model_.key_borrow_as_like(borrow_key);
                        const auto child = right.get_child(id);
                        visit_node([&node](auto& c) { c.set_parent(node.self()); }, child);

                        node.insert_child(node.size(), std::move(id_like), child);
                    }

                    // update the latest child
                    const auto last_child = right.get_child(right.size()); // last
                    visit_node([&node](auto& c) { c.set_parent(node.self()); }, last_child);
                    node.update_child(node.size(), last_child); 

                    swap_children(parent, right_pos - 1, right_pos);

                    accessor.destroy(parent.get_child(right_pos - 1));
                    parent.erase(right_pos - 1);
                    return node;
                }
            }
            return {};
        }

        inode_type merge_inode_with_left(inode_type& node) {
            auto left = get_accessor().load_inode(find_left_sibling(node));
            if (left.is_valid()) {
                return merge_inode_with_right(left);
            }
            return {};
        }

        inode_type try_merge_inode(inode_type& node) {
            auto tmp = merge_inode_with_right(node);
            if (tmp.is_valid()) {
                return tmp;
            }
            return merge_inode_with_left(node);
        }

        //endregion merging
#pragma endregion "merging"

        node_id_type get_leftmost_leaf(node_id_type id) const {

            if (model_.is_leaf_id(id)) {
                return id;
            }
            else {
                auto& accessor = get_accessor();
                auto next = accessor.load_inode(id);
                auto id0 = next.get_child(0);
                while (!model_.is_leaf_id(id0)) {
                    next = accessor.load_inode(id0);
                    id0 = next.get_child(0);
                }
                return id0;
            }
        }

        node_id_type get_rightmost_leaf(node_id_type id) const {

            if (model_.is_leaf_id(id)) {
                return id;
            }
            else {
                auto& accessor = get_accessor();
                auto next = accessor.load_inode(id);
                auto id0 = next.get_child(next.size());
                while (!model_.is_leaf_id(id0)) {
                    next = accessor.load_inode(id0);
                    id0 = next.get_child(next.size());
                }
                return id0;
            }
        }

        std::size_t find_child_index_in_parent(node_id_type parent, const node_id_type node) {
            return find_child_index_in_parent(get_accessor().load_inode(parent), node);
        }

        std::size_t find_child_index_in_parent(const inode_type& parent, const node_id_type node) {
            if (parent.is_valid()) {
                for (std::size_t id = 0; id < parent.size() + 1; ++id) {
                    if (parent.get_child(id) == node) {
                        return id;
                    }
                }
            }
            return npos;
        }

        template <typename NodeT>
        node_id_type find_left_sibling(NodeT& node) {
            auto parent = get_accessor().load_inode(node.get_parent());
            if (parent.is_valid()) {
                const std::size_t pos = find_child_index_in_parent(parent, node.self());
                if ((pos != npos) && (pos != 0)) {
                    return parent.get_child(pos - 1);
                }
            }
            return node_id_type{};
        }

        template <typename NodeT>
        node_id_type find_right_sibling(NodeT& node) {
            auto parent = get_accessor().load_inode(node.get_parent());
            if (parent.is_valid()) {
                const std::size_t pos = find_child_index_in_parent(parent, node.self());
                if ((pos != npos) && ((pos + 1) <= parent.size())) {
                    return parent.get_child(pos + 1);
                }
            }
            return node_id_type{};
        }

        template <typename NodeT>
        NodeT load(node_id_type id) {
            if constexpr (std::is_same_v<NodeT, inode_type>) {
                return get_accessor().load_inode(id);
            }
            else if constexpr (std::is_same_v<NodeT, leaf_type>) {
                return get_accessor().load_leaf(id);
            }
        }

        template <typename F>
        auto visit_node(F&& visitor, node_id_type node) {
            auto leaf = get_accessor().load_leaf(node);
            if (leaf.is_valid()) {
                return visitor(leaf);
            }
            else {
                auto inode = get_accessor().load_inode(node);
                return visitor(inode);
            }
        }

        bool is_full(const auto& node) const {
            return node.size() == node.capacity();
        }

        bool is_parent_leftmost(const auto& node, const inode_type& parent_node) const {
            return parent_node.get_child(0) == node.self();
        }

        bool is_parent_rightmost(const auto& node, const inode_type& parent_node) const {
            return parent_node.get_child(parent_node.children_count()) == node.self();
        }

        search_result find_node_with(const key_like_type &key) {
            auto [root, exists] = get_accessor().load_root();
            if (exists) {
                return find_node_with_(key, root);
            }
            return {};
        }

        search_result find_node_with_(const key_like_type &key, node_id_type current_id) {
            auto& accessor = get_accessor();
            while (1) {
                auto leaf = accessor.load_leaf(current_id);
                if (leaf.is_valid()) {
                    const auto pos = leaf.key_position(key);
                    const bool found = (pos != leaf.size()) && leaf.keys_eq(model_.key_out_as_like(leaf.get_key(pos)), key);
                    return { current_id, pos, found };
                }
                else {
                    auto inode = accessor.load_inode(current_id);
                    if (inode.is_valid()) {
                        auto pos = inode.key_position(key);
                        current_id = inode.get_child(pos);
                    }
                }
            }
        }

        void fix_parent_index(leaf_type& node) {
            auto parent = get_accessor().load_inode(node.get_parent());
            const auto pos = find_child_index_in_parent(parent, node.self());
            if (pos != npos && pos > 0) {
                parent.update_key(pos - 1, model_.key_out_as_like(node.get_key(0)));
            }
        }

        static void swap_children(inode_type& node, std::size_t a, std::size_t b) {
            const auto ca = node.get_child(a);
            const auto cb = node.get_child(b);
            node.update_child(a, cb);
            node.update_child(b, ca);
        }

        accessor_type& get_accessor() {
            return model_.get_accessor();
        }

        const accessor_type& get_accessor() const {
            return model_.get_accessor();
        }

        model_type model_;
    };

} // namespace fulla::bpt
