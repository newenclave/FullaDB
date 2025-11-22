/*
 * File: containter.hpp
 * Author: newenclave
 * GitHub: https://github.com/newenclave
 * Created: 2025-11-01
 * License: MIT
 */

#pragma once
#include "fulla/core/static_vector.hpp"

namespace fulla::bpt::memory::container {

    template <typename NodeIdT, typename KeyT, std::size_t MaxElements>
    struct base {

        using node_id_type = NodeIdT;

        using key_type = KeyT;
        constexpr static const std::size_t max_elements = MaxElements;

        virtual ~base() = default;
        virtual bool is_leaf() const = 0;
        
        template <std::derived_from<base> T>
        T* as() {
            return static_cast<T*>(this);
        }

        std::size_t cap_ = MaxElements;
        node_id_type parent_ = {};
        core::static_vector<key_type, max_elements> keys_;        
    };

    template <typename NodeIdT, typename KeyT, std::size_t MaxElements>
    struct inode : public base<NodeIdT, KeyT, MaxElements> {
        using typename base<NodeIdT, KeyT, MaxElements>::node_id_type;
        using typename base<NodeIdT, KeyT, MaxElements>::key_type;
        constexpr static const std::size_t max_elements = MaxElements;
        bool is_leaf() const override {
            return false;
        }
        node_id_type last_child_ = {};
        core::static_vector<node_id_type, max_elements> children_;
    };

    template <typename NodeIdT, typename KeyT, typename valueT, std::size_t MaxElements>
    struct leaf : public base<NodeIdT, KeyT, MaxElements> {
        using base_type = base<NodeIdT, KeyT, MaxElements>;
        using typename base_type::node_id_type;
        using typename base_type::key_type;
        using value_type = valueT;
        constexpr static const std::size_t max_elements = MaxElements;
        bool is_leaf() const override {
            return true;
        }
        core::static_vector<value_type, max_elements> values_;
        node_id_type prev_ = {};
        node_id_type next_ = {};
    };

} // namespace fulla::bpt::memory::container
