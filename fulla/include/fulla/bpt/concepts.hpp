/*
 * File: concepts.hpp
 * Author: newenclave
 * GitHub: https://github.com/newenclave
 * Created: 2025-11-01
 * License: MIT
 */

#pragma once
#include <concepts>

namespace fulla::bpt::concepts {

    template <typename NodeT, typename KeyOutT, typename KeyLikeT, typename KeyBorrowT>
    concept NodeKeys = requires (NodeT n, std::size_t i, const KeyLikeT &k) {
        typename NodeT::node_id_type;
        // Capacity
        { n.keys_maximum() } -> std::convertible_to<std::size_t>;
        { n.keys_count() } -> std::convertible_to<std::size_t>;

        // Search and comparison
        { n.key_position(k) } -> std::convertible_to<std::size_t>;
        { n.keys_eq(k, k) } -> std::convertible_to<bool>;

        // Access
        { n.get_key(i) } -> std::convertible_to<KeyOutT>;
        { n.borrow_key(i) } -> std::convertible_to<KeyBorrowT>;

        // Modification
        { n.insert_key(i, k) } -> std::convertible_to<bool>;
        { n.erase_key(i) } -> std::convertible_to<bool>;
        { n.update_key(i, k) } -> std::convertible_to<bool>;

        // Check
        { n.is_valid() } -> std::convertible_to<bool>;

        // Parent
        { n.set_parent(typename NodeT::node_id_type{}) };
        { n.get_parent() } -> std::convertible_to <typename NodeT::node_id_type>;

        { n.self() } -> std::convertible_to <typename NodeT::node_id_type>;
    };

    template <typename INodeT, typename KeyOutT, typename KeyLikeT, typename KeyBorrowT>
    concept INode = NodeKeys<INodeT, KeyOutT, KeyLikeT, KeyBorrowT> 
        && requires(INodeT n, std::size_t pos) {

        { n.children_count() } -> std::convertible_to<std::size_t>;
        { n.get_child(pos) } -> std::convertible_to<typename INodeT::node_id_type>;

        { n.insert_child(pos, typename INodeT::node_id_type{}) } -> std::convertible_to<bool>;
        { n.erase_child(pos) } -> std::convertible_to<bool>;
        { n.update_child(pos, typename INodeT::node_id_type{}) } -> std::convertible_to<bool>;

    };

    template <typename LeafNodeT, typename KeyOutT, typename KeyLikeT, typename KeyBorrowT, 
                typename ValueOutT, typename ValueInT, typename ValueBorrowT>
    concept LeafNode = NodeKeys<LeafNodeT, KeyOutT, KeyLikeT, KeyBorrowT> 
        && requires(LeafNodeT n, std::size_t pos, ValueInT val) {

        { n.values_count() } -> std::convertible_to<std::size_t>;
        { n.get_value(pos) } -> std::convertible_to<ValueOutT>;
        { n.borrow_value(pos) } -> std::convertible_to<ValueBorrowT>;

        { n.insert_value(pos, val) } -> std::convertible_to<bool>;
        { n.erase_value(pos) } -> std::convertible_to<bool>;
        { n.update_value(pos, val) } -> std::convertible_to<bool>;

        // next/prev leafs
        { n.set_next(typename LeafNodeT::node_id_type{}) };
        { n.set_prev(typename LeafNodeT::node_id_type{}) };
        { n.get_next() } -> std::convertible_to<typename LeafNodeT::node_id_type>;
        { n.get_prev() } -> std::convertible_to<typename LeafNodeT::node_id_type>;
    };

    template <typename AccessT, typename NodeId, typename INodeT, typename LeafT>
    concept NodeAccessor = requires(AccessT a, NodeId id) {
        // Create:
        { a.create_leaf() }  -> std::convertible_to<LeafT>;
        { a.create_inode() } -> std::convertible_to<INodeT>;

        // destroy
        { a.destroy(id) } -> std::convertible_to<bool>;

        // load:
        { a.load_leaf(id) }  -> std::convertible_to<LeafT>;
        { a.load_inode(id) } -> std::convertible_to<INodeT>;

        // root:
        { a.load_root() }  -> std::convertible_to<std::tuple<NodeId, bool>>; // bool: exists?
        { a.set_root(id) } -> std::same_as<void>;
    };

    template<typename ModelT>
    concept BptModel = requires (ModelT m) {

        typename ModelT::key_type;
        typename ModelT::key_like_type;
        typename ModelT::key_out_type;
        typename ModelT::key_borrow_type;
        typename ModelT::less_type;

        typename ModelT::value_in_type;
        typename ModelT::value_out_type;
        typename ModelT::value_borrow_type;

        typename ModelT::leaf_type;
        typename ModelT::inode_type;
        typename ModelT::accessor_type;

        typename ModelT::node_id_type;

        requires std::same_as<typename ModelT::node_id_type, typename ModelT::leaf_type::node_id_type>;
        requires std::same_as<typename ModelT::node_id_type, typename ModelT::inode_type::node_id_type>;
        requires std::equality_comparable<typename ModelT::node_id_type>;

        { m.is_valid_id(typename ModelT::node_id_type{}) } -> std::convertible_to<bool>;
        { m.is_leaf_id(typename ModelT::node_id_type{}) } -> std::convertible_to<bool>;
        { m.get_accessor() } -> std::convertible_to<typename ModelT::accessor_type &>;

        requires requires(typename ModelT::key_out_type kout, typename ModelT::value_out_type vout, 
                           typename ModelT::key_borrow_type kbor, typename ModelT::value_borrow_type vbor) {

            { m.key_out_as_like(kout) } -> std::convertible_to<typename ModelT::key_like_type>;
            { m.key_borrow_as_like(kbor) } -> std::convertible_to<typename ModelT::key_like_type>;

            { m.value_out_as_in(vout) } -> std::convertible_to<typename ModelT::value_in_type>;
            { m.value_borrow_as_in(vbor) } -> std::convertible_to<typename ModelT::value_in_type>;
        };

        requires concepts::NodeAccessor<
            typename ModelT::accessor_type,
            typename ModelT::node_id_type,
            typename ModelT::inode_type,
            typename ModelT::leaf_type
        >;

        requires concepts::LeafNode<
            typename ModelT::leaf_type,
            typename ModelT::key_out_type,
            typename ModelT::key_like_type,
            typename ModelT::key_borrow_type,
            typename ModelT::value_out_type,
            typename ModelT::value_in_type,
            typename ModelT::value_borrow_type
        >;

        requires concepts::INode<
            typename ModelT::inode_type,
            typename ModelT::key_out_type,
            typename ModelT::key_like_type,
            typename ModelT::key_borrow_type
        >;
    };
}