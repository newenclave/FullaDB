/*
 * File: radix_table/concepts.hpp
 * Author: newenclave
 * GitHub: https://github.com/newenclave
 * Created: 2025-12-16
 * License: MIT
 */

#pragma once

#include <concepts>
#include "fulla/core/concepts.hpp"

namespace fulla::radix_table::concepts {
	template <typename RLT>
	concept RadixLevel = requires(RLT rlt, typename RLT::index_type id, typename RLT::value_in_type value_in) {

		typename RLT::value_out_type;
		typename RLT::value_in_type;
		typename RLT::index_type;

		{ rlt.size() } -> std::convertible_to<std::size_t>;

		{ rlt.set_parent(rlt) } -> std::same_as<void>;
		{ rlt.get_parent() } -> std::convertible_to<RLT>;

		{ rlt.get_level() } -> std::convertible_to<typename RLT::index_type>;
		{ rlt.set_table(id, rlt) } -> std::same_as<void>;
		{ rlt.get_table(id) } -> std::convertible_to<RLT>;

		{ rlt.set_value(id, value_in) } -> std::same_as<void>;
		{ rlt.get_value(id) } -> std::convertible_to<typename RLT::value_out_type>;

		{ rlt.remove(id) } -> std::same_as<void>;

		{ rlt.holds_value(id) } -> std::convertible_to<bool>;
		{ rlt.holds_table(id) } -> std::convertible_to<bool>;

		{ rlt.is_valid() } -> std::convertible_to<bool>;
		{ rlt.is_same(rlt) } -> std::convertible_to<bool>;
	};

	template <typename AllocT>
	concept Allocator = requires(AllocT allocator, 
		typename AllocT::output_type val, 
		typename AllocT::index_type id) {
		
		typename AllocT::output_type;
		typename AllocT::index_type;

		{ allocator.create_level(id) } -> std::convertible_to<typename AllocT::output_type>;
		{ allocator.destroy(val) } -> std::same_as<void>;
	};

	template <typename MT>
	concept Model = requires (MT model, typename MT::allocator_type allocator) {

		typename MT::radix_level_type;
		typename MT::allocator_type;
		typename MT::root_accessor_type;

		requires(RadixLevel<typename MT::radix_level_type>);
		requires(Allocator<typename MT::allocator_type>);
		requires(core::concepts::RootManager<typename MT::root_accessor_type>);

		{ model.split_factor() } -> std::convertible_to<unsigned int>;
		{ model.get_allocator() } -> std::convertible_to<typename MT::allocator_type&>;
		{ model.get_root_accessor() } -> std::convertible_to<typename MT::root_accessor_type&>;
	};

}
