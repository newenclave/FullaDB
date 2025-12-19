/*
 * File: radix_table/memory/model.hpp
 * Author: newenclave
 * GitHub: https://github.com/newenclave
 * Created: 2025-12-19
 * License: MIT
 */

#pragma once

#include <concepts>
#include <memory>
#include <optional>

#include "fulla/core/types.hpp"
#include "fulla/core/debug.hpp"
#include "fulla/core/concepts.hpp"
#include "fulla/radix_table/concepts.hpp"

namespace fulla::radix_table::memory {

    template <typename ValueT, std::size_t SplitFactor>
	struct container {
		using sptr_type = std::shared_ptr<container<ValueT, SplitFactor>>;
		using value_type = ValueT;
		struct none_type {};
		using data_variant = std::variant<none_type, value_type, sptr_type>;
		using slot_array = std::array<data_variant, SplitFactor>;

		container(std::size_t lvl): level(lvl) {}
		container() = default;

		bool is_value(std::size_t id) const noexcept {
			return std::holds_alternative<value_type>(data[id]);
		}

		bool is_ptr(std::size_t id) const noexcept {
			return std::holds_alternative<sptr_type>(data[id]);
		}

		value_type as_value(std::size_t id) {
			return std::get<value_type>(data[id]);
		}

		sptr_type as_ptr(std::size_t id) {
			return std::get<sptr_type>(data[id]);
		}

		std::size_t level = 0;
		slot_array data;
	};

	template <typename ValueT, std::size_t SplitFactor>
	struct radix_level {

		using value_in_type = ValueT;
		using value_out_type = ValueT;
		using index_type = std::size_t;
		using chunk_type = container<ValueT, SplitFactor>;

		radix_level() = default;
		radix_level(typename chunk_type::sptr_type d) : data(d) {}

		void check_valid() const {
			if (!is_valid()) {
				throw std::runtime_error("Bad call");
			}
		}

		index_type get_level() const noexcept {
			check_valid();
			return data->level;
		}

		void set_table(index_type id, radix_level rd) {
			check_valid();
			data->data[id] = { rd.data };
		}

		radix_level get_table(index_type id) {
			check_valid();
			return radix_level(data->as_ptr(id));
		}

		void set_value(index_type id, value_in_type value) {
			check_valid();
			data->data[id] = { std::move(value) };
		}

		value_out_type get_value(index_type id) {
			check_valid();
			return data->as_value(id);
		}

		void remove(index_type id) {
			check_valid();
			data->data[id] = { typename chunk_type::none_type{} };
		}
		
		bool holds_value(index_type id) const noexcept {
			check_valid();
			return data->is_value(id);
		}

		bool holds_table(index_type id) const noexcept {
			check_valid();
			return data->is_ptr(id);
		}

		bool is_valid() const noexcept {
			return data.operator bool();
		}

		using data_type = std::shared_ptr<chunk_type>;
		data_type data;
	};

	static_assert(concepts::RadixLevel<radix_level<int, 256>>, "");

	template <typename ValueT, std::size_t SplitFactor>
	struct allocator {
		using output_type = radix_level<ValueT, SplitFactor>;
		using index_type = std::size_t;
		using data_type = container<ValueT, SplitFactor>;

		output_type create_level(index_type lvl) {
			return { std::make_shared<data_type>(lvl) };
		};

		void destroy(output_type val) {
			
		}
	};

	template <typename ValueT, std::size_t SplitFactor>
	struct root_accessor {
		using value_type = ValueT;
		using root_type = radix_level<value_type, SplitFactor>;

		root_type get_root() {
			if (root.has_value()) {
				return *root;
			}
			return {};
		}

		void set_root(root_type val) {
			root = { val };
		}

		bool has_root() const noexcept {
			return root.has_value();
		}

		std::optional<root_type> root;
	};

	static_assert(concepts::Allocator<allocator<int, 256>>, "");
	static_assert(core::concepts::RootManager<root_accessor<int, 256>, radix_level<int, 256>>, "");

	template <typename ValueT, std::size_t SplitFactor = 0x100>
	struct model {
		using radix_level_type = radix_level<ValueT, SplitFactor>;
		using allocator_type = allocator<ValueT, SplitFactor>;
		using root_accessor_type = root_accessor<ValueT, SplitFactor>;

		constexpr static std::uint32_t split_factor() {
			return SplitFactor;
		}

		allocator_type& get_allocator() {
			return allocator_;
		}

		root_accessor_type& get_root_accessor() {
			return root_;
		}

		allocator_type allocator_;
		root_accessor_type root_;
	};
}

