/*
 * File: radix_table/trie.hpp
 * Author: newenclave
 * GitHub: https://github.com/newenclave
 * Created: 2025-12-16
 * License: MIT
 */

#pragma once

#include <concepts>
#include <cstdint>

#include "fulla/core/concepts.hpp"
#include "fulla/slots/directory.hpp"
#include "fulla/page/radix_level.hpp"
#include "fulla/page_allocator/concepts.hpp"
#include "fulla/radix_table/concepts.hpp"

namespace fulla::radix_table {

	template <std::unsigned_integral KeyT, concepts::Model ModelT>
	struct trie {

		using key_type = KeyT;
		using model_type = ModelT;

		using allocator_type = typename model_type::allocator_type;
		using root_accessor_type = typename model_type::root_accessor_type;
		using radix_level_type = typename model_type::radix_level_type;

		using value_in_type = typename radix_level_type::value_in_type;
		using value_out_type = typename radix_level_type::value_out_type;
		using index_type = typename radix_level_type::index_type;

		using index_span = std::span<index_type>;
		using stack_buffer = std::array<index_type, sizeof(key_type) * 2>;

		template<typename ...Args>
		trie(Args&&...args)
			: model_(std::forward<Args>(args)...)
		{}

		value_out_type get(key_type key) {
			auto [lvl, id] = find_level_for(key);
			if (lvl.is_valid() && lvl.holds_value(id)) {
				return lvl.get_value(id);
			}
			return {};
		}

		bool set(key_type key, value_in_type value) {
			stack_buffer output;
			auto split = split_key(key, { output });

			if (split.size() == 0) {
				auto zero = create_top_level(0);
				zero.set_value(0, std::move(value));
				return true;
			}
			else {
				auto& raccess = get_root_accessor();
				const auto need_level = static_cast<index_type>(split.size() - 1);
				auto current = raccess.get_root();

				if (!current.is_valid()) {
					current = create_top_level(need_level);
				}

				if (need_level > current.get_level()) {

					const auto level_diff = need_level - current.get_level();
					const auto s0 = split.subspan(0, level_diff);
					const auto s1 = split.subspan(level_diff);

					auto root = raccess.get_root();

					current = set_create_path(root, s0);
					set_value_into(current, s1, std::move(value));
				}
				else {
					current = get_create_level(need_level);
					set_value_into(current, split, std::move(value));
				}
			}
			return false;
		}

		bool has(key_type key) {
			auto [lvl, id] = find_level_for(key);
			if (lvl.is_valid()) {
				return lvl.holds_value(id);
			}
			return false;
		}

		bool remove(key_type key) {
			auto [lvl, id] = find_level_for(key);
			if (lvl.is_valid() && lvl.holds_value(id)) {
				lvl.remove(id);
				remove_up(lvl);
				return true;
			}
			return false;
		}

		allocator_type& get_allocator() {
			return model_.get_allocator();
		}

		root_accessor_type& get_root_accessor() {
			return model_.get_root_accessor();
		}

	private:

		void remove_up(radix_level_type lvl) {
			auto& allocator = get_allocator();
			auto root = get_root_accessor().get_root();
			while (lvl.is_valid() && (lvl.size() == 0)) {
				auto [parent, parent_id] = lvl.get_parent();
				allocator.destroy(lvl);
				lvl = parent;
				if(!lvl.is_valid()) {
					get_root_accessor().set_root(lvl);
				}
				else {
					lvl.remove(parent_id);
				}
			}
		}

		std::tuple<radix_level_type, index_type> find_level_for(key_type key) {
			if (get_root_accessor().has_root()) {
				stack_buffer output;
				auto split = split_key(key, { output });
				if (split.empty()) {
					return { get_level(0), index_type{0} };
				}
				auto level = split.size() - 1;
				auto current = get_level(level);
				while (current.is_valid() && !split.empty()) {
					if (split.size() == 1) {
						return { current, split[0] };
					}
					else {
						current = current.get_table(split[0]);
					}
					split = split.subspan(1);
				}
			}
			return { {}, index_type{0} };
		}

		radix_level_type get_create_table(radix_level_type level, index_type id) {
			if (!level.holds_table(id)) {
				auto& allocator = get_allocator();
				auto new_level = allocator.create_level(level.get_level() - 1);
				level.set_table(id, std::move(new_level));
			}
			return level.get_table(id);
		}

		radix_level_type set_create_path(radix_level_type from, index_span path) {
			auto& allocator = get_allocator();
			auto& raccess = get_root_accessor();

			radix_level_type result{};
			while (!path.empty()) {

				auto new_root = allocator.create_level(from.get_level() + 1);
				auto root = raccess.get_root();
				new_root.set_table(0, std::move(root));
				raccess.set_root(std::move(new_root));
				root = raccess.get_root();
				if (!result.is_valid()) {
					result = get_create_table(root, path.back());
				}
				from = root;
				path = path.subspan(0, path.size() - 1);
			}
			return result;
		}

		void set_value_into(radix_level_type from, index_span path, value_in_type value) {
			while (!path.empty()) {
				if (path.size() == 1) {
					from.set_value(path[0], std::move(value));
					return;
				}
				else {
					from = get_create_table(from, path[0]);
					path = path.subspan(1);
				}
			}
		}

		radix_level_type get_create_level(std::size_t lvl) {
			auto& allocator = get_root_accessor();
			check_create_root();
			auto current = allocator.get_root();
			while (current.is_valid() && (current.get_level() > lvl)) {
				current = get_create_table(current, 0);
			}
			return current;
		}

		radix_level_type create_top_level(index_type lvl) {
			auto& allocator = get_allocator();
			auto& raccess = get_root_accessor();

			auto current = raccess.get_root();
			if (!current.is_valid()) {
				raccess.set_root(allocator.create_level(lvl));
				return raccess.get_root();
			}

			while (current.get_level() < lvl) {
				auto new_root = allocator.create_level(current.get_level() + 1);
				auto old_root = raccess.get_root();
				new_root.set_table(0, old_root);
				raccess.set_root(std::move(new_root));
				current = raccess.get_root();
			}
			return current;
		}

		radix_level_type get_level(std::size_t lvl) {
			if (!get_root_accessor().has_root()) {
				return {};
			}
			auto current = get_root_accessor().get_root();
			if (current.get_level() < lvl) {
				return {};
			}
			while (current.is_valid() && (current.get_level() > lvl)) {
				current = current.get_table(0);
			}
			return current;
		}

		index_span split_key(key_type k, index_span output) const noexcept {
			if (k == 0) {
				return {};
			}
			std::size_t span_pos = output.size() - 1;
			while (k > 0) {
				output[span_pos] = static_cast<index_type>(k % model_.split_factor());
				k /= static_cast<key_type>(model_.split_factor());
				if (k > 0) {
					span_pos--;
				}
			}
			return output.subspan(span_pos);
		}

		void check_create_root() {
			auto& allocator = get_allocator();
			auto& raccess = get_root_accessor();
			if (!raccess.has_root()) {
				auto root = allocator.create_level(0);
				raccess.set_root(std::move(root));
			}
		}

		model_type model_;
	};
}
