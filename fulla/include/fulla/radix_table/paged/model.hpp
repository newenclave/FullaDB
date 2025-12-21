/*
 * File: radix_table/paged/model.hpp
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
#include "fulla/radix_table/concepts.hpp"
#include "fulla/page/radix_level.hpp"
#include "fulla/page/page_view.hpp"
#include "fulla/page_allocator/concepts.hpp"
#include "fulla/page/slot_directory.hpp"

namespace fulla::radix_table::paged {

	using page_view_type = page::page_view<page::slots::empty_directory_view>;
	using cpage_view_type = page::const_page_view<page::slots::empty_directory_view>;


	template <typename T>
	concept RadixLevelKinds = requires (T s) {
		{ T::page_kind_value } -> std::convertible_to<std::uint16_t>;
	};

	template <typename T>
	concept RadixLevelMetadata = requires {
		typename T::page_metadata_type;
	};

	template <typename T>
	concept RadixLevelDescriptor = RadixLevelKinds<T> && RadixLevelMetadata<T>;

	struct default_radix_level_descriptor {
		constexpr static std::uint16_t page_kind_value = 0x30;
		using page_metadata_type = page::empty_metadata;
	};

	enum class value_enum_type : std::uint8_t {
		none = 0,
		level = 1,
		value = 2,
	};

	template <page_allocator::concepts::PageAllocator PaT>
	class radix_level {
	public:

		using allocator_type = PaT;
		using pid_type = typename allocator_type::pid_type;
		using page_handle = typename allocator_type::page_handle;
		using values_span = std::span<page::radix_value>;
		using cvalues_span = std::span<const page::radix_value>;

		using value_in_type = pid_type;
		using value_out_type = pid_type;
		using index_type = std::uint16_t;

		radix_level() = default;
		radix_level(allocator_type& allocator, page_handle page)
			: allocator_(&allocator)
			, page_(std::move(page))
		{
		}

		std::size_t size() const {
			// naiv impl. need to be fixed
			auto values = get_values();
			std::size_t res = 0;
			for (auto& v : values) {
				res += (v.type != static_cast<core::byte>(value_enum_type::none));
			}
			return res;
		}

		void set_parent(radix_level &rlt, index_type id) {
			page_view_type pv{ page_.rw_span() };
			pv.subheader<page::radix_level_header>()->parent = rlt.page_.pid();
			pv.subheader<page::radix_level_header>()->parent_id = id;
			mark_dirty();
		}

		std::tuple<radix_level, index_type> get_parent() const {
			cpage_view_type pv{ page_.ro_span() };
			auto parent = allocator_->fetch(pv.subheader<page::radix_level_header>()->parent.get());
			const auto parent_id = pv.subheader<page::radix_level_header>()->parent_id.get();
			return { { *allocator_, std::move(parent) }, parent_id };
		}

		void reset_values() {
			auto values = get_values();
			for (auto& v : values) {
				v.init();
			}
			mark_dirty();
		}

		index_type get_level() const {
			if (!level_cache_.has_value()) {
				cpage_view_type pv{ page_.ro_span() };
				level_cache_ = { pv.subheader<page::radix_level_header>()->level.get() };
			}
			return *level_cache_;
		}

		auto get_table(index_type id) {
			auto values = get_values();

			DB_ASSERT(id < values.size(), "Bad value");
			DB_ASSERT(get_level() > 0, "Bad level");

			auto pid = values[id].value.get();
			auto load_page = allocator_->fetch(pid);
			return radix_level{ *allocator_, load_page };
		}

		value_out_type get_value(index_type id) {
			auto values = get_values();

			DB_ASSERT(id < values.size(), "Bad value");
			DB_ASSERT(get_level() == 0, "Bad level");

			return values[id].value.get();
		}

		void set_table(index_type id, radix_level rl) {
			auto values = get_values();
			DB_ASSERT(id < values.size(), "Bad value");
			DB_ASSERT(get_level() > 0, "Bad level");
			values[id].value = rl.pid();
			values[id].type = static_cast<core::byte>(value_enum_type::level);
			mark_dirty();
			rl.set_parent(*this, id);
		}

		void set_value(index_type id, const value_in_type val) {
			auto values = get_values();
			DB_ASSERT(id < values.size(), "Bad value");
			DB_ASSERT(get_level() == 0, "Bad level");
			values[id].value = val;
			values[id].type = static_cast<core::byte>(value_enum_type::value);
			mark_dirty();
		}

		void remove(index_type id) {
			auto values = get_values();
			values[id].type = static_cast<core::byte>(value_enum_type::none);
			mark_dirty();
		}

		bool is_valid() const noexcept {
			return (allocator_ != nullptr) && page_.is_valid();
		}

		bool is_same(const radix_level& rd) const noexcept {
			return (allocator_ == rd.allocator_) 
				&& (page_.pid() == rd.page_.pid());
		}

		pid_type pid() const noexcept {
			return page_.pid();
		}

		bool holds_value(index_type id) const {
			if (get_level() > 0) {
				return false;
			}
			auto values = get_values();
			DB_ASSERT(id < values.size(), "Bad value");
			return values[id].type == static_cast<core::byte>(value_enum_type::value);
		}

		bool holds_table(index_type id) const {
			if (get_level() == 0) {
				return false;
			}
			auto values = get_values();
			DB_ASSERT(id < values.size(), "Bad value");
			return values[id].type == static_cast<core::byte>(value_enum_type::level);
		}

		values_span get_values() {
			page_view_type pv{ page_.rw_span() };
			const auto max_cap = pv.capacity();
			const auto max_values = max_cap / sizeof(page::radix_value);
			DB_ASSERT(max_cap >= 256, "Page is too short");
			auto page_span = pv.rw_span();
			return { reinterpret_cast<page::radix_value*>(page_span.data()), max_values };
		}

		cvalues_span get_values() const {
			cpage_view_type pv{ page_.ro_span() };
			const auto max_cap = pv.capacity();
			const auto max_values = max_cap / sizeof(page::radix_value);
			DB_ASSERT(max_cap >= 256, "Page is too short");
			auto page_span = pv.ro_span();
			return { reinterpret_cast<const page::radix_value*>(page_span.data()), max_values };
		}

	private:

		void mark_dirty() {
			page_.mark_dirty();
		}

		mutable std::optional<std::uint16_t> level_cache_ = {};
		allocator_type* allocator_ = nullptr;
		page_handle page_{};
	};

	template <page_allocator::concepts::PageAllocator PaT, RadixLevelDescriptor RlDT = default_radix_level_descriptor>
	class allocator {
	public:
		using allocator_type = PaT;
		using pid_type = typename PaT::pid_type;
		using output_type = radix_level<allocator_type>;
		using index_type = std::uint16_t;

		using page_metadata_type = typename RlDT::page_metadata_type;
		constexpr static const auto page_kind_value = RlDT::page_kind_value;

		allocator() = default;
		allocator(allocator_type& al)
			: allocator_(&al)
		{
		}

		output_type create_level(index_type lvl) {
			auto new_page = allocator_->allocate();
			if (new_page.is_valid()) {
				page_view_type pv{ new_page.rw_span() };
				pv.header().init(page_kind_value, allocator_->page_size(), new_page.pid(),
					sizeof(page::radix_level_header),
					page::metadata_size<page_metadata_type>());
				auto sh = pv.subheader<page::radix_level_header>();

				sh->init(allocator_type::invalid_pid, lvl);
				if constexpr (core::concepts::HasInit<page::radix_level_header>) {
					pv.metadata_as<page::radix_level_header>()->init();
				}
				
				output_type res(*allocator_, new_page);
				res.reset_values();
				return res;
			}
			return {};
		}

		void destroy(output_type& value) {
			allocator_->destroy(value.pid());
		}

	private:

		allocator_type* allocator_ = nullptr;
	};

	template <page_allocator::concepts::PageAllocator PaT>
	struct root_accessor {
		using root_type = radix_level<PaT>;

		root_type get_root() {
			if (root.has_value()) {
				return *root;
			}
			return {};
		}

		void set_root(root_type val) {
			root = val.is_valid() ? std::optional{val} : std::nullopt;
		}
		bool has_root() const noexcept {
			return root.has_value() && root->is_valid();
		}
		std::optional<root_type> root;
	};

	template <page_allocator::concepts::PageAllocator PaT,
		core::concepts::RootManager RootManagerT = root_accessor<PaT>,
		RadixLevelDescriptor RlDT = default_radix_level_descriptor
	>
	class model {
	public:
		using page_allocator_type = PaT;
		using radix_level_type = radix_level<PaT>;
		using allocator_type = allocator<PaT, RlDT>;
		using root_accessor_type = RootManagerT;

		static_assert(concepts::RadixLevel<radix_level_type>);
		static_assert(concepts::Allocator<allocator_type>);
		static_assert(core::concepts::RootManager<root_accessor_type>);

		model(page_allocator_type& allocator, root_accessor_type ra = {})
			: allocator_(allocator)
			, root_(std::move(ra))
			, page_size(allocator.page_size())
		{}
		
		std::uint32_t split_factor() const {
			return 256;
		}
		
		allocator_type& get_allocator() {
			return allocator_;
		}
		
		root_accessor_type& get_root_accessor() {
			return root_;
		}
	private:
		allocator_type allocator_{};
		root_accessor_type root_{};
		std::size_t page_size = 0;
	};

}
