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

	//template <typename RLT>
	//concept RadixLevel = requires(RLT rlt, typename RLT::index_type id, typename RLT::value_in_type value_in) {

	//	typename RLT::value_out_type;
	//	typename RLT::value_in_type;
	//	typename RLT::index_type;

	//	{ rlt.holds_value(id) } -> std::convertible_to<bool>;
	//	{ rlt.holds_table(id) } -> std::convertible_to<bool>;

	//	{ rlt.is_valid() } -> std::convertible_to<bool>;
	//};

	struct default_radix_level_descriptor {
		constexpr static std::uint16_t page_kind_value = 0x30;
		using page_metadata_type = page::empty_metadata;
	};

	template <page_allocator::concepts::PageAllocator PaT, RadixLevelDescriptor RlDT>
	class radix_level {
	public:

		using allocator_type = PaT;
		using pid_type = typename allocator_type::pid_type;
		using page_handle = typename allocator_type::page_handle;
		using values_span = std::span<page::radix_value>;
		using cvalues_span = std::span<const page::radix_value>;

		using page_metadata_type = typename RlDT::page_metadata_type;
		constexpr static const auto page_kind_value = RlDT::page_kind_value;

		using value_in_type = pid_type;
		using value_out_type = pid_type;
		using index_type = std::uint16_t;

		radix_level() = default;
		radix_level(allocator_type& allocator, page_handle page)
			: allocator_(&allocator)
			, page_(std::move(page))
		{
		}

		void reset_values() {		
			auto values = get_values();
			for (auto& v : values) {
				v.init();
			}
			page_.mark_dirty();
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

		void set_table(index_type id, const radix_level& rl) {
			auto values = get_values();
			DB_ASSERT(id < values.size(), "Bad value");
			DB_ASSERT(get_level() > 0, "Bad level");
			values[id].value = rl.pid();
		}

		void set_value(index_type id, const value_in_type val) {
			auto values = get_values();
			DB_ASSERT(id < values.size(), "Bad value");
			DB_ASSERT(get_level() == 0, "Bad level");
			values[id].value = val;
		}

		void remove(radix_level &rl) {
			allocator_->destroy(rl.pid());
			rl = {};
		}

		bool is_valid() const noexcept {
			return (allocator_ != nullptr) && page_.is_valid();
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
			const auto res = values[id].value.get();
			return res != allocator_type::invalid_pid;
		}

		bool holds_table(index_type id) const {
			if (get_level() == 0) {
				return false;
			}
			auto values = get_values();
			DB_ASSERT(id < values.size(), "Bad value");
			return allocator_->valid_id(values[id].value.get());
		}

		values_span get_values() {
			page_view_type pv{ page_.rw_span() };
			const auto max_cap = pv.capacity(); // page_view_type::capacity_max<page::radix_level_header, page_metadata_type>(allocator_->page_size());
			const auto max_values = max_cap / sizeof(page::radix_value);
			DB_ASSERT(max_cap >= 256, "Page is too short");
			auto page_span = pv.rw_span();
			return { reinterpret_cast<page::radix_value*>(page_span.data()), max_values };
		}

		cvalues_span get_values() const {
			cpage_view_type pv{ page_.ro_span() };
			const auto max_cap = pv.capacity(); //page_view_type::capacity_max<page::radix_level_header, page_metadata_type>(allocator_->page_size());
			const auto max_values = max_cap / sizeof(page::radix_value);
			DB_ASSERT(max_cap >= 256, "Page is too short");
			auto page_span = pv.ro_span();
			return { reinterpret_cast<const page::radix_value*>(page_span.data()), max_values };
		}

	private:
		mutable std::optional<std::uint16_t> level_cache_ = {};
		allocator_type* allocator_ = nullptr;
		page_handle page_{};
	};

	//template <typename AllocT>
	//concept Allocator = requires(AllocT allocator,
	//	typename AllocT::output_type val,
	//	typename AllocT::index_type id) {

	//	typename AllocT::output_type;
	//	typename AllocT::index_type;

	//	{ allocator.create_level(id) } -> std::convertible_to<typename AllocT::output_type>;
	//	{ allocator.destroy(val) } -> std::same_as<void>;
	//};

	template <page_allocator::concepts::PageAllocator PaT, RadixLevelDescriptor RlDT = default_radix_level_descriptor>
	class allocator {
	public:
		using allocator_type = PaT;
		using pid_type = typename PaT::pid_type;
		using output_type = radix_level<allocator_type, RlDT>;
		using index_type = std::uint16_t;

		using page_metadata_type = typename RlDT::page_metadata_type;
		constexpr static const auto page_kind_value = RlDT::page_kind_value;

		allocator() = default;
		allocator(allocator_type& al) 
			: allocator_(&al) 
		{}

		output_type create_level(index_type lvl) {
			auto new_page = allocator_->allocate();
			if (new_page.is_valid()) {
				page_view_type pv{ new_page.rw_span() };
				pv.header().init(page_kind_value, allocator_->page_size(), new_page.pid(), 
					sizeof(page::radix_level_header), 
					page::metadata_size<page_metadata_type>());
				auto sh = pv.subheader<page::radix_level_header>();
				sh->init(0, lvl);
				if constexpr (core::concepts::HasInit<page::radix_level_header>) {
					pv.metadata_as<page::radix_level_header>()->init();
				}
				output_type res(*allocator_, new_page);
				res.reset_values();
				return res;
			}
			return {};
		}

	private:

		allocator_type* allocator_ = nullptr;
	};
}
