/*
 * File: slab_store/store.hpp
 * Author: newenclave
 * GitHub: https://github.com/newenclave
 * Created: 2025-12-23
 * License: MIT
 */

#pragma once

#include <cstdint>

#include "fulla/core/concepts.hpp"
#include "fulla/page/slab_store.hpp"

#include "fulla/slots/stable_directory.hpp"
#include "fulla/page_allocator/concepts.hpp"
#include "fulla/page/metadata.hpp"
#include "fulla/page/page_view.hpp"

namespace fulla::slab_store {

	using fulla::core::word_u16;
	using fulla::core::byte_span;
	using fulla::core::byte_view;

	template <typename T>
	concept SlabStoreKinds = requires (T s) {
		{ T::page_kind_value } -> std::convertible_to<std::uint16_t>;
	};

	template <typename T>
	concept SlabStoreMetadata = requires {
		typename T::page_metadata_type;
	};

	template <typename T>
	concept SlabStoreDescriptor = SlabStoreKinds<T> && SlabStoreMetadata<T>;

	struct default_slab_store_descriptor {
		constexpr static std::uint16_t page_kind_value = 0x40;
		using page_metadata_type = page::empty_metadata;
	};

	template <page_allocator::concepts::PageAllocator DevT>
	struct default_root_manager {

		using allocator_type = DevT;
		using root_type = typename allocator_type::pid_type;

		constexpr default_root_manager() = default;

		bool has_root() const noexcept {
			return root.has_value() && (allocator_type::invalid_pid != *root);
		}

		root_type get_root() {
			if (has_root()) {
				return *root;
			}
			return allocator_type::invalid_pid;
		}
		
		void set_root(root_type val) {
			root = { val };
		}
		std::optional<root_type> root;
	};

	template <page_allocator::concepts::PageAllocator DevT, std::uint16_t SlotSize,
		fulla::core::concepts::RootManager RootMgrT = default_root_manager<DevT>,
		SlabStoreDescriptor SlabDescT = default_slab_store_descriptor,
		typename PidT = std::uint32_t>
	class store {
		struct header_handle;
	public:

		using allocator_type = DevT;
		using root_manager_type = RootMgrT;
		using under_page_handle = typename allocator_type::page_handle;

		struct pid_type {
			using under_pid_type = typename allocator_type::pid_type;
			under_pid_type pid = allocator_type::invalid_pid;
			std::uint16_t slot = word_u16::max();
			auto operator <=> (const pid_type&) const noexcept = default;
		};

		constexpr static const pid_type invalid_pid = {};

		struct page_handle {

			using pid_type = pid_type;

			page_handle(under_page_handle ph, std::uint16_t s) 
				: handle(std::move(ph))
				, slot_id(s)
			{};

			page_handle() = default;
			page_handle(page_handle&&) = default;
			page_handle(const page_handle&) = default;
			page_handle& operator = (page_handle&&) = default;
			page_handle& operator = (const page_handle&) = default;

			bool is_valid() const noexcept {
				if (handle.is_valid()) {
					header_handle hh{ handle };
					auto slots = hh.get_slots();
					return slots.test(slot_id);
				}
				return false;
			}

			pid_type pid() const noexcept {
				return { .pid = handle.pid(), .slot = slot_id };
			}

			std::uint16_t slot() const noexcept {
				return slot_id;
			}

			void mark_dirty() {
				handle.mark_dirty();
			}

			core::byte_span rw_span() {
				if (is_valid()) {
					header_handle hh{ handle };
					return hh.get_slots().get(slot_id);
				}
				return {};
			}
			
			core::byte_view ro_span() const {
				if (is_valid()) {
					header_handle hh{ handle };
					return hh.get_slots().get(slot_id);
				}
				return {};
			}

			under_page_handle underlying_handle() const {
				return handle;
			}

		private:
			under_page_handle handle{};
			std::uint16_t slot_id = word_u16::max();
		};

		static_assert(page_allocator::concepts::PageHandle<page_handle>);

		using slot_directory_type = slots::stable_directory_view<byte_span>;
		using cslot_directory_type = slots::stable_directory_view<byte_view>;
		using page_view_type = page::page_view<slot_directory_type>;
		using cpage_view_type = page::const_page_view<cslot_directory_type>;
		using underlying_device_type = allocator_type;

		using page_metadata_type = typename SlabDescT::page_metadata_type;
		constexpr static const auto page_kind_value = SlabDescT::page_kind_value;

		constexpr static std::uint16_t slot_size = SlotSize;

        using page_header_type = page::slab_storage;

		store(allocator_type& allocator, root_manager_type rmgr = {})
			: allocator_(&allocator)
			, root_(std::move(rmgr))
		{}
		
		allocator_type & underlying_device() {
			return *allocator_;
		}

		bool valid_id(pid_type pid) {
			return allocator_->valid_id(pid.pid) && (pid.slot != word_u16::max());
		}

		constexpr std::size_t page_size() {
			return slot_size;
		}

		page_handle allocate() {
			auto [ph, _] = create_entry();
			return ph;
		}

		page_handle fetch(pid_type pid) {
			auto [ph, _] = get_entry(pid, pid.slot);
			return ph;
		}

		void destroy(pid_type pid) {
			clear_entry(pid, pid.slot);
		}

		void flush(pid_type pid) {
			allocator_->flush(pid.pid);
		}

		void flush_all() {
			allocator_->flush_all();
		}

	private:

		std::tuple<page_handle, std::uint16_t> create_entry() {
			
			byte_span ready_slot;
			std::uint16_t position = 0xFFFF;
			under_page_handle page = {};

			if (auto ff = get_free()) {
				auto slots = ff.get_slots();
				if (const auto available_id = slots.find_available()) {
					position = *available_id;
					slots.set(position, {});
					ready_slot = slots.get(position);
					page = ff.handle;
					const auto capacity_for = slots.capacity() - slots.size();
					if (capacity_for == 0) {
						pop_page_from_list(ff);
					}
				}
			}
			else if(auto new_page = header_handle(allocator_->allocate())) {
				page_view_type pv{ new_page.handle.rw_span() };
				pv.header().init(static_cast<std::uint16_t>(page_kind_value),
					allocator_->page_size(), new_page.pid(),
					sizeof(page_header_type),
					page::metadata_size<typename default_slab_store_descriptor::page_metadata_type>());
				auto sh = pv.subheader<page_header_type>();
				sh->init();

				auto slots_dir = new_page.get_slots();
				slots_dir.init(slot_size);
				slots_dir.set(0, {});
				ready_slot = slots_dir.get(0);
				position = 0;
				page = new_page.handle;
				push_page_to_list(new_page);
			}
			if (!ready_slot.empty()) {
				page.mark_dirty();
				return { { std::move(page), position }, position };
			}
			return {};
		}

		std::tuple<page_handle, fulla::core::byte_span> get_entry(pid_type pid, std::uint16_t slot) {
			if (auto sh = header_handle(allocator_->fetch(pid.pid))) {
				auto slots = sh.get_slots();
				if (slots.test(slot)) {
					auto data = slots.get(slot);
					return { { sh.handle, slot }, data };
				}
			}
			return {};
		}

		void clear_entry(pid_type ph, std::uint16_t sid) {
			clear_entry({ allocator_->fetch(ph.pid), sid }, sid);
		}

		void clear_entry(page_handle ph, std::uint16_t sid) {
			if (auto sh = header_handle(ph.underlying_handle())) {
				auto slots = sh.get_slots();
				if (slots.test(sid)) {
					slots.erase(sid);
					const auto current_size = slots.size();
					sh.mark_dirty();
					if (current_size == 0) {
						allocator_->destroy(sh.handle.pid());
					}
				}
				if (!page_in_list(sh)) {
					push_page_to_list(sh);
				}
			}
		}

		struct header_handle {
			auto get() {
				page_view_type pv{ handle.rw_span() };
				return pv.subheader<page_header_type>();
			}

			auto get() const {
				page_view_type pv{ handle.ro_span() };
				return handle.subheader<page_header_type>();
			}

			operator bool() const {
				return handle.is_valid();
			}

			auto pid() const {
				return handle.pid();
			}

			void mark_dirty() {
				handle.mark_dirty();
			}

			auto get_slots() {
				page_view_type pv{ handle.rw_span() };
				return pv.get_slots_dir();
			}

			auto get_slots() const {
				cpage_view_type pv{ handle.ro_span() };
				return pv.get_slots_dir();
			}

			under_page_handle handle;
		};

		bool page_in_list(header_handle& ph) {
			const auto next = ph.get()->next.get();
			const auto prev = ph.get()->prev.get();
			return allocator_->valid_id(next) || allocator_->valid_id(prev);
		}

		void push_page_to_list(header_handle& ph) {
			auto current = fetch_root();
				
			ph.get()->next = current.pid();

			if (current) {
				current.get()->prev = ph.handle.pid();
			}
			root_.set_root(ph.handle.pid());
			ph.mark_dirty();
		}

		header_handle get_free() {
			return fetch_root();
		}

		void pop_page_from_list(header_handle& ph) {

			header_handle next{ allocator_->fetch(ph.get()->next) };
			header_handle prev{ allocator_->fetch(ph.get()->prev) };

			ph.get()->next = allocator_type::invalid_pid;
			ph.get()->prev = allocator_type::invalid_pid;
			ph.mark_dirty();

			if (next) {
				next.get()->prev = prev.pid();
			}

			if (prev) {
				prev.get()->next = next.pid();
			}

			root_.set_root(next.pid());
		}

		header_handle fetch_root() {
			if (root_.has_root()) {
				return header_handle{ allocator_->fetch(root_.get_root()) };
			}
			return {};
		}

		allocator_type* allocator_ = nullptr;
		root_manager_type root_{};
	};

}
