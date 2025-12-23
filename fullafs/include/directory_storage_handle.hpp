#pragma once

#include "fulla/bpt/tree.hpp"
#include "fulla/slots/stable_directory.hpp"
#include "fulla/page/page_view.hpp"

#include "page_headers.hpp"
#include "fs_page_allocator.hpp"
#include "core.hpp"
#include "page_kinds.hpp"
#include "handle_base.hpp"

namespace fullafs {
	
	using fulla::core::word_u16;
	using fulla::core::byte_span;
	using fulla::core::byte_view;

	template <fulla::storage::RandomAccessBlockDevice DevT, typename PidT = std::uint32_t>
	class directory_storage_handle {
	public:

		using slot_type = fulla::slots::stable_directory_view<byte_span>;
		using cslot_type = fulla::slots::stable_directory_view<byte_view>;

		using page_view_type = fulla::page::page_view<slot_type>;
		using cpage_view_type = fulla::page::const_page_view<cslot_type>;

		using allocator_type = storage::fs_page_allocator<DevT, PidT>;

		using page_handle = typename allocator_type::page_handle;
		using pid_type = typename allocator_type::pid_type;

		using page_header_type = page::directory_storage;
		using page_slot_type = page::directory_header;
		using superblock_header_type = page::superblock;

		directory_storage_handle(allocator_type& allocator)
			: allocator_(&allocator)
		{}
		
		std::tuple<page_handle, std::uint16_t> allocate_entry(pid_type parent, std::uint16_t parent_slot = 0xFFFF) {
			byte_span ready_slot;
			std::uint16_t position = 0xFFFF;
			page_handle page = {};

			if (auto ff = get_free()) {
				auto slots = ff.get_slots();
				if (const auto available_id = slots.find_available()) {
					position = *available_id;
					slots.set(position, {});
					ready_slot = slots.get(position);
					page = ff.handle();
					
					const auto capacity_for = slots.capacity() - slots.size();

					if (capacity_for == 0) {
						pop_page_from_list(ff);
					}
				}
			}

			else if(auto new_page = storage_handle{ allocator_->allocate() }) {
				page_view_type pv{ new_page.handle().rw_span() };

				pv.header().init(static_cast<std::uint16_t>(page::kind::directory_storage),
					allocator_->page_size(), new_page.handle().pid(),
					sizeof(page::directory_storage), 0);

				auto slots = new_page.get_slots();
				slots.init(static_cast<std::uint16_t>(sizeof(page_slot_type)));

				[[maybe_unused]] const auto available = slots.capacity();

				position = 0;
				slots.set(0, {});
				ready_slot = slots.get(0);
				page = new_page.handle();
				push_page_to_list(new_page);
			}

			if (!ready_slot.empty()) {
				auto hdr = reinterpret_cast<page_slot_type*>(ready_slot.data());
				hdr->init(parent, parent_slot);
				page.mark_dirty();
				return { std::move(page), position };
			}
			return {};
		}
		
		std::tuple<page_handle, fulla::core::byte_span> open_entry(pid_type pid, std::uint16_t slot) {
			storage_handle sh(allocator_->fetch(pid)); 
			if (sh) {
				auto slots = sh.get_slots();
				if (slots.test(slot)) {
					auto data = slots.get(slot);
					return { sh.handle(), data };
				}
			}
			return {};
		}

		void free_entry(pid_type ph, std::uint16_t sid) {
			free_entry(allocator_->fetch(ph), sid);
		}

		void free_entry(page_handle ph, std::uint16_t sid) {
			storage_handle sh(ph);
			if (sh) {
				auto slots = sh.get_slots();
				if (sid < slots.size()) {
					slots.erase(sid);
					sh.mark_dirty();
				}
				if (!page_in_list(sh)) {
					push_page_to_list(sh);
				}
			}
		}

	//private:

		struct storage_handle : storage::handle_base<allocator_type, page_header_type> {
			
			storage_handle(page_handle ph) : storage::handle_base<allocator_type, page_header_type>(ph) {}
			storage_handle() = default;

			auto get_slots() {
				page_view_type pv{ this->handle().rw_span() };
				return pv.get_slots_dir();
			}
		};

		bool page_in_list(storage_handle& ph) {
			const auto next = ph.get()->next.get();
			const auto prev = ph.get()->prev.get();
			return allocator_->valid_id(next) || allocator_->valid_id(prev);
		}

		void push_page_to_list(storage_handle& ph) {
			if (auto sb = allocator_->fetch_superblock()) {
				auto current = storage_handle{ allocator_->fetch(sb.get()->first_directory_storage) };
				ph.get()->next = sb.get()->first_directory_storage;
				if (current) {
					current.get()->prev = ph.handle().pid();
				}
				sb.get()->first_directory_storage = ph.handle().pid();
				ph.mark_dirty();
			}
		}

		storage_handle get_free() {
			if (auto sb = allocator_->fetch_superblock()) {
				return storage_handle{ allocator_->fetch(sb.get()->first_directory_storage) };
			}
			return {};
		}

		void pop_page_from_list(storage_handle&ph) {

			storage_handle next{ allocator_->fetch(ph.get()->next) };
			storage_handle prev{ allocator_->fetch(ph.get()->prev) };

			ph.get()->next = allocator_type::invalid_pid;
			ph.get()->prev = allocator_type::invalid_pid;
			ph.mark_dirty();

			if (next) {
				next.get()->prev = prev.handle().pid();
			}

			if (prev) {
				prev.get()->next = next.handle().pid();
			}

			if (auto sb = allocator_->fetch_superblock()) {
				sb.get()->first_directory_storage = next.handle().pid();
			}
		}

		allocator_type* allocator_ = nullptr;
	};
}