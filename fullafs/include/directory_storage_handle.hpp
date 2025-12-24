#pragma once

#include "fulla/bpt/tree.hpp"
#include "fulla/slots/stable_directory.hpp"
#include "fulla/slab_store/store.hpp"
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
		using pid_type = typename allocator_type::pid_type;

		struct root_accessor {

			using root_type = pid_type;

			root_accessor(allocator_type& allocator) : allocator_(&allocator) {}

			bool has_root() const noexcept {
				if (allocator_ != nullptr) {
					if (auto sb = allocator_->fetch_superblock()) {
						return sb.get()->first_directory_storage != allocator_type::invalid_pid;
					}
				}
				return false;
			}

			pid_type get_root() const {
				if (allocator_ != nullptr) {
					if (auto sb = allocator_->fetch_superblock()) {
						return sb.get()->first_directory_storage;
					}
				}
				return allocator_type::invalid_pid;
			}

			void set_root(pid_type pid) {
				if (allocator_ != nullptr) {
					if (auto sb = allocator_->fetch_superblock()) {
						sb.get()->first_directory_storage = pid;
					}
				}
			}

			allocator_type* allocator_ = nullptr;
		};

		struct slab_descriptor {
			constexpr static std::uint16_t page_kind_value = static_cast<std::uint16_t>(fullafs::page::kind::directory_storage);
			using page_metadata_type = fulla::page::empty_metadata;
		};

		using slab_allocator_type = fulla::slab_store::store<allocator_type,
			sizeof(page::directory_header),
			root_accessor,
			slab_descriptor, pid_type>;

		using page_handle = typename allocator_type::page_handle;
		using pid_type = typename allocator_type::pid_type;

		using page_header_type = page::directory_storage;
		using page_slot_type = page::directory_header;
		using superblock_header_type = page::superblock;

		directory_storage_handle(allocator_type& allocator)
			: allocator_(allocator, root_accessor(allocator))
		{}

		std::tuple<page_handle, std::uint16_t> allocate_entry(pid_type parent, std::uint16_t parent_slot = 0xFFFF) {
			auto page = allocator_.allocate();
			if (page.is_valid()) {
				auto ready_slot = page.rw_span();
				if (!ready_slot.empty()) {
					auto hdr = reinterpret_cast<page_slot_type*>(ready_slot.data());
					hdr->init(parent, parent_slot);
					page.mark_dirty();
					auto pos = page.slot();
					return { page.underlying_handle(), pos };
				}
			}
			return {};
		}

		std::tuple<page_handle, fulla::core::byte_span> open_entry(pid_type pid, std::uint16_t slot) {
			auto page = allocator_.fetch({ pid, slot });
			if (page.is_valid()) {
				return { page.underlying_handle(), page.rw_span() };
			}
			return {};
		}

		void free_entry(pid_type pid, std::uint16_t slot) {
			allocator_.destroy({ pid, slot });
		}

	private:

		slab_allocator_type allocator_{};
	};
}