#pragma once

#include "fulla/long_store/handle.hpp"
#include "fs_page_allocator.hpp"
#include "core.hpp"
#include "page_kinds.hpp"
#include "handle_base.hpp"
#include "key_values.hpp"

namespace fullafs {

	using fulla::core::word_u16;

	template <fulla::storage::RandomAccessBlockDevice DevT, typename PidT = std::uint32_t>
	class file_handle {
		struct file_descriptor {
			using header_metadata_type = page::file_metadata;
			using chunk_metadata_type = fulla::page::empty_metadata;
			constexpr static const std::uint16_t header_kind_value = static_cast<std::uint16_t>(page::kind::file_header);
			constexpr static const std::uint16_t chunk_kind_value = static_cast<std::uint16_t>(page::kind::file_chunk);
		};

	public:
		using allocator_type = storage::fs_page_allocator<DevT, PidT>;
		using pid_type = typename allocator_type::pid_type;
		using page_handle = typename allocator_type::page_handle;
		using cpage_view_type = typename allocator_type::cpage_view_type;
		using page_view_type = typename allocator_type::page_view_type;

		using store_handle_type = fulla::long_store::handle<allocator_type, file_descriptor>;

		file_handle() = default;
		file_handle(const file_handle&) = default;
		file_handle& operator = (const file_handle&) = default;
		file_handle(file_handle&&) = default;
		file_handle& operator = (file_handle&&) = default;

		file_handle(pid_type header_page, allocator_type& alloc)
			: header_page_(header_page)
			, allocator_(&alloc) 
		{}

		bool is_valid() const noexcept {
			return (allocator_ != nullptr) && (header_page_ != allocator_type::invalid_pid);
		}

		operator bool() const noexcept {
			return is_valid();
		}

		static file_handle create(allocator_type* allocator, pid_type parent) {
			store_handle_type fh(*allocator, allocator_type::invalid_pid);
			const auto pid = fh.create();
			file_handle res(pid, *allocator); 
			if (auto hdr = res.open_header()) {
				hdr.metadata()->parent = parent;
			}
			return res;
		}

		pid_type pid() const noexcept {
			return header_page_;
		}

	private:

		struct header_handle : public storage::handle_base<allocator_type, fulla::page::long_store_header> {
			using parent_type = storage::handle_base<allocator_type, fulla::page::long_store_header>;
			
			header_handle() = default;
			header_handle(page_handle ph)
				: parent_type(std::move(ph)) 
			{}

			auto metadata() noexcept {
				page_view_type pv{ this->handle().rw_span()};
				return pv.metadata_as<page::file_metadata>();
			}

			auto metadata() const noexcept {
				cpage_view_type pv{ this->handle().ro_span() };
				return pv.metadata_as<page::file_metadata>();
			}
		};

		auto open() {
			if (is_valid()) {
				return fulla::long_store::handle(*allocator_, header_page_);
			}
			return {};
		}

		auto open_header() {
			if (is_valid()) {
				return header_handle(allocator_->fetch(header_page_));
			}
			return header_handle{};
		}

		allocator_type* allocator_ = nullptr;
		pid_type header_page_ = allocator_type::invalid_pid;
	};
}
