/*
 * File: long_store/handle.hpp
 * Author: newenclave
 * GitHub: https://github.com/newenclave
 * Created: 2025-12-01
 * License: MIT
 */

#pragma once

#include <variant>

#include "fulla/core/assert.hpp"

#include "fulla/page/header.hpp"
#include "fulla/page/page_view.hpp"
#include "fulla/page/long_store.hpp"
#include "fulla/storage/device.hpp"
#include "fulla/storage/buffer_manager.hpp"

namespace fulla::long_store {

	template <storage::RandomAccessDevice DeviceT, typename PidT = std::uint32_t,
		std::uint16_t HeaderKindValue = 0, std::uint16_t ChunkKindValue =  1
	>	
	class handle {

		struct page_iterator;

	public:

		constexpr static const std::uint16_t header_kind_value = HeaderKindValue;
		constexpr static const std::uint16_t chunk_kind_value = ChunkKindValue;

		static_assert(header_kind_value != chunk_kind_value, "values must not be equal");

		using device_type = DeviceT;
		using pid_type = PidT;
		using buffer_manager_type = storage::buffer_manager<device_type, pid_type>;
		using page_handle = typename buffer_manager_type::page_handle;
		using header_type = page::long_store_header;
		using chunk_type = page::long_store_chunk;
		
		using slot_directory_type = page::slots::empty_directory_view;
		using page_view_type = page::page_view<slot_directory_type>;
		using cpage_view_type = page::const_page_view<slot_directory_type>;

		constexpr static const pid_type invalid_pid = std::numeric_limits<pid_type>::max();

		struct position_type {
			
			position_type(pid_type pid, std::size_t off) 
				: page_id(pid), offset(off) {}
			
			bool is_valid() const noexcept {
				return page_id != invalid_pid;
			}

		private:
			friend class handle;
			pid_type page_id{ invalid_pid };
			std::size_t offset{ 0 };
		};

		handle(buffer_manager_type& mgr, pid_type header_page)
			: mgr_(&mgr) 
			, header_page_(header_page)
			, gpage_(header_page)
			, spage_(header_page)
			, gpos_(0)
			, spos_(0)
		{}

		bool is_open() const noexcept {
			return (mgr_ != nullptr) && (header_page_ != invalid_pid);
		}

		std::size_t size() {
			if (header_page_ != invalid_pid) {
				auto hdr = load_header();
				if (hdr.is_valid()) {
					return static_cast<std::size_t>(hdr.total_size());
				}
			}
			return 0;
		}

		bool create() {
			if (header_page_ == invalid_pid) {
				auto ph = create_header();
				header_page_ = ph.pid();
				gpage_ = spage_ = header_page_;
				return ph.is_valid();
			}
			return false;
		}
		
		position_type tellg() const {
			return { gpage_, gpos_ };
		}

		position_type tellp() const {
			return { spage_, spos_ };
		}

		void seekg(std::size_t offset) {
			auto pos = iterator_at(offset);
			seekg({ pos.current_pid, pos.offset_in_page });
		}

		void seekg(position_type pos) {
			gpage_ = pos.page_id;
			gpos_ = pos.offset;
		}

		void seekp(std::size_t offset) {
			auto pos = iterator_at(offset);
			seekp({ pos.current_pid, pos.offset_in_page });
		}

		void seekp(position_type pos) {
			spage_ = pos.page_id;
			spos_ = pos.offset;
		}

		std::size_t append(const core::byte* buf, std::size_t len) {
			if (!is_open() || (buf == nullptr) || (len == 0)) {
				return 0;
			}
			auto it = last_iterator();
			return write_impl(it, buf, len);
		}

		std::size_t write(const core::byte* buf, std::size_t len) {
			if (!is_open() || (buf == nullptr) || (len == 0)) {
				return 0;
			}
			auto it = iterator_from(spage_, spos_);
			return write_impl(it, buf, len);
		}

		std::size_t read(core::byte* buf, std::size_t len) {
			if (!is_open() || (buf == nullptr) || (len == 0)) {
				return 0;
			}
			auto it = iterator_from(gpage_, gpos_);
			return read_impl(it, buf, len);
		}

	//private:

		std::size_t write_impl(page_iterator it, const core::byte* buf, std::size_t len) {
			auto hdr = load_header();
			if (!hdr.is_valid()) {
				return 0;
			}
			const auto total_written = traverse_pages(it, len, true,
				[&buf, &len, &hdr, this](auto& it) -> std::size_t {
					const auto current_size = it.get_size();
					const auto current_cap = it.capacity_in_current();
					const auto available = (current_cap - it.offset_in_page);
					const auto target_size = (it.offset_in_page + (len < available ? len : available));

					const auto written = it.write({ buf, len });
					if (target_size > current_size) {
						it.set_size(target_size);
						hdr.add_total_size(target_size - current_size);
					}
					it.offset_in_page = target_size;

					spage_ = it.current_pid;
					spos_ = it.offset_in_page;

					buf += written;
					len -= written;
					return written;
				}
			);
			return total_written;
		}

		std::size_t read_impl(page_iterator it, core::byte* buf, std::size_t len) {
			const auto total_read = traverse_pages(it, len, false,
				[&buf, &len, this](auto& it) -> std::size_t {
					const auto available = it.readable_bytes();
					const auto target_size = (it.offset_in_page + std::min(len, available));
					const auto read = it.read({ buf, len });
					it.offset_in_page = target_size;
					gpage_ = it.current_pid;
					gpos_ = it.offset_in_page;
					buf += read;
					len -= read;
					return read;
				}
			);
			return total_read;
		}

		void dump_pages() {
			auto hdr = load_header();
			if (hdr.is_valid()) {
				traverse_pages(begin_iterator(), hdr.total_size(), false, [](auto& itr) {
					std::cout << itr.current_pid << ":" << itr.get_size() << ", ";
					itr.offset_in_page = itr.get_size();
					return itr.get_size();
				});
				std::cout << "\n";
				return;
			}
			std::cout << "<empty>\n";
		}

		struct none_handle {
			constexpr static bool is_valid() noexcept {
				return false;
			}
		};

		struct handle_base {
			virtual ~handle_base() = default;
			handle_base() = default;
			handle_base(handle_base&&) = default;
			handle_base(const handle_base&) = default;
			handle_base& operator = (handle_base&&) = default;
			handle_base& operator = (const handle_base&) = default;
			handle_base(page_handle p) : ph(std::move(p)) {};
			
			pid_type pid() const noexcept {
				return ph.pid();
			}

			page_handle& get_page() noexcept {
				return ph;
			}

			const page_handle& get_page() const noexcept {
				return ph;
			}

			page_view_type get_page_view() noexcept {
				return page_view_type{ ph.rw_span() };
			}

			cpage_view_type get_page_view() const noexcept {
				return cpage_view_type{ ph.ro_span() };
			}

			bool is_valid() const noexcept {
				return ph.is_valid();
			}

			void mark_dirty() {
				ph.mark_dirty();
			}

			core::byte_span rw_all_data() {
				return { get_page_view().base_ptr(), capacity() };
			}

			core::byte_view ro_all_data() const {
				return { get_page_view().base_ptr(), capacity()};
			}

			core::byte_span rw_data() {
				return { get_page_view().base_ptr(), size() };
			}

			core::byte_view ro_data() const {
				return { get_page_view().base_ptr(), size() };
			}

			std::size_t capacity() const noexcept {
				cpage_view_type pv{ this->get_page().ro_span() };
				return static_cast<std::size_t>(pv.header().capacity());
			}

			std::size_t available() const noexcept {
				return capacity() - size();
			}
			
			virtual std::size_t size() const noexcept = 0;

		private:
			page_handle ph;
		};

		struct header_handle: public handle_base {
			
			header_handle() = default;
			header_handle(page_handle ph) : handle_base(ph) {}

			const page::long_store_header &header() const {
				cpage_view_type pv{ this->get_page().ro_span()};
				return *pv.subheader<page::long_store_header>();
			}

			page::long_store_header& header() {
				page_view_type pv{ this->get_page().rw_span() };
				return *pv.subheader<page::long_store_header>();
			}
			
			pid_type get_next() const {
				return static_cast<pid_type>(header().next);
			}

			void set_next(pid_type pid) {
				this->mark_dirty();
				header().next = pid;
			}

			pid_type get_last() const {
				return static_cast<pid_type>(header().last);
			}

			void set_last(pid_type pid) {
				this->mark_dirty();
				header().last = pid;
			}

			std::size_t size() const noexcept override {
				return get_size();
			}

			std::size_t get_size() const {
				const auto &hdr = header();
				return static_cast<std::size_t>(hdr.data.size);
			}

			void set_size(std::size_t val) {
				this->mark_dirty();
				header().data.size = static_cast<core::word_u16::word_type>(val);
			}

			std::size_t total_size() const {
				return static_cast<std::size_t>(header().total_size);
			}

			void add_total_size(std::size_t val) {
				header().total_size = static_cast<core::word_u32::word_type>(static_cast<std::size_t>(header().total_size) + val);
			}

			void dec_total_size(std::size_t val) {
				header().total_size = static_cast<core::word_u32::word_type>(static_cast<std::size_t>(header().total_size) - val);
			}

			void set_total_size(std::size_t val) {
				this->mark_dirty();
				header().total_size = static_cast<core::word_u32::word_type>(val);
			}
		};

		struct chunk_handle : public handle_base {

			chunk_handle() = default;
			chunk_handle(page_handle ph) : handle_base(ph) {}

			const page::long_store_chunk& header() const {
				cpage_view_type pv{ this->get_page().ro_span() };
				return *pv.subheader<page::long_store_chunk>();
			}
			
			page::long_store_chunk& header() {
				page_view_type pv{ this->get_page().rw_span() };
				return *pv.subheader<page::long_store_chunk>();
			}

			pid_type get_next() const {
				return static_cast<pid_type>(header().next);
			}

			void set_next(pid_type pid) {
				this->mark_dirty();
				header().next = pid;
			}

			pid_type get_prev() const {
				return static_cast<pid_type>(header().prev);
			}

			void set_prev(pid_type pid) {
				this->mark_dirty();
				header().prev = pid;
			}

			std::size_t size() const noexcept override {
				return get_size();
			}

			std::size_t get_size() const {
				const auto& hdr = header();
				return static_cast<std::size_t>(hdr.data.size);
			}
			void set_size(std::size_t val) {
				this->mark_dirty();
				header().data.size = static_cast<core::word_u16::word_type>(val);
			}
		};

		using page_variant = std::variant<none_handle, header_handle, chunk_handle>;

		struct page_iterator {
			handle* owner{ nullptr };
			pid_type current_pid{ invalid_pid };
			std::size_t offset_in_page{ 0 };

			constexpr static const auto npos = std::numeric_limits<std::size_t>::max();

			page_iterator() = default;
			page_iterator(handle* h, pid_type pid, std::size_t page_off)
				: owner(h)
				, current_pid(pid)
				, offset_in_page(page_off)
			{
			}

			bool is_valid() const noexcept {
				return current_pid != invalid_pid;
			}

			bool is_header() const noexcept {
				return (owner != nullptr) && (current_pid == owner->header_page_);
			}

			auto get_page() const {
				if (owner != nullptr) {
					return owner->fetch(current_pid);
				}
				return page_variant{};
			}

			auto get_owner_header() const {
				if (owner != nullptr) {
					return owner->load_header();
				}
				return header_handle{};
			}

			std::size_t get_size() const {
				auto pv = get_page();
				if (std::holds_alternative<header_handle>(pv)) {
					const auto h = std::get<header_handle>(pv);
					return h.get_size();
				}
				else if (std::holds_alternative<chunk_handle>(pv)) {
					const auto c = std::get<chunk_handle>(pv);
					return c.get_size();
				}
				return npos;
			}

			void set_size(std::size_t value) {
				auto pv = get_page();
				if (std::holds_alternative<header_handle>(pv)) {
					auto h = std::get<header_handle>(pv);
					h.set_size(value);
				}
				else if (std::holds_alternative<chunk_handle>(pv)) {
					auto c = std::get<chunk_handle>(pv);
					c.set_size(value);
				}
			}

			void mark_dirty() {
				auto ph = owner->mgr_->fetch(current_pid);
				if (ph.is_valid()) {
					mark_dirty();
				}
			}

			void mark_dirty(auto &page) {
				page.mark_dirty();
			}

			std::size_t writable_capacity() {
				auto pv = get_page();
				if (std::holds_alternative<header_handle>(pv)) {
					const auto h = std::get<header_handle>(pv);
					return h.capacity() - offset_in_page;
				}
				else if (std::holds_alternative<chunk_handle>(pv)) {
					const auto c = std::get<chunk_handle>(pv);
					return c.capacity() - offset_in_page;
				}
				return 0;
			}

			std::size_t readable_bytes() {
				auto pv = get_page();
				if (std::holds_alternative<header_handle>(pv)) {
					const auto h = std::get<header_handle>(pv);
					DB_ASSERT(h.capacity() >= h.size(), "Something went wrong");
					return std::min(h.capacity(), h.size()) - offset_in_page;
				}
				else if (std::holds_alternative<chunk_handle>(pv)) {
					const auto c = std::get<chunk_handle>(pv);
					DB_ASSERT(c.capacity() >= c.size(), "Something went wrong");
					return std::min(c.capacity(), c.size()) - offset_in_page;
				}
				return 0;
			}

			std::size_t capacity_in_current() {
				auto pv = get_page();
				if (std::holds_alternative<header_handle>(pv)) {
					const auto h = std::get<header_handle>(pv);
					return h.capacity();
				}
				else if (std::holds_alternative<chunk_handle>(pv)) {
					const auto c = std::get<chunk_handle>(pv);
					return c.capacity();
				}
				return 0;
			}

			std::size_t write(core::byte_view data) {
				auto pv = get_page();
				core::byte* dst = nullptr;
				handle_base* page_ptr = nullptr;
				std::size_t available = 0;

				if (std::holds_alternative<header_handle>(pv)) {
					auto h = std::get<header_handle>(pv);
					dst = h.rw_all_data().data() + offset_in_page;
					available = (h.capacity() - offset_in_page);
					page_ptr = &h;
				}
				else if (std::holds_alternative<chunk_handle>(pv)) {
					auto c = std::get<chunk_handle>(pv);
					dst = c.rw_all_data().data() + offset_in_page;
					available = (c.capacity() - offset_in_page);
					page_ptr = &c;
				}
				if ((dst != nullptr) && (available > 0)) {
					const auto to_write = std::min(data.size(), available);
					std::memcpy(dst, data.data(), to_write);
					mark_dirty(*page_ptr);
					return to_write;
				}
				return 0;
			}

			std::size_t read(core::byte_span dst) {
				auto pv = get_page();
				const core::byte* src = nullptr;
				std::size_t available = 0;
				if (std::holds_alternative<header_handle>(pv)) {
					const auto h = std::get<header_handle>(pv);
					src = h.ro_all_data().data() + offset_in_page;
					available = (h.size() < offset_in_page) ? 0 : h.size() - offset_in_page;
				}
				else if (std::holds_alternative<chunk_handle>(pv)) {
					const auto c = std::get<chunk_handle>(pv);
					src = c.ro_all_data().data() + offset_in_page;
					available = (c.size() < offset_in_page) ? 0 : c.size() - offset_in_page;
				}
				if ((src != nullptr) && (available > 0)) {
					const auto to_write = std::min(dst.size(), available);
					std::memcpy(dst.data(), src, to_write);
					return to_write;
				}
				return 0;
			}

			bool advance_to_next() {
				auto pv = get_page();
				if (std::holds_alternative<header_handle>(pv)) {
					auto h = std::get<header_handle>(pv);
					current_pid = h.get_next();
					offset_in_page = 0;
					return current_pid != invalid_pid;
				}
				else if (std::holds_alternative<chunk_handle>(pv)) {
					auto c = std::get<chunk_handle>(pv);
					current_pid = c.get_next();
					offset_in_page = 0;
					return current_pid != invalid_pid;
				}
				return false;
			}
		};

		page_iterator last_iterator() {
			auto header = load_header();
			if (!header.is_valid()) {
				return { this, invalid_pid, 0 };
			}
			return { this, header.get_last(), 0 };
		}

		page_iterator begin_iterator() {
			auto header = load_header();
			if (header.is_valid()) {
				return { this, header_page_, 0};
			}
			return { this, invalid_pid, 0};
		}

		page_iterator iterator_from(pid_type pid, std::size_t off) {
			return { this, pid, off };
		}

		page_iterator iterator_at(std::size_t target_offset) {
			auto it = begin_iterator();
			if (!it.is_valid()) {
				return it;
			}

			std::size_t remaining = target_offset;

			while (it.is_valid() && remaining > 0) {
				const auto available = it.writable_capacity();

				if (remaining < available) {
					it.offset_in_page += remaining;
					return it;
				}

				remaining -= available;
				if (!it.advance_to_next()) {
					return { this, invalid_pid, 0 };
				}
			}

			return it;
		}

		page_iterator expand_to(std::size_t target_offset) {
			auto header = load_header();
			if (!header.is_valid()) {
				return { this, invalid_pid, 0 };
			}

			const auto current_total = header.total_size();
			if (target_offset <= current_total) {
				return last_iterator();
			}

			std::size_t needed = (target_offset - current_total);

			auto it = last_iterator();

			it.offset_in_page = it.get_size();
			const auto last_available = it.writable_capacity();
			if (last_available > 0) {
				if (needed < last_available) {
					it.set_size(it.get_size() + needed);
					it.offset_in_page = it.get_size();
					header.add_total_size(needed);
					return it;
				}
				else {
					needed -= last_available;
					header.add_total_size(last_available);
					it.set_size(it.get_size() + last_available);
					it.offset_in_page = it.get_size();
				}
			}

			while (needed > 0) {
				const auto capacity = it.writable_capacity();
				if (capacity > 0) {
					const auto to_add = std::min(capacity, needed);

					auto pv = it.get_page();
					if (std::holds_alternative<header_handle>(pv)) {
						auto h = std::get<header_handle>(pv);
						h.set_size(h.get_size() + to_add);
						h.add_total_size(to_add);
					}
					else if (std::holds_alternative<chunk_handle>(pv)) {
						auto c = std::get<chunk_handle>(pv);
						c.set_size(c.get_size() + to_add);
						header.add_total_size(to_add);
					}

					needed -= to_add;
					if (needed == 0) {
						it.offset_in_page = it.get_size();
						break;
					}
				}

				auto new_chunk = create_chunk();
				if (!new_chunk.is_valid()) {
					return { this, invalid_pid, 0 };
				}
				it.current_pid = new_chunk.pid();
				it.offset_in_page = 0;
			}

			return it;
		}

		bool expand_current_or_create_next(page_iterator& it, std::size_t needed) {
			const auto capacity = it.capacity_in_current();
			const auto available = it.writable_capacity();
			const auto can_expand = capacity - available;

			if (can_expand > 0) {
				const auto to_add = (can_expand < needed) ? can_expand : needed;
				auto pv = it.get_page();

				if (std::holds_alternative<header_handle>(pv)) {
					auto h = std::get<header_handle>(pv);
					h.set_size(h.get_size() + to_add);
					h.add_total_size(to_add);
				}
				else if (std::holds_alternative<chunk_handle>(pv)) {
					auto c = std::get<chunk_handle>(pv);
					c.set_size(c.get_size() + to_add);
					auto header = load_header();
					header.add_total_size(to_add);
				}
				return true;
			}

			if (!it.advance_to_next()) {
				auto new_chunk = create_chunk();
				if (new_chunk.is_valid()) {
					it.current_pid = new_chunk.pid();
					it.offset_in_page = 0;
					return true;
				}
			}
			return false;
		}

		template <typename Func>
		std::size_t traverse_pages(page_iterator it, std::size_t length, bool allow_expand, Func&& func) {
			if (!it.is_valid()) {
				return 0;
			}

			std::size_t processed = 0;
			std::size_t remaining = length;

			while (remaining > 0 && it.is_valid()) {
				auto pv = it.get_page();
				const auto available = it.writable_capacity();
				const auto to_process = (available < remaining) ? available : remaining;

				if (to_process == 0) {
					if (!allow_expand) {
						break;
					}
					if (!expand_current_or_create_next(it, remaining)) {
						break;
					}
					continue;
				}

				std::size_t actually_processed = func(it);

				processed += actually_processed;
				remaining -= actually_processed;

				if (actually_processed < to_process) {
					break;
				}


				const auto available_for_operation = allow_expand ? it.writable_capacity() : it.readable_bytes();

				if ((remaining > 0) && (it.offset_in_page >= available_for_operation)) {
					if (!it.advance_to_next()) {
						if (!allow_expand) {
							break;
						}
						auto new_chunk = create_chunk();
						if (new_chunk.is_valid()) {
							it.current_pid = new_chunk.pid();
							it.offset_in_page = 0;
						}
						else {
							break;
						}
					}
				}
			}

			return processed;
		}

		bool remove_page(pid_type pid) {
			auto pv = fetch(pid);
			if (pv.holds_alternative<header_handle>()) {
				/// think about header removal
				auto head = std::get<header_handle>(pv);
				if (head.get_next() != invalid_pid) {
					head.set_next(invalid_pid);
					head.set_last(invalid_pid);
					head.set_size(0);
				}
			}
			else if (pv.holds_alternative<chunk_handle>()) {
				auto chunk = std::get<chunk_handle>(pv);
				auto next = chunk.get_next();
				auto prev = chunk.get_prev();
				if (next != invalid_pid) {
					auto next_chunk = load_chunk(next);
					if (next_chunk.is_valid()) {
						next_chunk.set_prev(prev);
					}
				}
				if (prev != invalid_pid) {
					auto prev_page = fetch(prev);
					if (prev_page.holds_alternative<header_handle>()) {
						auto header = std::get<header_handle>(prev_page);
						header.set_next(next);
					}
					else if (prev_page.holds_alternative<chunk_handle>()) {
						auto prev_chunk = std::get<chunk_handle>(prev_page);
						prev_chunk.set_next(next);
					}
				}
				auto header_page = load_header();
				if (header_page.is_valid()) {
					if (header_page.get_last() == pid) {
						header_page.set_last(prev);
					}
				}
			}
			return false;
		}

		page_variant fetch(pid_type pid) {
			auto ph = mgr_->fetch(pid);
			if (ph.is_valid()) {
				cpage_view_type pv{ ph.ro_span() };
				if (pv.header().kind.get() == header_kind_value) {
					return { header_handle{ph} };
				}
				else if (pv.header().kind.get() == chunk_kind_value) {
					return { chunk_handle{ph} };
				}
			}
			return { none_handle{} };
		}

		header_handle load_header() {
			if (is_open()) {
				auto ph = mgr_->fetch(header_page_);
				if (ph.is_valid()) {
					return { ph };
				}
			}
			return {};
		}

		chunk_handle load_chunk(pid_type pid) {
			if (is_open()) {
				auto ph = mgr_->fetch(pid);
				if (ph.is_valid()) {
					return { ph };
				}
			}
			return {};
		}

		position_type read_data(core::byte* ptr, std::size_t len) {
			auto header = load_header();
			if (header.is_valid()) {
				if (header.get_size() >= len) {
					
				}
			}
			return { invalid_pid, 0 };
		}

		position_type jump_to_position(std::size_t offset) {
			auto header = load_header();
			if (header.is_valid()) {

				if (header.get_size() >= offset) {
					return { header_page_, offset };
				}
				else if (header.capacity() >= offset) {
					header.set_size(offset);
					header.set_total_size(offset);
					return { header_page_, offset };
				}
				else {
					offset = (offset - header.get_size());
				}
				pid_type current_page = header.get_next();
				while (offset > 0) {
					auto chunk = load_chunk(current_page);
					if (chunk.is_valid()) {
						if (chunk.get_size() > offset) {
							return { current_page, offset };
						}
						else if (chunk.capacity() >= offset) {
							header.add_total_size(offset - chunk.get_size());
							chunk.set_size(offset);
							return { current_page, offset };
						}
						else {
							offset = (offset - chunk.get_size());
							current_page = chunk.get_next();
						}
					}
					else {
						// need to create more chunks
						auto new_chunk = create_chunk();
						if (new_chunk.is_valid()) {
							if (new_chunk.available() >= offset) {
								const auto size = offset;
								new_chunk.set_size(offset);
								header.add_total_size(offset);
								offset = 0;
								return { new_chunk.pid(), size};
							}
							else {
								const auto full_available = new_chunk.available();
								offset = (offset - full_available);
								header.add_total_size(full_available);
								new_chunk.set_size(full_available);
							}
							current_page = invalid_pid;
						}
						else {
							return { invalid_pid, 0 };
						}
					}
				}
			}
			return { invalid_pid, 0 };
		}

		auto create_header() {
			auto ph = mgr_->create(true);
			header_page_ = ph.pid();
			page_view_type pv{ ph.rw_span() };
			pv.header().init(header_kind_value, mgr_->block_size(), ph.pid(), sizeof(header_type));
			pv.get_slots_dir().init();
			auto* sh = pv.subheader<header_type>();
			sh->total_size = 0;
			sh->data.size = 0;
			sh->next = invalid_pid;
			sh->last = header_page_;
			return header_handle{ ph };
		}

		auto create_chunk() {
			auto ph = mgr_->create(true);
			page_view_type pv{ ph.rw_span() };
			pv.header().init(chunk_kind_value, mgr_->block_size(), ph.pid(), sizeof(chunk_type));
			pv.get_slots_dir().init();
			auto* sh = pv.subheader<chunk_type>();
			sh->data.size = 0;
			sh->next = invalid_pid;
			sh->prev = invalid_pid;	

			/// fixing the links
			auto hdr = load_header();
			const auto last_pid = hdr.get_last();
			if (last_pid != header_page_) {
				auto last_chunk = load_chunk(last_pid);
				if (last_chunk.is_valid()) {
					last_chunk.set_next(ph.pid());
					last_chunk.mark_dirty();
					pv.subheader<chunk_type>()->prev = last_pid;
				}
			}
			else {
				hdr.set_next(ph.pid());
				hdr.mark_dirty();
				pv.subheader<chunk_type>()->prev = hdr.pid();
			}
			hdr.set_last(ph.pid());
			return chunk_handle{ ph };
		}

		buffer_manager_type *mgr_ = nullptr;
		pid_type header_page_ = invalid_pid;
		pid_type gpage_ = invalid_pid;
		pid_type spage_ = invalid_pid;
		std::size_t gpos_ = 0;
		std::size_t spos_ = 0;
	};
}
