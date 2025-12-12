/*
 * File: buffer_manager.hpp
 * Author: newenclave
 * GitHub: https://github.com/newenclave
 * Created: 2025-10-25
 * License: MIT
 */

#pragma once
#include <cstdint>
#include <limits>
#include <unordered_map>
#include <vector>
#include <span>
#include <algorithm>
#include <cstring>
#include <atomic>

#include "fulla/core/bytes.hpp"
#include "fulla/core/debug.hpp"
#include "fulla/storage/device.hpp" // RandomAccessDevice, position_type
#include "fulla/storage/block_device.hpp" // RandomAccessBlockDevice, position_type
#include "fulla/storage/stats.hpp"  // stats / null_stats


namespace fulla::storage {

    using core::byte_view;
	template <storage::RandomAccessBlockDevice RadT, typename PidT = std::uint32_t>
	class buffer_manager {
		using block_id_type = typename RadT::block_id_type;
	public:

		using pid_type = PidT;
		using underlying_device_type = RadT;

		constexpr static const pid_type invalid_pid = std::numeric_limits<pid_type>::max();

		struct frame {

			frame() = default;
			frame(const frame&) = delete;
			frame& operator = (const frame&) = delete;
			frame(frame&&) = delete;
			frame& operator = (frame&&) = delete;

			void reset() {
				dirty = false;
				pid = invalid_pid;
				ref_count = 0;
				data = {};
			}

			void reinit(pid_type p, core::byte_span d) {
				dirty = false;
				pid = p;
				ref_count = 0;
				data = d;
				++gen;
			}

			void make_dirty() {
				dirty = true;
			}

			void ref() {
				++ref_count;
			}

			void unref() {
				DB_ASSERT(ref_count > 0, "Trying to unfer zero");
				--ref_count;
			}

			bool is_valid() const noexcept {
				return pid != invalid_pid;
			}

			bool dirty = false;
			pid_type pid = invalid_pid;
			std::size_t ref_count = 0;
			std::size_t gen = 1;
			core::byte_span data;
			frame* next = nullptr;
			frame* prev = nullptr;
		};

		struct page_handle {

			using pid_type = PidT;

			page_handle() = default;

			page_handle(const page_handle& other) noexcept {
				copy_impl(other);
			}

			page_handle& operator = (const page_handle& other) noexcept {
				copy_impl(other);
				return *this;
			}

			page_handle(page_handle&& other) noexcept {
				move_impl(std::move(other));
			}

			page_handle& operator = (page_handle&& other) noexcept {
				move_impl(std::move(other));
				return *this;
			}

			page_handle(buffer_manager* mgr, frame* s)
				: mgr_(mgr)
				, frame_(s)
			{
				if (frame_) {
					gen_ = frame_->gen;
					frame_->ref();
				}
			}

			~page_handle() noexcept {
				unref();
			}

			pid_type pid() const noexcept {
				if (frame_) {
					return frame_->pid;
				}
				return invalid_pid;
			}

			bool is_valid() const noexcept {
				return pid() != invalid_pid;
			}

			void mark_dirty() {
				if (frame_) {
					frame_->make_dirty();
				}
			}

			core::byte_span rw_span() noexcept {
				if (frame_) {
					DB_ASSERT(check_slot_gen(), "Bad slot");
					//frame_->make_dirty();
					return frame_->data;
				}
				return {};
			}

			core::byte_view ro_span() const noexcept {
				if (frame_) {
					DB_ASSERT(check_slot_gen(), "Bad slot");
					return { frame_->data.begin(), frame_->data.end() };
				}
				return {};
			}

			friend bool operator == (const page_handle& lhs, const page_handle& rhs) {
				return (lhs.frame_ == rhs.frame_);
			}

			//private:

			bool check_slot_gen() const noexcept {
				if (frame_) {
					return frame_->gen == gen_;
				}
				// empty slot, no check
				return true;
			}

			void copy_impl(const page_handle& other) noexcept {
				if (this != &other) {
					unref();
					reset();
					if (other.frame_) {
						mgr_ = other.mgr_;
						frame_ = other.frame_;
						gen_ = other.gen_;
						ref();
					}
				}
			}

			void move_impl(page_handle&& other) noexcept {
				if (this != &other) {
					unref();
					reset();
					if (other.frame_) {
						mgr_ = other.mgr_;
						frame_ = other.frame_;
						gen_ = other.gen_;
						other.reset();
					}
				}
			}

			void ref() {
				if (frame_) {
					DB_ASSERT(check_slot_gen(), "Bad slot");
					frame_->ref();
				}
			}

			void unref() {
				if (frame_) {
					DB_ASSERT(check_slot_gen(), "Bad slot");
					frame_->unref();
				}
			}

			void reset() {
				mgr_ = nullptr;
				frame_ = nullptr;
				gen_ = 0;
			}

			buffer_manager* mgr_ = nullptr;
			frame* frame_ = nullptr;
			std::size_t gen_ = 0;
		};

		using cache_map_type = std::unordered_map<pid_type, frame*>;

		buffer_manager(underlying_device_type& device, std::size_t maximum_pages)
			: device_(&device)
			, buffer_(maximum_pages* device.block_size())
			, frames_(maximum_pages)
		{
			frame* last = nullptr;
			for (auto& s : frames_) {
				s.prev = last;
				s.next = nullptr;
				if (last) {
					last->next = &s;
				}
				last = &s;
			}
			first_freed_ = &frames_[0];
		}

		buffer_manager() = delete;
		buffer_manager(buffer_manager&&) = default;
		buffer_manager& operator = (buffer_manager&&) = default;
		buffer_manager(const buffer_manager&) = delete;
		buffer_manager& operator = (const buffer_manager&) = delete;
		~buffer_manager() {
			flush_all();
		}

		page_handle allocate() {
			return create(true);
		}

		page_handle create(bool mark_dirty = false) {
			if (auto fs_idx = find_free_frame()) {
				auto buffer_data = frame_id_to_span(*fs_idx);
				const auto new_bid = device_->allocate_block();
				
				if (new_bid == RadT::invalid_block_id) {
					auto* fs = &frames_[*fs_idx];
					fs->reset();
					push_frame_freed(fs);
					return {};
				}
				
				const auto new_pid = static_cast<pid_type>(new_bid);
				auto* fs = &frames_[*fs_idx];
				fs->reinit(new_pid, buffer_data);
				push_frame_used(fs);
				cache_[new_pid] = fs;
				if (mark_dirty) {
					fs->make_dirty();
				}
				return page_handle(this, fs);
			}
			return {};
		}

		page_handle fetch(pid_type pid) {
			if (pid == invalid_pid) {
				return {};
			}
			if (auto itr = cache_.find(pid); itr != cache_.end()) {
				auto fs = itr->second;
				pop_frame_from_list(fs);
				push_frame_used(fs);
				return { this, fs };
			}
			if (auto fs_idx = find_free_frame()) {
				auto buffer_data = frame_id_to_span(*fs_idx);
				const auto ok = read(pid, buffer_data);
				auto* fs = &frames_[*fs_idx];
				if (ok) {
					fs->reinit(pid, buffer_data);
					push_frame_used(fs);
					cache_[pid] = fs;
					return { this, fs };
				}
				else {
					fs->reset();
					push_frame_freed(fs);
				}
			}
			return {};
		}

		std::size_t resident_pages() const noexcept {
			std::size_t c = 0;
			for (auto& s : frames_) {
				if (s.is_valid()) {
					++c;
				}
			}
			return c;
		}

		std::size_t evict_inactive() {
			std::size_t count = 0;
			for (auto& s : frames_) {
				if ((s.ref_count == 0) && (s.pid != invalid_pid)) {
					pop_frame_from_list(&s);
					evict(s.pid, true);
					count++;
				}
			}
			return count;
		}

		bool has_free_frames() const noexcept {
			for (auto& s : frames_) {
				if ((s.ref_count == 0) || (s.pid == invalid_pid)) {
					return true;
				}
			}
			return false;
		}

		void flush_all() {
			std::ranges::for_each(frames_, [this](auto& frame) { flush(&frame); });
		}

		void destroy(pid_type) {
			// TODO: rename/remove this call from here. 
			/// this is not a real call.This class is not supposed to be a page_allocator.
		}

		void flush(pid_type pid) {
			if (pid == invalid_pid) {
				return;
			}
			if (auto itr = cache_.find(pid); itr != cache_.end()) {
				auto fs = itr->second;
				flush(fs);
				return;
			}
			auto used = first_used_;
			while (used) {
				if (used->pid() == pid) {
					flush(used);
					return;
				}
				used = used->next;
			}
		}

		// TODO: remove this call when page_allocator is implemented
		underlying_device_type& underlying_device() noexcept {
			return *device_;
		}
		
		// TODO: remove this call when page_allocator is implemented
		const underlying_device_type& underlying_device() const noexcept {
			return *device_;
		}

		bool valid_id(pid_type pid) const {
			return pid < (device_->blocks_count());
		}

		auto page_size() const noexcept {
			return block_size();
		}

		auto pages_count() noexcept {
			return device_->blocks_count();
		}

		void evict(pid_type pid) {
			evict(pid, true);
		}

		void evict(pid_type pid, bool push_free) {
			auto itr = cache_.find(pid);
			if (itr != cache_.end()) {
				auto fs = itr->second;

				DB_ASSERT(fs->ref_count == 0, "Trying to evict a pinned page");

				flush(fs);
				fs->reset();
				cache_.erase(itr);
				if (push_free) {
					push_frame_freed(fs);
				}
			}
		}

	//private:

		void flush(frame* fs) {
			if (fs->dirty) {
				const auto ok = write(fs->pid, fs->data);
				if (ok) {
					fs->dirty = false;
				}
			}
		}

		core::byte_span frame_id_to_span(std::size_t id) {
			const auto buff_off = frame_id_to_buffer_offset(id);
			return { reinterpret_cast<core::byte*>(&buffer_[buff_off]), block_size() };
		}

		std::size_t frame_id_to_buffer_offset(std::size_t id) const noexcept {
			return static_cast<std::size_t>(id * block_size());
		}

		bool valid_slot_id(std::size_t id) const noexcept {
			return id < frames_.size();
		}

		auto block_size() const noexcept {
			return device_->block_size();
		}

		void push_frame_freed(frame* s) {
			if (first_freed_) {
				first_freed_->prev = s;
			}
			s->next = first_freed_;
			first_freed_ = s;
			first_freed_->prev = nullptr;
		}

		void push_frame_used(frame* s) {
			if (first_used_) {
				first_used_->prev = s;
			}
			s->next = first_used_;
			first_used_ = s;
			first_used_->prev = nullptr;
			if (nullptr == last_used_) {
				last_used_ = first_used_;
			}
		}

		void pop_frame_from_list(frame* s) {
			auto next = s->next;
			auto prev = s->prev;
			if (next) {
				next->prev = prev;
			}
			if (prev) {
				prev->next = next;
			}
			if (s == first_used_) {
				first_used_ = next;
			}
			if (s == last_used_) {
				last_used_ = prev;
			}
			if (s == first_freed_) {
				first_freed_ = next;
			}
			s->next = s->prev = nullptr;
		}

		std::optional<std::size_t> find_free_frame() {

			if (auto freed = try_pop_freed_frame()) {
				return freed;
			}

			if (auto first = try_find_first_available()) {
				return first;
			}

			return {};
		}

		std::optional<std::size_t> try_pop_freed_frame() {
			if (first_freed_) {
				auto s = first_freed_;
				pop_frame_from_list(s);
				return { std::distance(&frames_[0], s) };
			}
			return {};
		}

		std::optional<std::size_t> try_find_first_available() {
			auto last = last_used_;
			while (last) {
				if (last->ref_count == 0) {
					pop_frame_from_list(last);
					evict(last->pid, false);
					return { std::distance(&frames_[0], last) };
				}
				last = last->prev;
			}
			return {};
		}

		bool write(pid_type pid, core::byte_view data) {
			DB_ASSERT(data.size() <= block_size(), "src must be page_size maximum");
			const bool ok = device_->write_block(pid, data.data(), data.size());
			return ok;
		}

		bool read(pid_type pid, core::byte_span data) {
			const auto ok = device_->read_block(pid, data.data(), data.size());
			return ok;
		}

		RadT* device_ = nullptr;
		cache_map_type cache_;
		core::byte_buffer buffer_;
		std::vector<frame> frames_;
		frame* first_used_ = nullptr;
		frame* last_used_ = nullptr;
		frame* first_freed_ = nullptr;
	};

} // namespace fulla::storage
