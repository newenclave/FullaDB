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

#include "fulla/core/bytes.hpp"
#include "fulla/storage/device.hpp" // RandomAccessDevice, position_type
#include "fulla/storage/stats.hpp"  // stats / null_stats

#ifndef DB_ASSERT
  #include <cassert>
  #define DB_ASSERT(cond, msg) do { (void)(msg); assert(cond); } while(0)
#endif

namespace fulla::storage {

    using core::byte_view;

    template <RandomAccessDevice DevT, typename PidT = std::uint32_t, typename StatsT = stats>
    class buffer_manager {
    public:
        using PID = PidT;
        using device_type = DevT;
        static constexpr PID invalid_page_pid = std::numeric_limits<PID>::max();

        struct frame {
            PID           page_id { invalid_page_pid };
            std::uint32_t pin     { 0 };
            bool          dirty   { false };
            bool          ref_bit { false };
            std::uint32_t gen     { 0 };
            fulla::core::byte_span data{};

            void reinit(PID pid) {
                page_id = pid;
                pin     = 1;
                dirty   = false;
                ref_bit = true;
                ++gen;
            }
        };
        using frame_span = std::span<frame>;

        class page_handle {
        public:
            page_handle() = default;
            page_handle(buffer_manager* pool, frame* f, std::uint32_t gen_snapshot)
                : pool_(pool), f_(f), gen_(gen_snapshot) {
                if (f_ != nullptr) { 
                    f_->ref_bit = true; 
                }
            }

            page_handle(const page_handle &other)
                : pool_(other.pool_) 
                , f_(other.f_)
                , gen_(other.gen_)
            {
                if (f_ != nullptr) {
                    f_->ref_bit = true;
                    ++f_->pin;
                }
            };

            page_handle& operator=(const page_handle &other) {
                if (this != &other) {
                    reset();
                    pool_ = other.pool_;
                    f_ = other.f_;
                    gen_ = other.gen_;
                    if (f_ != nullptr) {
                        f_->ref_bit = true;
                        ++f_->pin;
                    }
                }
                return *this;
            }

            page_handle(page_handle&& other) noexcept
                : pool_(other.pool_)
                , f_(other.f_)
                , gen_(other.gen_) {
                other.pool_ = nullptr; 
                other.f_ = nullptr; 
                other.gen_ = 0;
            }

            page_handle& operator=(page_handle&& other) noexcept {
                if (this != &other) {
                    reset();
                    pool_ = other.pool_; 
                    f_ = other.f_; 
                    gen_ = other.gen_;
                    other.pool_ = nullptr; 
                    other.f_ = nullptr; 
                    other.gen_ = 0;
                }
                return *this;
            }

            ~page_handle() { reset(); }

            explicit operator bool() const noexcept { return f_ != nullptr; }
            PID id() const noexcept { return f_ ? f_->page_id : invalid_page_pid; }

            friend bool operator == (const page_handle& lhs, const page_handle& rhs) {
                if (lhs.f_ && rhs.f_) {
                    return (lhs.pool_ == rhs.pool_)
                        && (lhs.f_ == rhs.f_)
                        && (lhs.gen_ == rhs.gen_)
                        ;
                }
                else {
                    return (lhs.f_ == rhs.f_);
                }
            }

            fulla::core::byte_view ro_span() const noexcept {
                DB_ASSERT(!f_ || f_->gen == gen_, "stale handle: page was evicted/reused");
                if (f_ == nullptr) { return {}; }
                return { f_->data.data(), f_->data.size() };
            }

            fulla::core::byte_span rw_span() {
                DB_ASSERT(f_ && f_->gen == gen_, "stale handle: page was evicted/reused");
                if (f_ == nullptr) { return {}; }
                f_->dirty = true;
                return { f_->data.data(), f_->data.size() };
            }

            void reset() noexcept {
                if (pool_ != nullptr && f_ != nullptr) { pool_->unpin(f_); }
                pool_ = nullptr; 
                f_ = nullptr; 
                gen_ = 0;
            }

        private:
            friend class buffer_manager;
            buffer_manager*  pool_ { nullptr };
            frame*           f_    { nullptr };
            std::uint32_t    gen_  { 0 };
        };

    public:
        buffer_manager(device_type& dev, fulla::core::byte_span data_store, frame_span frame_store)
            : dev_(dev)
            , all_data_(data_store)
            , frames_(frame_store) {
            const auto ps = page_size();
            DB_ASSERT(ps > 0, "page size must be > 0");
            DB_ASSERT(all_data_.size() % ps == 0, "arena size must be multiple of page_size");

            const auto total_frames = all_data_.size() / ps;
            DB_ASSERT(frames_.size() == total_frames, "frames count must match arena pages");

            page_map_.reserve(frames_.size());
            for (std::size_t i = 0; i < total_frames; ++i) {
                frames_[i].data = { all_data_.data() + (i * ps), ps };
            }
        }

        std::size_t page_size() const { return dev_.block_size(); }
        std::size_t capacity() const noexcept { return frames_.size(); }

        page_handle create() {
            st_.created_pages++;
            PID pid = allocate_page();
            frame* f = take_frame_for_new_page(pid);
            DB_ASSERT(f != nullptr, "no frame available (all pinned)");
            std::fill(f->data.begin(), f->data.end(), fulla::core::byte{0});
            f->dirty = true;
            return page_handle{ this, f, f->gen };
        }

        page_handle fetch(PID pid) {
            auto it = page_map_.find(pid);
            if (it != page_map_.end()) {
                frame* f = it->second;
                st_.hits++;
                ++f->pin;
                f->ref_bit = true;
                return page_handle{ this, f, f->gen };
            }
            st_.misses++;
            frame* f = load_into_victim_with_evict(pid);
            DB_ASSERT(f != nullptr, "no victim available (all pinned)");
            return page_handle{ this, f, f->gen };
        }

        page_handle try_fetch(PID pid) {
            auto it = page_map_.find(pid);
            if (it != page_map_.end()) {
                frame* f = it->second;
                ++f->pin; 
                f->ref_bit = true;
                return page_handle{ this, f, f->gen };
            }
            if (auto* freef = try_take_free_frame()) {
                if (!read(pid, freef->data)) { return {}; }
                freef->reinit(pid);
                page_map_.emplace(pid, freef);
                return page_handle{ this, freef, freef->gen };
            }
            const auto idx = pick_victim();
            if (idx == npos) { return {}; }

            frame& vict = frames_[idx];
            if (vict.page_id != invalid_page_pid) {
                page_map_.erase(vict.page_id);
            }
            if (!read(pid, vict.data)) { return {}; }
            vict.reinit(pid);
            page_map_.emplace(pid, &vict);
            return page_handle{ this, &vict, vict.gen };
        }

        void flush(PID pid, bool force = false) {
            auto it = page_map_.find(pid);
            DB_ASSERT(it != page_map_.end(), "flush: page not loaded");
            frame* f = it->second;
            if (!force) {
                DB_ASSERT(f->pin == 0, "flush: page is pinned");
            }
            if (f->dirty) {
                if (force) { 
                    st_.forced_flushes++; 
                }
                write(pid, f->data); // best-effort
                f->dirty = false;
            }
        }

        void flush_all(bool force = false) {
            for (auto& f : frames_) {
                if (f.page_id != invalid_page_pid && f.dirty && ((f.pin == 0) || force)) {
                    if (force) { 
                        st_.forced_flushes++; 
                    }
                    write(f.page_id, f.data);
                    f.dirty = false;
                }
            }
        }

        void mark_dirty(PID pid) {
            auto it = page_map_.find(pid);
            DB_ASSERT(it != page_map_.end(), "mark_dirty: page not loaded");
            it->second->dirty = true;
        }

        bool evict(PID pid) {
            auto it = page_map_.find(pid);
            if (it == page_map_.end()) { 
                return false; 
            }
            frame* f = it->second;
            if (f->pin != 0 || f->dirty) { 
                return false; 
            }
            f->page_id = invalid_page_pid;
            f->ref_bit = false;
            ++f->gen;
            page_map_.erase(it);
            return true;
        }

        const StatsT& get_stats() const noexcept { return st_; }

        std::size_t resident_pages() const noexcept {
            std::size_t c = 0;
            for (auto& f : frames_) {
                if (f.page_id != invalid_page_pid) { ++c; }
            }
            return c;
        }

        void unpin(frame* f) noexcept {
            if (f == nullptr) { return; }
            DB_ASSERT(f->pin > 0, "unpin underflow");
            f->pin -= 1;
        }

    private:
        static constexpr std::size_t npos = std::numeric_limits<std::size_t>::max();

        frame* take_frame_for_new_page(PID pid) {
            if (auto* f = try_take_free_frame()) {
                f->reinit(pid);
                page_map_.emplace(pid, f);
                return f;
            }
            const auto idx = pick_victim();
            if (idx == npos) {
                return nullptr;
            }

            frame& vict = frames_[idx];
            if (vict.page_id != invalid_page_pid) {
                page_map_.erase(vict.page_id);
                st_.evictions++;
            }
            vict.reinit(pid);
            page_map_.emplace(pid, &vict);
            return &vict;
        }

        frame* try_take_free_frame() {
            for (auto& f : frames_) {
                if (f.page_id == invalid_page_pid && (f.pin == 0)) { return &f; }
            }
            return nullptr;
        }

        frame* load_into_victim_with_evict(PID pid) {
            if (auto* freef = try_take_free_frame()) {
                if (!read(pid, freef->data)) { return nullptr; }
                freef->reinit(pid);
                page_map_.emplace(pid, freef);
                return freef;
            }
            const std::size_t idx = pick_victim();
            if (idx == npos) { return nullptr; }

            frame& vict = frames_[idx];
            if (vict.page_id != invalid_page_pid) {
                page_map_.erase(vict.page_id);
                st_.evictions++;
            }
            if (!read(pid, vict.data)) { return nullptr; }
            vict.reinit(pid);
            page_map_.emplace(pid, &vict);
            return &vict;
        }

        std::size_t pick_victim() {
            const auto n = frames_.size();
            DB_ASSERT(n > 0, "no frames configured");
            for (std::size_t step = 0; step < 2 * n; ++step) {
                const std::uint32_t i = clock_hand_;
                clock_hand_ = (clock_hand_ + 1) % n;
                st_.clock_scans++;
                frame& f = frames_[i];
                if (f.pin != 0) { continue; }
                if (f.ref_bit) {
                    st_.refbit_clears++;
                    f.ref_bit = false;
                    continue;
                }
                if (f.dirty) {
                    if (f.page_id != invalid_page_pid) {
                        write(f.page_id, f.data);
                        st_.writebacks++;
                    }
                    f.dirty = false;
                    continue;
                }
                return i;
            }
            st_.pinned_fail++;
            return npos;
        }

        PID allocate_page() {
            st_.alloc_pages++;
            const auto pos = dev_.allocate_block();
            DB_ASSERT(pos % page_size() == 0, "device returned misaligned block");
            return static_cast<PID>(pos / page_size());
        }

        bool read(PID pid, fulla::core::byte_span dst) {
            DB_ASSERT(dst.size() == page_size(), "dst must be page_size");
            const bool ok = dev_.read_at_offset(static_cast<position_type>(pid) * page_size(),
                                                dst.data(), dst.size());
            if (ok) { st_.reads++; }
            return ok;
        }

        bool write(PID pid, fulla::core::byte_view src) {
            DB_ASSERT(src.size() == page_size(), "src must be page_size");
            const bool ok = dev_.write_at_offset(static_cast<position_type>(pid) * page_size(),
                                                src.data(), src.size());
            if (ok) { st_.writes++; }
            return ok;
        }

    private:
        device_type& dev_;
        fulla::core::byte_span all_data_;
        frame_span frames_;
        std::unordered_map<PID, frame*> page_map_;
        std::uint32_t clock_hand_ { 0 };
        StatsT st_{};
    };

} // namespace fulla::storage
