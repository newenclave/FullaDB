#pragma once

#include <filesystem>
#include <cstdint>

#include "fulla/core/types.hpp"
#include "fulla/storage/buffer_manager.hpp"
#include "fulla/storage/file_block_device.hpp"
#include "fulla/page_allocator/base.hpp"
#include "fulla/page/page_view.hpp"

#include "page_kinds.hpp"
#include "page_headers.hpp"
#include "handle_base.hpp"

namespace fullafs::storage { 

    using namespace fulla; 
    using default_device_type = fulla::storage::file_block_device;

    template <fulla::storage::RandomAccessBlockDevice DevT = default_device_type, 
        typename PidT = std::uint32_t>
    class fs_page_allocator: public fulla::page_allocator::base<DevT, PidT> {
    public:
        
        using device_type = DevT;
        using parent_type = fulla::page_allocator::base<device_type, PidT>;
        using pid_type = typename parent_type::pid_type;
        using page_handle = typename parent_type::page_handle;
        using empty_slot_directory = fulla::page::slots::empty_directory_view;

        using cpage_view_type = fulla::page::const_page_view<empty_slot_directory>;
        using page_view_type = fulla::page::page_view<empty_slot_directory>;

        fs_page_allocator(device_type &dev, std::size_t maximum_pages)
            : fulla::page_allocator::base<device_type, std::uint32_t>(dev, maximum_pages)
        {}

        void destroy(pid_type pid) override {
            auto ph = this->fetch(pid);
            auto sb = fetch_superblock();
            if (ph.is_valid() && sb.is_valid()) {
                init_freed(ph);
                freed_handle fh{ ph };
                fh.set_next(sb.first_freed());
                sb.set_first_freed(ph.pid());
            }
        }
        
        page_handle allocate() override {
            auto freed_block = pop_freed();
            if (freed_block.is_valid()) {
                return freed_block;
            }
            else {
                return parent_type::allocate();
            }
        }

        void create_superblock(bool force = false) {
            check_create_superblock(force);
        }

        struct freed_handle: handle_base<parent_type, page::freed> {
            freed_handle(page_handle ph)
                : handle_base<parent_type, page::freed>(std::move(ph))
            {}
            pid_type next() const {
                return this->get()->next;
            }
            void set_next(pid_type pid) {
                this->get()->next = pid;
            }
        };

        struct superblock_handle: handle_base<parent_type, page::superblock> {
  
            superblock_handle(page_handle ph)
                : handle_base<parent_type, page::superblock>(std::move(ph))
            {}

            page::entry_descriptor& root() noexcept {
                return this->get()->root;
            }

            const page::entry_descriptor& root() const noexcept {
                return this->get()->root;
            }

            pid_type first_freed() const {
                return this->get()->first_freed_page;
            }

            void set_first_freed(pid_type ff) {
                this->get()->first_freed_page = ff;
            }
        };

        auto fetch_freed(pid_type pid) {
            return freed_handle{ this->fetch(pid) };
        }

        auto fetch_superblock() {
            return superblock_handle{ this->fetch(0) };
        }

    private:

        auto pop_freed() {
            auto sb = fetch_superblock();
            if (sb.is_valid()) {
                auto first_pid = sb.first_freed();
                if (this->valid_id(first_pid)) {
                    auto fp = fetch_freed(first_pid);
                    if (fp.is_valid()) {
                        auto next = fp.get()->next.get();
                        sb.set_first_freed(next);
                        sb.handle().mark_dirty();
                        fp.handle().mark_dirty();
                        return fp.handle();
                    }
                }
            }
            return page_handle{};
        }

        void check_create_superblock(bool force) {
            page_handle sbh;
            if (this->pages_count() != 0) {
                sbh = this->fetch(0);
            }
            else {
                sbh = allocate();
            }
            if (sbh.is_valid()) {
                init_superblock(sbh, force);
            }
            else {
                // TODO: something went wrong...
            }
        }
        
        void init_freed(page_handle& ph) {
            page_view_type pv{ ph.rw_span() };
            pv.header().init(static_cast<std::uint16_t>(page::kind::freed),
                this->page_size(), ph.pid(),
                sizeof(page::freed), 0);
            auto sh = pv.subheader<page::freed>();
            sh->init();
            ph.mark_dirty();
        }

        void init_superblock(page_handle &ph, bool force) {
            page_view_type pv{ ph.rw_span() };
            if (force || (pv.header().kind.get() != static_cast<std::uint16_t>(page::kind::superblock))) {
                pv.header().init(static_cast<std::uint16_t>(page::kind::superblock),
                    this->page_size(), 0,
                    sizeof(page::superblock), 0);
                auto sh = pv.subheader<page::superblock>();
                sh->init();
                ph.mark_dirty();
            }
        }
    };
}
