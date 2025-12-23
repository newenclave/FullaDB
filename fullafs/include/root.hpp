#pragma once

#include <cstdint>

#include "fulla/core/types.hpp"
#include "fs_page_allocator.hpp"
#include "directory_handle.hpp"
#include "directory_storage_handle.hpp"
#include "page_kinds.hpp"

namespace fullafs {

	template <fulla::storage::RandomAccessBlockDevice DevT, typename PidT = std::uint32_t>
	class root {
	public:
		
		using device_type = DevT;
		using pid_type = PidT;
		using allocator_type = storage::fs_page_allocator<device_type, PidT>;
		using page_handle = typename allocator_type::page_handle;
		using cpage_view_type = typename allocator_type::cpage_view_type;
		using page_view_type = typename allocator_type::page_view_type;
		using directory_handle = directory_handle<device_type, pid_type>;
		using directory_storage_handle = directory_storage_handle<device_type, pid_type>;

		root(device_type &dev, std::size_t cache_maximum_page = 10)
			: allocator_(dev, cache_maximum_page)
		{}

		void format() {
			allocator_.create_superblock(true);
			for (pid_type p = static_cast<pid_type>(allocator_.pages_count()) - 1; p > 0; --p) {
				allocator_.destroy(p);
			}
			create_root_directory();
		}

		directory_handle open_root() {
			auto sb = allocator_.fetch_superblock();
			if (sb.is_valid()) {
				return directory_handle(sb.root().page, word_u16::max(), allocator_);
			}
			return {};
		}

		allocator_type& get_allocator() noexcept {
			return allocator_;
		}

		const allocator_type& get_allocator() const noexcept {
			return allocator_;
		}

	private:

		void create_root_directory() {
			auto sb = allocator_.fetch_superblock();
			if (sb.is_valid()) {
				auto new_dir = directory_handle::create(&allocator_, 0);
				sb.root().page = new_dir.pid();
			}
		}

		allocator_type allocator_;
	};

}
