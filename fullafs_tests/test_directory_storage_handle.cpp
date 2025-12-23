#include "fulla/storage/memory_block_device.hpp"
#include "tests.hpp"
#include "directory_storage_handle.hpp"
#include "fs_page_allocator.hpp"

namespace {
	using namespace fullafs;
	using block_device_type = fulla::storage::file_block_device;
	std::filesystem::path temp_file(const char* stem) {
		namespace fs = std::filesystem;
		static std::random_device rd;
		auto p = fs::temp_directory_path() / (std::string(stem) + "_" + std::to_string(rd()) + ".bin");
		return p;
	}

	using mem_device_type = fulla::storage::memory_block_device;
}

TEST_SUITE("fullafs/directory_storage_handle") {
	using page_allocator = storage::fs_page_allocator<mem_device_type>;
	using directory_storage_handle = directory_storage_handle<mem_device_type>;
	TEST_CASE("Create pages") {
		mem_device_type dev(4096);
		page_allocator allocator(dev, 10);
		allocator.create_superblock();
		directory_storage_handle hdr(allocator, 0);
		
		auto [first_dir, first_id] = hdr.allocate_entry(0);
		std::cout << std::format("id: {}\n", first_dir.pid());
		for (int i = 0; i < 400; ++i) {
			if (i == 143) {
				std::cout << "";
			}
			[[maybe_unused]] auto [dir, id] = hdr.allocate_entry(0);
		}

		hdr.free_entry(3, 0);

		auto [tdir, tid] = hdr.allocate_entry(0);
		auto [tdir0, tid0] = hdr.allocate_entry(0);

		std::cout << std::format("\nnew page: {}:{}\n", tdir.pid(), tid);
		std::cout << std::format("\nnew page 0: {}:{}\n", tdir0.pid(), tid0);

		std::cout << std::format("\nPage allocated: {}\n", dev.blocks_count());
	}
}

