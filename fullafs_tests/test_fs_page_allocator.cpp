#include "tests.hpp"
#include "fs_page_allocator.hpp"

namespace {
	using namespace fullafs;
	using block_device_type = fulla::storage::file_block_device;
	static std::filesystem::path temp_file(const char* stem) {
		namespace fs = std::filesystem;
		static std::random_device rd;
		auto p = fs::temp_directory_path() / (std::string(stem) + "_" + std::to_string(rd()) + ".bin");
		return p;
	}
}

TEST_SUITE("fs/page_allocator") {
	constexpr const auto DEFAULT_PAGE_SIZE = 4096;
	TEST_CASE("create file with superblock") {
		auto tmp_file_name = temp_file("fs_test");
		{
			block_device_type device(tmp_file_name, DEFAULT_PAGE_SIZE);
			storage::fs_page_allocator<> allocator(device, 10);
			allocator.create_superblock();
			auto sb = allocator.fetch(0);
			CHECK(sb.is_valid());

			for (int i = 1; i <= 10; ++i) {
				CHECK(allocator.allocate().is_valid());
			}

			CHECK(allocator.pages_count() == 11);

			for (int i = 1; i <= 10; i += 2) {
				CHECK(allocator.pages_count() == 11);
				allocator.destroy(i);
			}

			for (int i = 0; i < 5; ++i) {
				auto next = allocator.allocate();
				CHECK(next.pid() <= 10);
				CHECK(allocator.pages_count() == 11);
			}

			auto next = allocator.allocate();
			CHECK(next.pid() == 11);
			CHECK(allocator.pages_count() == 12);
		}

		std::filesystem::remove(tmp_file_name);
	}
}
