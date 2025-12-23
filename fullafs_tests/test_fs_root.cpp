#include "tests.hpp"
#include "root.hpp"

namespace {
	using namespace fullafs;
	using block_device_type = fulla::storage::file_block_device;
	using root_type = root<block_device_type>;
	static std::filesystem::path temp_file(const char* stem) {
		namespace fs = std::filesystem;
		static std::random_device rd;
		auto p = fs::temp_directory_path() / (std::string(stem) + "_" + std::to_string(rd()) + ".bin");
		return p;
	}
}

TEST_SUITE("fs/root") {
	constexpr const auto DEFAULT_PAGE_SIZE = 4096;
	TEST_CASE("create file with superblock") {
		auto tmp_file_name = temp_file("fs_test");
        {
			std::cout << "Creating path: " << tmp_file_name.string() << "\n";
			block_device_type dev(tmp_file_name.string(), DEFAULT_PAGE_SIZE);
			root_type root(dev, 10);
			root.format();
			CHECK(dev.blocks_count() == 2);
			auto root_dir = root.open_root();
			CHECK(root_dir.is_valid());

			for (int i = 0; i < 10; ++i) {
				if (i % 2 == 0) {
					auto next_dir = root_dir.mkdir("test_" + std::to_string(i));
					CHECK(next_dir.is_valid());
				}
				else {
					auto next_fil = root_dir.touch("file_" + std::to_string(i));
					CHECK(next_fil.is_valid());
				}
			}
			root.get_allocator().flush_all();
		}

		{
			std::cout << "Opening file: " << tmp_file_name.string() << "\n";
			block_device_type dev(tmp_file_name.string(), DEFAULT_PAGE_SIZE);
			root_type root(dev, 10);

			auto root_dir = root.open_root();

			CHECK(root_dir.total_entries() == 10);

			for (const auto &d : root_dir) {
				const bool isdir = d.is_directory();
				std::cout << (isdir ? "[" : " ") << d.name() << (isdir ? "]" : " ") << "\n";
			}

			auto itr = root_dir.find("test_0");
			CHECK(itr != root_dir.end());

			root.get_allocator().flush_all();

			std::cout << root.get_allocator().pages_count() << " pages allocated\n";
		}

		//std::filesystem::remove(tmp_file_name);
    }
}

