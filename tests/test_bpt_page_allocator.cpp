#include <filesystem>
#include <vector>

#include "tests.hpp"

#include "fulla/bpt/paged/model.hpp"
#include "fulla/storage/file_device.hpp"
#include "fulla/page/header.hpp"
#include "fulla/page/bpt_inode.hpp"
#include "fulla/page/bpt_leaf.hpp"

#include "fulla/codec/prop.hpp"

namespace {
	using fulla::core::byte_buffer;
	using fulla::core::byte_view;
	using fulla::core::byte_span;
	using fulla::core::byte;
	
	using namespace fulla::storage;
	using namespace fulla::bpt;

	using file_device = fulla::storage::file_device;

	using model_type = paged::model<file_device>;
	using key_like_type = typename model_type::key_like_type;

	using page_header_type = fulla::page::page_header;
	using page_view_type = typename model_type::page_view_type;

	using file_device = fulla::storage::file_device;

	static std::filesystem::path temp_file(const char* stem) {
		namespace fs = std::filesystem;
		std::random_device rd;
		auto p = fs::temp_directory_path() / (std::string(stem) + "_" + std::to_string(rd()) + ".bin");
		return p;
	}

	constexpr static const auto DEFAULT_BUFFER_SIZE = 4096UL;

	key_like_type as_key_like(const std::string& val) {
		return { .key = byte_view{ reinterpret_cast<const byte*>(val.data()), val.size()} };
	}


}

TEST_SUITE("bpt/paged/model allocator_type") {
	
	TEST_CASE("nodes creating") {
		auto path = temp_file("allocator");
		{
			file_device dev(path, DEFAULT_BUFFER_SIZE);
			std::vector<byte> arena(DEFAULT_BUFFER_SIZE * 10);
			using BM = buffer_manager<file_device>;
			std::vector<typename BM::frame> frames(10);
			BM bm(dev, byte_span{ arena.data(), arena.size() },
				std::span{ frames.data(), frames.size() });

			SUBCASE("create leaf") {
				model_type::accessor_type acc(bm);
				auto leaf = acc.create_leaf();
				CHECK(leaf.self() == 0);
				CHECK(leaf.size() == 0);
				CHECK(leaf.capacity() == 19);
				CHECK(leaf.get_parent() == 0);
				CHECK(leaf.get_next() == 0);
				CHECK(leaf.get_prev() == 0);

				auto leaf2 = acc.create_leaf();
				CHECK(leaf2.self() == 1);

				auto leaf3 = acc.create_leaf();
				CHECK(leaf3.self() == 2);

				auto leaf_copy = leaf2;
				leaf_copy.set_parent(990099);
				CHECK(leaf_copy.self() == 1);
				CHECK(leaf2.self() == leaf_copy.self());
				CHECK(leaf2.get_parent() == leaf_copy.get_parent());

				bm.flush_all(true);
			}

			SUBCASE("load leaf") {
				model_type::accessor_type acc(bm);
				auto create_leaf = acc.create_leaf();
				create_leaf.set_parent(887766);

				auto load_leaf = acc.load_leaf(create_leaf.self());
				auto invalid_leaf = acc.load_leaf(4);
				CHECK(!invalid_leaf.is_valid());

				REQUIRE(load_leaf.is_valid());
				CHECK(load_leaf.self() == create_leaf.self());
				CHECK(load_leaf.get_parent() == 887766);
			}

			SUBCASE("check merge test") {
				model_type::accessor_type acc(bm);
				auto leaf0 = acc.create_leaf();
				auto leaf1 = acc.create_leaf();
				auto inode0 = acc.create_inode();
				auto inode1 = acc.create_inode();
				
				CHECK(acc.can_merge_leafs(leaf0, leaf1));
				CHECK(acc.can_merge_inodes(inode0, inode1));

				int i = 0;
				while(!inode0.is_full()) {
					auto ts = std::to_string(i) + " value as key!";
					inode0.insert_child(0, as_key_like(ts), 10);
				}

				[[maybe_unused]] const auto count = inode0.size();

				inode1.insert_child(0, as_key_like("New key!"), 1000);
				CHECK(!acc.can_merge_inodes(inode0, inode1));

				inode0.erase(inode0.size() / 2);
				CHECK(acc.can_merge_inodes(inode0, inode1));
			}
		}

		CHECK(std::filesystem::remove(path));

	}	
}
