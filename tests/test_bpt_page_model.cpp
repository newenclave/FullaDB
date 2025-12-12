#include <filesystem>
#include <vector>
#include <map>

#include "tests.hpp"

#include "fulla/bpt/paged/model.hpp"
#include "fulla/storage/file_block_device.hpp"
#include "fulla/storage/memory_block_device.hpp"
#include "fulla/storage/buffer_manager.hpp"
#include "fulla/page_allocator/base.hpp"

#include "fulla/page/header.hpp"
#include "fulla/page/bpt_inode.hpp"
#include "fulla/page/bpt_leaf.hpp"
#include "fulla/bpt/tree.hpp"

#include "fulla/codec/prop.hpp"

namespace {
	using fulla::core::byte_buffer;
	using fulla::core::byte_view;
	using fulla::core::byte_span;
	using fulla::core::byte;

	using namespace fulla::storage;
	using namespace fulla::bpt;
	using namespace fulla::codec;

	using key_like_type = typename paged::model_common::key_like_type;
	using key_out_type = typename paged::model_common::key_out_type;
	using value_in_type = typename paged::model_common::value_in_type;
	using value_out_type = typename paged::model_common::value_out_type;

	using page_header_type = fulla::page::page_header;
	using page_view_type = typename paged::model_common::page_view_type;

	static std::filesystem::path temp_file(const char* stem) {
		namespace fs = std::filesystem;
		static std::random_device rd;
		auto p = fs::temp_directory_path() / (std::string(stem) + "_" + std::to_string(rd()) + ".bin");
		return p;
	}

	static std::string get_random_string(std::size_t min_len, std::size_t max_len = 20) {
		static std::random_device rd;
		static std::mt19937 gen(rd());

		const std::string chars =
			"0123456789"
			"ABCDEFGHIJKLMNOPQRSTUVWXYZ"
			"abcdefghijklmnopqrstuvwxyz";

		std::uniform_int_distribution<std::size_t> len_dist(min_len, max_len);
		std::uniform_int_distribution<std::size_t> char_dist(0, chars.size() - 1);

		std::size_t len = len_dist(gen);
		std::string res;
		res.reserve(len);

		for (std::size_t i = 0; i < len; ++i) {
			res.push_back(chars[char_dist(gen)]);
		}

		return res;
	}

	constexpr static const auto DEFAULT_BUFFER_SIZE = 4096UL;

	key_like_type as_key_like(const std::string& val) {
		return { .key = byte_view{ reinterpret_cast<const byte*>(val.data()), val.size()} };
	}

	value_in_type as_value_in(const std::string& val) {
		return { .val = byte_view{ reinterpret_cast<const byte*>(val.data()), val.size()} };
	}
	std::string as_string(value_out_type val) {
		return { (const char*)val.val.data(),val.val.size() };
	}

	template <typename TreeT>
	void validate_keys(TreeT& t) {
		std::optional<key_out_type> last;
		auto less_type = fulla::page::make_record_less();
		std::size_t count = 0;
		for (auto& k : t) {
			++count;
			if (last.has_value()) {
				CHECK(less_type(last->key, k.first.key));
			}

		}
		(void)count;
	}
	template <typename C1, typename C2>
	bool compare(const C1& c1, const C2& c2) {
		return std::is_eq(std::lexicographical_compare_three_way(
			c1.begin(), c1.end(),
			c2.begin(), c2.end()
		));
	}

	struct string_less {
		bool operator ()(byte_view a, byte_view b) const noexcept {
			return std::is_lt(compare(a, b));
		}
		auto compare(byte_view a, byte_view b) const noexcept {
			return std::lexicographical_compare_three_way(
				a.begin(), a.end(),
				b.begin(), b.end()
			);
		}
	};

}

TEST_SUITE("bpt/paged/model bpt") {

	TEST_CASE("creating") {

		auto path = temp_file("test_page_model");
		{
			constexpr static const auto small_buffer_size = DEFAULT_BUFFER_SIZE;
			constexpr static const auto element_mximum = 10000;

			memory_block_device mem(small_buffer_size);
			using buffer_manager_type = buffer_manager<memory_block_device>;
			using model_type = paged::model<buffer_manager_type>;
			using bpt_type = fulla::bpt::tree<model_type>;

			using BM = buffer_manager_type;
			BM bm(mem, 40);
			static std::random_device rd;
			static std::mt19937 gen(rd());

			SUBCASE("create tree") {
				bpt_type bpt(bm);
				bpt.set_rebalance_policy(policies::rebalance::neighbor_share);

				std::map<std::string, std::string> test;

				std::cout << "File: " << path.string() << "\n";

				for (unsigned int i = 0; i < element_mximum; ++i) {

					auto ts = get_random_string(5, 60);
					auto key = prop::make_record(prop::str{ts});

					if (!test.contains(ts)) {
						test[ts] = ts;
						CHECK(bm.has_free_frames());

						REQUIRE(bpt.insert(key_like_type{ key.view() }, as_value_in(ts)));
						auto itr = bpt.find(key_like_type{ key.view() });
						if (itr == bpt.end()) {
							std::cout << "\n\nfailed to find: " << ts << "\n\n";
							bpt.dump();
						}
					}
				}

				const auto check_map = [&]() {
					int id = 0;
					for (auto& ti : test) {
						auto key = prop::make_record(prop::str{ ti.first });
						auto it = bpt.find(key_like_type{ key.view() });
						CHECK(bpt.end() != it);
						auto val = ti.second;
						CHECK(compare(as_value_in(val).val, it->second.val));
						id++;
					}
				};

				validate_keys(bpt);
				check_map();

				auto tsize = test.size();
				for (unsigned int i = 0; i < tsize / 2; ++i) {

					std::uniform_int_distribution<std::size_t> dist(0, test.size() - 1);
					auto idx = dist(gen);
					auto itr = test.begin();
					std::advance(itr, idx);

					auto key = prop::make_record(prop::str{ itr->first });
					auto it = bpt.find(key_like_type{ key.view() });

					CHECK(bpt.end() != it);
					test.erase(itr);
					bpt.erase(it);
				}

				for (auto &t: test) {
					auto ts = get_random_string(5, 90);
					auto key = prop::make_record(prop::str{ t.first });
					REQUIRE(bpt.update(key_like_type{ key.view() }, as_value_in(ts)));
					test[t.first] = ts;
				}

				validate_keys(bpt);
				check_map();

				tsize = test.size();
				for (unsigned int i = 0; i < tsize; ++i) {

					if (i == tsize - 1) {
						std::cout << "";
					}
					std::uniform_int_distribution<std::size_t> dist(0, test.size() - 1);
					auto idx = dist(gen);
					auto itr = test.begin();
					std::advance(itr, idx);

					auto key = prop::make_record(prop::str{ itr->first });
					auto it = bpt.find(key_like_type{ key.view() });

					CHECK(bpt.end() != it);
					test.erase(itr);
					bpt.erase(it);
				}
				validate_keys(bpt);
				check_map();

				CHECK(test.size() == 0);
				auto [root, found] = bpt.get_model().get_accessor().load_root();
				CHECK(root == bpt.get_model().get_invalid_node_id());
				CHECK_FALSE(found);
			}
			std::cout << "Result filesize: " <<  mem.blocks_count() << " blocks " 
				<< mem.blocks_count() * mem.block_size() << " bytes\n";
		}
		std::filesystem::remove(path);
	}

	TEST_CASE("custom less") {
		constexpr static const auto small_buffer_size = DEFAULT_BUFFER_SIZE / 6;
		constexpr static const auto element_mximum = 1000;

		memory_block_device mem(small_buffer_size);

		using BM = buffer_manager<memory_block_device>;
		BM bm(mem, 6);
		using model_type = paged::model<BM, string_less>;

		using node_id_type = typename model_type::node_id_type;
		using bpt_type = fulla::bpt::tree<model_type>;

		static std::random_device rd;
		static std::mt19937 gen(rd());

		SUBCASE("Create string -> string") {
			bpt_type bpt(bm);
			std::map<std::string, std::string> test;

			bpt.get_model().set_stringifier_callbacks(
				[&](node_id_type id) -> std::string { return id == bpt.get_model().get_invalid_node_id() ? "<null>" : std::to_string(id); },
				[](key_out_type kout) -> std::string { return std::string{ (const char*)kout.key.data(), kout.key.size() }; },
				[](value_out_type vout) -> std::string { return std::string{ (const char*)vout.val.data(), vout.val.size() }; }
			);

			for (int i = 0; i < element_mximum; ++i) {
				auto ts = get_random_string(5, 26);
				if (!test.contains(ts)) {

					bpt.insert(as_key_like(ts), as_value_in(ts), policies::insert::insert);
					test[ts] = ts;
					//std::cout << "\"" << ts << "\", ";
					auto itr = bpt.find(as_key_like(ts));
					if (itr == bpt.end()) {
						std::cout << "\n\nFail to find: " << ts << "\n";
						bpt.dump();
					}
					REQUIRE(itr != bpt.end());
				}
			}
			validate_keys(bpt);
			//bpt.dump();

			while (!test.empty()) {
				auto val = test.begin();
				auto itr = bpt.find(as_key_like(val->first));
				
				CHECK(itr != bpt.end());
				bpt.erase(itr);
				itr = bpt.find(as_key_like(val->first));
				CHECK(itr == bpt.end());

				test.erase(val);
			}
			validate_keys(bpt);
		}

	}
}
