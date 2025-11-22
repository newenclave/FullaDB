#include <filesystem>
#include <vector>
#include <map>

#include "tests.hpp"

#include "fulla/bpt/paged/model.hpp"
#include "fulla/storage/file_device.hpp"
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

	using file_device = fulla::storage::file_device;

	using model_type = paged::model<file_device>;
	using bpt_type = fulla::bpt::tree<model_type>;

	using key_like_type = typename model_type::key_like_type;
	using key_out_type = typename model_type::key_out_type;
	using value_in_type = typename model_type::value_in_type;

	using page_header_type = fulla::page::page_header;
	using page_view_type = typename model_type::page_view_type;

	using file_device = fulla::storage::file_device;

	static std::filesystem::path temp_file(const char* stem) {
		namespace fs = std::filesystem;
		static std::random_device rd;
		auto p = fs::temp_directory_path() / (std::string(stem) + "_" + std::to_string(rd()) + ".bin");
		return p;
	}


	static std::string get_random_string(std::size_t max_len, std::size_t min_len = 10) {
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

	void validate_keys(bpt_type& t) {
		std::optional<key_out_type> last;
		auto less_type = fulla::page::make_record_less();
		for (auto& k : t) {
			if (last.has_value()) {
				CHECK(less_type(last->key, k.first.key));
			}
		}
	}
	template <typename C1, typename C2>
	bool compare(const C1& c1, const C2& c2) {
		return std::is_eq(std::lexicographical_compare_three_way(
			c1.begin(), c1.end(),
			c2.begin(), c2.end()
		));
	}
}

TEST_SUITE("bpt/paged/model bpt") {

	TEST_CASE("creating") {

		auto path = temp_file("allocator");
		{

			constexpr static const auto small_buffer_size = DEFAULT_BUFFER_SIZE;

			file_device dev(path, small_buffer_size);
			using BM = buffer_manager<file_device>;
			BM bm(dev, 40);
			static std::random_device rd;
			static std::mt19937 gen(rd());

			SUBCASE("create tree") {
				bpt_type bpt(bm);
				std::map<std::string, std::string> test;
				for (unsigned int i = 0; i < 2000; ++i) {
					auto ts = get_random_string(10, 5);
					auto key = prop::make_record(prop::str{ts});
					if (!test.contains(ts)) {
						test[ts] = ts;
						CHECK(bm.has_free_slots());
						REQUIRE(bpt.insert(key_like_type{ key.view() }, as_value_in(ts)));
					}
					else {
						std::cout << "";
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

				check_map();
				
				validate_keys(bpt);
				for (unsigned int i = 0; i < 1000; ++i) {

					std::uniform_int_distribution<std::size_t> dist(0, test.size() - 1);
					auto idx = dist(gen);
					auto itr = test.begin();
					std::advance(itr, idx);

					auto key = prop::make_record(prop::str{ itr->first });
					auto it = bpt.find(key_like_type{ key.view() });

					if (it->first.key.size() != key.view().size()) {
						std::cout << "";
					}

					//check_map();
					CHECK(bpt.end() != it);
					test.erase(itr);
					bpt.erase(it);
					//bpt.remove(key_like_type{ key.view() });
				}

				validate_keys(bpt);
				check_map();

				bpt.dump();

			}

		}

		//CHECK(std::filesystem::remove(path));

	}
}