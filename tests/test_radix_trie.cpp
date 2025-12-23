#include <filesystem>
#include <vector>
#include <map>

#include "tests.hpp"

#include "fulla/bpt/paged/model.hpp"
#include "fulla/storage/memory_device.hpp"
#include "fulla/storage/memory_block_device.hpp"

#include "fulla/page/header.hpp"
#include "fulla/page/long_store.hpp"
#include "fulla/long_store/handle.hpp"

#include "fulla/codec/prop.hpp"
#include "fulla/radix_table/trie.hpp"
#include "fulla/radix_table/memory/model.hpp"
#include "fulla/radix_table/paged/model.hpp"

namespace {
	using namespace fulla;
	using fulla::core::byte_buffer;
	using fulla::core::byte_view;
	using fulla::core::byte_span;
	using fulla::core::byte;

	std::uint32_t get_random_uint(std::uint32_t min, std::uint32_t max) {
		static std::random_device rd;
		static std::mt19937 gen(rd());
		std::uniform_int_distribution<std::size_t> len_dist(min, max);
		return static_cast<std::uint32_t>(len_dist(gen));
	}

	std::string get_random_string(std::size_t min_len, std::size_t max_len = 20) {
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

	template <storage::RandomAccessBlockDevice RadT, typename PidT = std::uint32_t>
	struct test_page_allocator final: public page_allocator::base<RadT, PidT> {
		using base_type = page_allocator::base<RadT, PidT>;
		using pid_type = PidT;
		using underlying_device_type = RadT;
		using page_handle = typename base_type::page_handle;

		constexpr static const pid_type invalid_pid = std::numeric_limits<pid_type>::max();

		test_page_allocator(underlying_device_type& device, std::size_t maximum_pages)
			: base_type(device, maximum_pages)
		{}

		page_handle allocate() override { 
			allocated++;
			return base_type::allocate();
		}
		void destroy(pid_type) override {
			destoyed++;
		}
		std::size_t allocated = 0;
		std::size_t destoyed = 0;
	};

}

TEST_SUITE("radix_table/trie/memory") {

	using model = radix_table::memory::model<std::string, 32>;
	using trie_type = radix_table::trie<std::uint64_t, model>;
	using test_map_type = std::map<std::uint64_t, std::string>;

	using device_type = storage::memory_block_device;
	using page_allocator_type = page_allocator::base<device_type>;

	constexpr static const std::uint32_t MAXIMUM_VALUES = 0xFFFF;

	TEST_CASE("Adding values 0 1 2 3 ...") {
		trie_type trie;
		test_map_type tests;

		for (int i = 0; i < MAXIMUM_VALUES; ++i) {
			auto value = get_random_string(5, 20);
			tests.emplace(i, value);
			trie.set(i, value);
		}

		for (int i = 0; i < MAXIMUM_VALUES; ++i) {
			auto val = trie.get(i);
			CHECK(tests[i] == val);
		}

		CHECK(!trie.has(MAXIMUM_VALUES + 1));
		trie.remove(0);
		CHECK(trie.get(0) == "");
	}

	TEST_CASE("paged/allocator") {
		device_type dev(4 * 1024);
		page_allocator_type pal(dev, 10);
		radix_table::paged::allocator<page_allocator_type> allocator(pal);

		auto lvl = allocator.create_level(0);
		lvl.set_value(0, 1000);
		CHECK(1000 == lvl.get_value(0));
		CHECK(0 == lvl.get_level());

		auto tbl = allocator.create_level(1);
		tbl.set_table(10, lvl);
		auto testlvl = tbl.get_table(10);
		CHECK_EQ(testlvl.pid(), lvl.pid());

		auto [parent, id] = lvl.get_parent();

		CHECK(parent.pid() == tbl.pid());
		CHECK(id == 10);

		CHECK(tbl.holds_table(10));
		CHECK(!tbl.holds_value(10));
		CHECK(!tbl.holds_value(0));

		CHECK(lvl.holds_value(0));
		CHECK(!lvl.holds_table(0));
		CHECK(!lvl.holds_value(10));

		CHECK(lvl.size() == 1);
		lvl.remove(0);
		CHECK(lvl.size() == 0);

		{
			auto [tbl_parent, tbl_id] = tbl.get_parent();
			CHECK(!tbl_parent.is_valid());
		}
	}

	TEST_CASE("paged/model") {
		device_type dev(4 * 1024);
		page_allocator_type pal(dev, 10);
		using model_type = radix_table::paged::model<page_allocator_type>;
		using page_trie_type = radix_table::trie<std::uint32_t, model_type>;
		using page_test_map_type = std::map<std::uint32_t, std::uint32_t>;

		page_trie_type trie(pal);
		page_test_map_type tests;

		for (int i = 0; i < MAXIMUM_VALUES; ++i) {
			auto value = get_random_uint(5, 20);
			tests.emplace(i, value);
			trie.set(i, value);
		}

		for (int i = 0; i < MAXIMUM_VALUES; ++i) {
			auto val = trie.get(i);
			CHECK(tests[i] == val);
		}

		CHECK(!trie.has(MAXIMUM_VALUES + 1));
		trie.remove(0);
		CHECK(trie.get(0) == 0);

	}

	TEST_CASE("paged/model/removing") {
		device_type dev(4 * 1024);
		test_page_allocator pal(dev, 10);
		using model_type = radix_table::paged::model<page_allocator_type>;
		using page_trie_type = radix_table::trie<std::uint32_t, model_type>;
		using page_test_map_type = std::map<std::uint32_t, std::uint32_t>;

		page_trie_type trie(pal);
		page_test_map_type tests;

		for (int i = 0; i < MAXIMUM_VALUES; ++i) {
			auto value = get_random_uint(5, 20);
			tests.emplace(i, value);
			trie.set(i, value);
		}

		for (int i = 0; i < MAXIMUM_VALUES; ++i) {
			auto val = trie.get(i);
			CHECK(tests[i] == val);
			trie.remove(i);
		}

		for (int i = 0; i < MAXIMUM_VALUES + 10; ++i) {
			CHECK(!trie.has(i));
		}

		std::cout << std::format("0..ffff. Allocated: {}; Destroyed: {}\n", pal.allocated, pal.destoyed);
	}

	TEST_CASE("paged/model/random") {
		device_type dev(4 * 1024);
		test_page_allocator pal(dev, 10);
		using model_type = radix_table::paged::model<page_allocator_type>;
		using page_trie_type = radix_table::trie<std::uint32_t, model_type>;
		using page_test_map_type = std::map<std::uint32_t, std::uint32_t>;

		page_trie_type trie(pal);
		page_test_map_type tests;

		for (int i = 0; i < MAXIMUM_VALUES; ++i) {
			auto k = get_random_uint(0, 0xFFFFFFFF);
			auto value = get_random_uint(5, 20);
			if (!tests.contains(k)) {
				tests.emplace(k, value);
				trie.set(k, value);
			}
		}

		for (auto& v : tests) {
			auto val = trie.get(v.first);
			CHECK(v.second == val);
			trie.remove(v.first);
		}

		std::cout << std::format("random. values: {}; Allocated: {}; Destroyed: {}\n", tests.size(), pal.allocated, pal.destoyed);
	}

}

