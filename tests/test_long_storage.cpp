#include <filesystem>
#include <vector>
#include <map>

#include "tests.hpp"

#include "fulla/bpt/paged/model.hpp"
#include "fulla/storage/memory_device.hpp"

#include "fulla/page/header.hpp"
#include "fulla/page/long_store.hpp"
#include "fulla/long_store/handle.hpp"

#include "fulla/codec/prop.hpp"

namespace {
	using namespace fulla;
	using fulla::core::byte_buffer;
	using fulla::core::byte_view;
	using fulla::core::byte_span;
	using fulla::core::byte;

	using namespace fulla::storage;
	using namespace fulla::bpt;
	using namespace fulla::codec;

	std::filesystem::path temp_file(const char* stem) {
		namespace fs = std::filesystem;
		static std::random_device rd;
		auto p = fs::temp_directory_path() / (std::string(stem) + "_" + std::to_string(rd()) + ".bin");
		return p;
	}
	
	std::size_t get_random_value(std::size_t min, std::size_t max) {
		static std::random_device rd;
		static std::mt19937 gen(rd());
		std::uniform_int_distribution<std::size_t> len_dist(min, max);

		return len_dist(gen);
	}

	core::byte_view get_view(const std::string& str, std::size_t from, std::size_t len) {
		const auto begin = str.data() + from;
		const auto end = std::min(len, str.size() - from);
		return { reinterpret_cast<const core::byte*>(begin), end };
	}

	core::byte_span get_span(std::string& str, std::size_t from, std::size_t len) {
		const auto begin = str.data() + from;
		const auto end = std::min(len, str.size() - from);
		return { reinterpret_cast<core::byte*>(begin), end };
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

	const auto to_cbyte_ptr(const std::string& str) {
		return reinterpret_cast<const core::byte*>(str.data());
	}

	auto to_byte_ptr(std::string& str) {
		return reinterpret_cast<core::byte*>(str.data());
	}
	
	template <typename ConT1, typename ConT2>
	bool compare(const ConT1& a, const ConT2& b) {
		return std::is_eq(std::lexicographical_compare_three_way(
			std::begin(a), std::end(a),
			std::begin(b), std::end(b), 
			[](auto lhs, auto rhs) { 
				return core::byte(lhs) <=> core::byte(rhs); 
			}
		));
	}

	constexpr static const auto DEFAULT_BUFFER_SIZE = 4096UL;

}

TEST_SUITE("long_store in work") {

	using device_type = storage::memory_device;
	using pid_type = std::uint32_t;
	using buffer_manager_type = buffer_manager<device_type, pid_type>;
	using long_store_handle = fulla::long_store::handle<device_type, pid_type>;

	TEST_CASE("long_store handle basic operations") {

		device_type dev{ DEFAULT_BUFFER_SIZE };
		buffer_manager_type buf_mgr{ dev, 16 };
		long_store_handle lsh{ buf_mgr, long_store_handle::invalid_pid };
		REQUIRE(!lsh.is_open());
		REQUIRE(lsh.size() == 0);

		const bool created = lsh.create();
		CHECK(created);

		REQUIRE(lsh.is_open());
		REQUIRE(lsh.size() == 0);

		const std::string test_data = get_random_string(4500, 5000);
		auto itr = lsh.expand_to(test_data.size());
		CHECK(itr.is_valid());
	
		const std::size_t write_len0 = lsh.write(to_cbyte_ptr(test_data), test_data.size());

		lsh.dump_pages();
		const std::size_t write_len1 = lsh.write(to_cbyte_ptr(test_data), test_data.size());

		lsh.dump_pages();
		CHECK_EQ(write_len0, write_len1);
		CHECK(write_len0 == test_data.size());
		CHECK(lsh.size() == test_data.size() * 2);

		std::string result = std::string(write_len0 * 2, '\0');
		auto total_read = lsh.read(reinterpret_cast<core::byte*>(result.data()), result.size());
		CHECK_EQ(total_read, write_len0 * 2);

		// Further tests would go here to test long store functionality
	}

	TEST_CASE("double handle, single buffer") {
		device_type dev{ DEFAULT_BUFFER_SIZE };

		buffer_manager_type buf_mgr{ dev, 4 };
		long_store_handle lsh0{ buf_mgr, long_store_handle::invalid_pid };
		long_store_handle lsh1{ buf_mgr, long_store_handle::invalid_pid };

		REQUIRE(!lsh0.is_open());
		REQUIRE(lsh0.size() == 0);
		REQUIRE(!lsh1.is_open());
		REQUIRE(lsh1.size() == 0);
		const bool created0 = lsh0.create();
		const bool created1 = lsh1.create();
		CHECK(created0);
		CHECK(created1);

		auto test_data_0 = get_random_string(10000, 20000);
		auto test_data_1 = get_random_string(5000, 6000);

		const auto write0 = lsh0.write(to_cbyte_ptr(test_data_0), test_data_0.size());
		const auto write1 = lsh1.write(to_cbyte_ptr(test_data_1), test_data_1.size());

		REQUIRE(write0 == test_data_0.size());
		REQUIRE(write1 == test_data_1.size());

		auto test_result_0 = std::string(write0, '\0');
		auto test_result_1 = std::string(write1, '\0');

		const auto read0 = lsh0.read(to_byte_ptr(test_result_0), test_result_0.size());
		const auto read1 = lsh1.read(to_byte_ptr(test_result_1), test_result_1.size());

		CHECK_EQ(read0, write0);
		CHECK_EQ(read1, write1);

		buf_mgr.flush_all();

		CHECK_EQ(test_result_0, test_data_0);
		CHECK_EQ(test_result_1, test_data_1);

		auto old_pos0 = lsh0.tellg();
		auto old_pos1 = lsh1.tellg();

		lsh0.seekg(0);
		lsh1.seekg(0);

		auto new_string_0 = get_random_string(8000, 12000);
		auto new_string_1 = get_random_string(3000, 4000);
	
		const auto write2 = lsh0.write(to_cbyte_ptr(new_string_0), new_string_0.size());
		const auto write3 = lsh1.write(to_cbyte_ptr(new_string_1), new_string_1.size());

		CHECK_EQ(write2, new_string_0.size());
		CHECK_EQ(write3, new_string_1.size());

		auto test_result_2 = std::string(test_data_0.size(), '\0');
		auto test_result_3 = std::string(test_data_1.size(), '\0');

		auto read2 = lsh0.read(to_byte_ptr(test_result_2), test_result_2.size());
		auto read3 = lsh1.read(to_byte_ptr(test_result_3), test_result_3.size());

		CHECK_EQ(read2, write0);
		CHECK_EQ(read3, write1);

		CHECK_EQ(test_result_2, test_data_0);
		CHECK_EQ(test_result_3, test_data_1);

		lsh0.seekg(old_pos0);
		lsh1.seekg(old_pos1);

		test_result_2.resize(write2);
		test_result_3.resize(write3);

		read2 = lsh0.read(to_byte_ptr(test_result_2), test_result_2.size());
		read3 = lsh1.read(to_byte_ptr(test_result_3), test_result_3.size());

		CHECK_EQ(read2, write2);
		CHECK_EQ(read3, write3);

		CHECK_EQ(test_result_2, new_string_0);
		CHECK_EQ(test_result_3, new_string_1);

		CHECK(lsh1.read(to_byte_ptr(test_result_3), test_result_3.size()) == 0);
	}

	TEST_CASE("read and write from ranbom position") {
		device_type dev{ 256 };

		buffer_manager_type buf_mgr{ dev, 4 };
		long_store_handle lsh{ buf_mgr, long_store_handle::invalid_pid };
		REQUIRE(lsh.create());

		auto long_random_string = get_random_string(50000, 100000);

		REQUIRE(lsh.write(to_cbyte_ptr(long_random_string), long_random_string.size()) == long_random_string.size());

		for (int i = 0; i < 100; ++i) {
			const auto pos = get_random_value(0, long_random_string.size());
			const auto len = get_random_value(0, long_random_string.size());
			const auto view = get_view(long_random_string, pos, len);
			std::string res(len, '\0');
			lsh.seekg(pos);
			const auto res_len = lsh.read(to_byte_ptr(res), len);
			REQUIRE(res_len <= len);
			res.resize(res_len);
			CHECK(compare(res, view));
		}

		const auto check_data = [&](const std::string& expected) {
			auto tmp = std::string(expected.size(), '\0');
			lsh.seekg(0);
			CHECK(lsh.read(to_byte_ptr(tmp), tmp.size()) == tmp.size());
			CHECK(tmp == expected);
		};

		auto expected = long_random_string;

		for (int i = 0; i < 100; ++i) {
			const auto pos = get_random_value(0, expected.size());
			const auto len = get_random_value(0, expected.size());
			const auto span = get_span(expected, pos, len);
			std::string res(len, '\0');

			const auto tmp = get_random_string(span.size(), span.size());
			std::memcpy(span.data(), tmp.data(), span.size());
			lsh.seekp(pos);
			REQUIRE(lsh.write(to_cbyte_ptr(tmp.data()), tmp.size()) == span.size());
			check_data(expected);
		}

		CHECK(lsh.size() == long_random_string.size());
	}
}
