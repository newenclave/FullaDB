#include <filesystem>
#include <vector>
#include <map>

#include "tests.hpp"

#include "fulla/bpt/paged/model.hpp"
#include "fulla/storage/file_device.hpp"
#include "fulla/storage/memory_device.hpp"

#include "fulla/page/header.hpp"
#include "fulla/page/bpt_inode.hpp"
#include "fulla/page/bpt_leaf.hpp"
#include "fulla/page/bpt_root.hpp"
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

	std::filesystem::path temp_file(const char* stem) {
		namespace fs = std::filesystem;
		static std::random_device rd;
		auto p = fs::temp_directory_path() / (std::string(stem) + "_" + std::to_string(rd()) + ".bin");
		return p;
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

