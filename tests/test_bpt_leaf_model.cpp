#include <map>
#include <ranges>
#include <algorithm>

#include "tests.hpp"

#include "fulla/bpt/paged/model.hpp"
#include "fulla/storage/file_device.hpp"
#include "fulla/page/header.hpp"
#include "fulla/codec/prop.hpp"
#include "fulla/page/bpt_leaf.hpp"

namespace {

	using namespace fulla;

	using fulla::core::byte_buffer;
	using fulla::core::byte_view;
	using fulla::core::byte_span;
	using fulla::core::byte;

	using file_device = fulla::storage::file_device;

	using namespace fulla::bpt;
	using model_type = paged::model<file_device>;
	using page_header_type = fulla::page::page_header;
	using page_view_type = typename model_type::page_view_type;

	using key_like_type = model_type::key_like_type;
	using key_out_type = model_type::key_out_type;

	using value_in_type = model_type::value_in_type;
	using value_out_type = model_type::value_out_type;

	using node_id_type = model_type::node_id_type;

	using namespace fulla::codec;

	byte_buffer make_leaf_page(std::size_t size) {
		byte_buffer result(size);
		auto pv = page_view_type{ result };
		pv.header().init(fulla::page::page_kind::bpt_leaf, size, 1, sizeof(fulla::page::bpt_leaf_header));
		auto sh = pv.subheader<fulla::page::bpt_leaf_header>();
		sh->init();
		sh->next = sh->prev = model_type::invalid_node_value;
		pv.get_slots_dir().init();
		return result;
	}

	void validate_keys(model_type::inode_type inode) {
		std::optional<key_out_type> last;
		auto less_type = fulla::page::make_record_less();
		for (std::size_t i = 0; i < inode.size(); ++i) {
			if (last.has_value()) {
				CHECK(less_type(last->key, inode.get_key(i).key));
			}
			last = inode.get_key(i);
		}
	}

	value_in_type as_value_in(const std::string& val) {
		return { .val = byte_view{ reinterpret_cast<const byte*>(val.data()), val.size()}};
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
	struct leaf_key_extractor {
		byte_view operator ()(byte_view value) const noexcept {
			const auto* slot_hdr = reinterpret_cast<const page::bpt_leaf_slot*>(value.data());
			return { value.begin() + slot_hdr->key_offset(), slot_hdr->key_len };
		}
	};

	auto compare(core::byte_view a, core::byte_view b) {
		return std::lexicographical_compare_three_way(a.begin(), a.end(), b.begin(), b.end());
	}

}

TEST_SUITE("bpt/page/model/inode") {

	TEST_CASE("bpt::leaf page create") {
		auto page = make_leaf_page(4096);
		auto pv = page_view_type{ page };
		auto sh = pv.subheader<fulla::page::bpt_leaf_header>();
		REQUIRE(page.size() == 4096);
		CHECK(sh->next == model_type::invalid_node_value);
		CHECK(sh->prev == model_type::invalid_node_value);
		CHECK(pv.get_slots_dir().size() == 0);
	}

	TEST_CASE("bpt::leaf insert update value") {
		auto short_key1 = prop::make_record(prop::ui32{ 1 });
		auto short_key2 = prop::make_record(prop::ui32{ 2 });
		auto short_key3 = prop::make_record(prop::ui32{ 3 });

		auto long_key = prop::make_record(prop::ui32{ 1 }, prop::str{"1234567890qwertyuiop"});
		std::string value = "value  1234567890 !!@";

		auto page = make_leaf_page(4096);
		auto pv = page_view_type{ page };
		[[maybe_unused]] auto sh = pv.subheader<fulla::page::bpt_leaf_header>();
		model_type::leaf_type leaf(pv, 100, {});
		
		CHECK(leaf.can_insert_value(0, key_like_type{ short_key1.view() }, as_value_in(value)));
		CHECK(leaf.insert_value(0, key_like_type{ short_key1.view() }, as_value_in(value)));

		CHECK(leaf.can_insert_value(1, key_like_type{ short_key2.view() }, as_value_in(value)));
		CHECK(leaf.insert_value(1, key_like_type{ short_key2.view() }, as_value_in(value)));

		CHECK(leaf.can_insert_value(2, key_like_type{ short_key3.view() }, as_value_in(value)));
		CHECK(leaf.insert_value(2, key_like_type{ short_key3.view() }, as_value_in(value)));

		[[maybe_unused]] auto hang_value_do_not_use = leaf.get_value(1);

		CHECK(leaf.can_update_key(1, key_like_type{ long_key.view() }));
		CHECK(leaf.update_key(1, key_like_type{ long_key.view() }));
		CHECK(leaf.keys_eq(key_like_type{ long_key.view() }, key_like_type{ leaf.get_key(1).key }));

		REQUIRE(leaf.size() == 3);

		auto val1 = leaf.get_value(0);
		auto val2 = leaf.borrow_value(1);
		auto val3 = leaf.get_value(2);

		auto value_in = as_value_in(value);

		CHECK(std::is_eq(std::lexicographical_compare_three_way(value_in.val.begin(), value_in.val.end(), val1.val.begin(), val1.val.end())));
		CHECK(std::is_eq(std::lexicographical_compare_three_way(value_in.val.begin(), value_in.val.end(), val2.val.begin(), val2.val.end())));
		CHECK(std::is_eq(std::lexicographical_compare_three_way(value_in.val.begin(), value_in.val.end(), val3.val.begin(), val3.val.end())));


		SUBCASE("update value") {

			std::string new_long_value = "This is a long long value to update";

			CHECK(leaf.can_update_value(2, as_value_in(new_long_value)));
			CHECK(leaf.update_value(2, as_value_in(new_long_value)));

			auto new_value_as_in = as_value_in(new_long_value);

			val1 = leaf.get_value(0);
			val2 = leaf.borrow_value(1);
			val3 = leaf.get_value(2);

			CHECK(std::is_eq(std::lexicographical_compare_three_way(value_in.val.begin(), value_in.val.end(), val1.val.begin(), val1.val.end())));
			CHECK(std::is_eq(std::lexicographical_compare_three_way(value_in.val.begin(), value_in.val.end(), val2.val.begin(), val2.val.end())));
			CHECK(std::is_eq(std::lexicographical_compare_three_way(new_value_as_in.val.begin(), new_value_as_in.val.end(), val3.val.begin(), val3.val.end())));
		}

		SUBCASE("set/get next/prev") {
			const node_id_type new_next = 999;
			const node_id_type new_prev = 888;
			leaf.set_next(new_next);
			leaf.set_prev(new_prev);
			CHECK_EQ(leaf.get_next(), new_next);
			CHECK_EQ(leaf.get_prev(), new_prev);
		}

		SUBCASE("set/get parent") {
			const node_id_type new_value = 108090;
			leaf.set_parent(new_value);
			CHECK_EQ(new_value, leaf.get_parent());
		}
	}

	TEST_CASE("bpt::leaf insert / remove random value") {
		auto page = make_leaf_page(4096);
		auto pv = page_view_type{ page };
		auto slots = pv.get_slots_dir();
		std::map<std::string, std::string> tests;
		model_type::leaf_type leaf(pv, 100, {});

		static std::random_device rd;
		static std::mt19937 gen(rd());

		const auto check_slots = [&]() {
			for (auto& t : tests) {
				auto key = prop::make_record(prop::str{ t.first });
				auto pos = leaf.key_position(key_like_type{ key.view() });
				auto gk = leaf.get_key(pos);
				CHECK(std::is_eq(compare(gk.key, key.view())));
			}
			};

		while (1) {
			auto ts = get_random_string(20, 5);
			if (!tests.contains(ts)) {
				auto key = prop::make_record(prop::str{ ts });
				auto pos = leaf.key_position(key_like_type{ key.view() });
				if (leaf.can_insert_value(pos, key_like_type{ key.view() }, as_value_in(ts))) {
					REQUIRE(leaf.insert_value(pos, key_like_type{ key.view() }, as_value_in(ts)));
					auto gk = leaf.get_key(pos);
					CHECK(std::is_eq(compare(gk.key, key.view())));
					tests[ts] = ts;
				}
				else {
					break;
				}
				check_slots();
			}
		}

		auto size = tests.size();
		auto lsize = leaf.size();

		for (auto& t : tests) {
			auto ts = get_random_string(20, 5);
			auto key = prop::make_record(prop::str{ t.first });
			auto pos = leaf.key_position(key_like_type{ key.view() });
			if (leaf.can_update_value(pos, as_value_in(ts))) {
				REQUIRE(leaf.update_value(pos, as_value_in(ts)));
				t.second = ts;
				check_slots();
			}
		}

		check_slots();
		for (std::size_t i = 0; i < size / 2; ++i) {
			std::uniform_int_distribution<std::size_t> dist(0, tests.size() - 1);

			auto pos_itr = tests.begin();
			std::advance(pos_itr, dist(gen));

			auto key = prop::make_record(prop::str{ pos_itr->first });
			auto pos = leaf.key_position(key_like_type{ key.view() });
			auto gk = leaf.get_key(pos);

			CHECK(std::is_eq(compare(gk.key, key.view())));
			CHECK(leaf.erase(pos));
			tests.erase(pos_itr);
			slots.compact();
			check_slots();
		}

		for (auto& t : tests) {
			auto ts = get_random_string(20, 5);
			auto key = prop::make_record(prop::str{ t.first });
			auto pos = leaf.key_position(key_like_type{ key.view() });
			if (leaf.can_update_value(pos, as_value_in(ts))) {
				REQUIRE(leaf.update_value(pos, as_value_in(ts)));
				t.second = ts;
				check_slots();
			}
		}

		size = tests.size();
		lsize = leaf.size();
	}
}
