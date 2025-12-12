#include "tests.hpp"

#include "fulla/bpt/paged/model.hpp"
#include "fulla/storage/file_device.hpp"
#include "fulla/storage/file_block_device.hpp"
#include "fulla/storage/buffer_manager.hpp"
#include "fulla/page/header.hpp"
#include "fulla/page/bpt_inode.hpp"

#include "fulla/codec/prop.hpp"

namespace {
	using fulla::core::byte_buffer;
	using fulla::core::byte_view;
	using fulla::core::byte_span;

	using file_device = fulla::storage::file_block_device;
	using buffer_manager_type = fulla::storage::buffer_manager<file_device, std::uint32_t>;
	
	using namespace fulla::bpt;
	using model_type = paged::model<buffer_manager_type>;
	using page_header_type = fulla::page::page_header;
	using page_view_type = typename model_type::page_view_type;

	using key_like_type = model_type::key_like_type;
	using key_out_type = model_type::key_out_type;

	using namespace fulla::codec;

	byte_buffer make_inode_page(std::size_t size) {
		byte_buffer result(size);
		auto pv = page_view_type{ result };
		pv.header().init(static_cast<std::uint16_t>(fulla::page::page_kind::bpt_inode), size, 1, sizeof(fulla::page::bpt_inode_header));
		auto sh = pv.subheader<fulla::page::bpt_inode_header>();
		sh->init();
		sh->rightmost_child = model_type::invalid_node_value;
		pv.get_slots_dir().init();
		return result;
	}

	void validate_keys(model_type::inode_type &inode) {
		std::optional<key_out_type> last;
		auto less_type = fulla::page::make_record_less();
		for (std::size_t i = 0; i < inode.size(); ++i) {
			if (last.has_value()) {
				CHECK(less_type(last->key, inode.get_key(i).key));
			}
			last = inode.get_key(i);
		}
	}
}

TEST_SUITE("bpt/page/model/inode") {

	TEST_CASE("bpt::inode page create") {
		auto page = make_inode_page(4096);
		auto pv = page_view_type{ page };
		auto sh = pv.subheader<fulla::page::bpt_inode_header>();
		REQUIRE(page.size() == 4096);
		CHECK(sh->rightmost_child == model_type::invalid_node_value);
		CHECK(pv.get_slots_dir().size() == 0);
	}

	TEST_CASE("bpt::inode model inode_type") {
		auto page = make_inode_page(4096);
		model_type::inode_type inode(page_view_type{ page }, 100, {}, 5, 200);
		auto slots = page_view_type{ page }.get_slots_dir();
		CHECK_EQ(inode.self(), 100);
		auto capacity = inode.capacity();

		CHECK(capacity > 0);
		CHECK(capacity > inode.size());
		CHECK(inode.update_child(0, 100));
		CHECK_EQ(inode.get_child(0), 100);

		CHECK(inode.insert_child(0, key_like_type{ prop::make_record(prop::str{ "aaaaaa 1" }, prop::ui32{ 1 }).view() }, 200));
		CHECK(inode.insert_child(1, key_like_type{ prop::make_record(prop::str{ "aaaaaa 2" }, prop::ui32{ 2 }).view() }, 300));
		CHECK(inode.insert_child(2, key_like_type{ prop::make_record(prop::str{ "aaaaaa 3" }, prop::ui32{ 3 }).view() }, 400));

		for (auto i = 0; i < 400; i++) {
			std::string str_val = "bbbbbb " + std::to_string(999 - i);
			auto res = prop::make_record(prop::str{ str_val }, prop::ui32{ (std::uint32_t)i });
			auto kp = inode.key_position(key_like_type{res.view()});
			CHECK(kp == 3);
			if (!inode.insert_child(kp, key_like_type{ res.view() }, i)) {
				break;
			}
		}
		CHECK(inode.size() >= inode.capacity());
		[[maybe_unused]] auto size = inode.size();
		auto short_value = prop::make_record(prop::str{ "ccccccc" }, prop::ui32{ 0xFFFFFFFF });
		auto kt = inode.key_position(key_like_type{ short_value.view() });

		CHECK(inode.can_insert_child(kt, key_like_type{ short_value.view() }, 500));
		CHECK(inode.insert_child(kt, key_like_type{ short_value.view() }, 500));
		CHECK(inode.is_full());
		[[maybe_unused]] auto ava = slots.available();

		validate_keys(inode);

		CHECK(inode.get_child(inode.size()) == 100);

		inode.set_parent(666777);
		CHECK_EQ(inode.get_parent(), 666777);

		kt = inode.key_position(key_like_type{ short_value.view() }); // upper_bound
		CHECK(inode.keys_eq(key_like_type{ short_value.view() }, key_like_type{ inode.get_key(kt - 1).key }));
		CHECK(inode.keys_eq(key_like_type{ short_value.view() }, key_like_type{ short_value.view() }));
	}

	TEST_CASE("bpt::inode model inode_type underflow") {
		auto page = make_inode_page(4096);
		model_type::inode_type inode(page_view_type{ page }, 100, {});
		auto slots = page_view_type{ page }.get_slots_dir();

		for (auto i = 0; i < 400; i++) {
			std::string str_val = "bbbbbb " + std::to_string(i);
			auto res = prop::make_record(prop::str{ str_val }, prop::ui32{ (std::uint32_t)i });
			auto kp = inode.key_position(key_like_type{ res.view() });
			if (!inode.insert_child(kp, key_like_type{ res.view() }, i)) {
				break;
			}
		}
		validate_keys(inode);
	
		while (!inode.is_underflow()) {
			inode.erase(inode.size() / 2);
		}

		auto bk = inode.borrow_key(0);
		CHECK(inode.keys_eq(key_like_type{ bk.key }, key_like_type{ inode.get_key(0).key }));

		auto test_key = prop::make_record(prop::str{ "!!!updated_key_logn_long_value really long value" }, prop::ui32{(std::uint32_t)777});

		CHECK(inode.size() > 0);
		CHECK(slots.available() > slots.stored_size());

		CHECK(inode.update_key(0, key_like_type{ test_key.view() }));
		CHECK(inode.keys_eq(key_like_type{ test_key.view() }, key_like_type{ inode.get_key(0).key }));

		auto zero_key = inode.get_key(0).key;
		CHECK( std::is_eq(std::lexicographical_compare_three_way(
			test_key.view().begin(), 
			test_key.view().end(), 
			zero_key.begin(), 
			zero_key.end())));
	}
}
