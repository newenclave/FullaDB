#include "tests.hpp"

#include "fulla/codec/prop.hpp"
#include "fulla/codec/data_view.hpp"

#include "fulla/page/bpt_inode.hpp"
#include "fulla/page/bpt_leaf.hpp"
#include "fulla/page/header.hpp"
#include "fulla/page/page_view.hpp"
#include "fulla/slots/directory.hpp"
#include "fulla/page/ranges.hpp"

namespace {

    using namespace fulla;
    using namespace fulla::codec::prop;

    using slot_dir_type = slots::variadic_directory_view<>;
    using page_view_type = page::page_view<slot_dir_type>;
    core::byte_buffer make_inode_page(std::size_t size) {
        core::byte_buffer result(size);
        page::page_view<slot_dir_type> pv(result);
        pv.header().init(static_cast<std::uint16_t>(page::page_kind::bpt_inode), size, 1, sizeof(page::bpt_inode_header));
        pv.subheader<page::bpt_inode_header>()->init();
        pv.get_slots_dir().init();
        return result;
    }

    struct inode_key_slot_extractor {
        core::byte_view operator ()(core::byte_view val) const {
            return extract_key(val);
        }
        core::byte_view extract_key(core::byte_view val) const {
            const auto slot_header = reinterpret_cast<const page::bpt_inode_slot *>(val.data());
            return { val.begin() + slot_header->key_offset(), val.end() };
        }
    };

    template <typename ...Args> 
    core::byte_buffer make_key(Args&&...args) {
        auto res = codec::prop::make_record(std::forward<Args>(args)...);
        core::byte_buffer buf{ sizeof(page::bpt_inode_slot) };
        buf.insert(buf.end(), res.view().begin(), res.view().end());
        return buf;
    }
}

TEST_SUITE("fulla/page/bpt_inode") {
    TEST_CASE("creating page with inode subheader") {
        auto inode = make_inode_page(4096);
        page_view_type pv(inode);
        const inode_key_slot_extractor IKE{};

        auto slots_dir = pv.get_slots_dir();
        REQUIRE(slots_dir.size() == 0);

        auto key1 = make_key(ui32{ 1 }, str{ "Hello" });
        auto key2 = make_key(ui32{ 2 }, str{ "very long string" });
        auto key3 = make_key(ui32{ 3 }, str{ "" });

        CHECK(slots_dir.insert(0, { key1 }));
        CHECK(slots_dir.insert(1, { key2 }));
        CHECK(slots_dir.insert(2, { key3 }));

        auto proj = page::make_slot_projection_with_extracor<inode_key_slot_extractor>(pv);
        auto less = page::make_record_less();

        const auto key_to_search = make_record(ui32{ 1 }, str{ "Hello" });
        auto it = std::ranges::lower_bound(slots_dir.view(), key_to_search.view(), less, proj);

        REQUIRE(it != slots_dir.view().end());

        CHECK(fulla::codec::data_view::compare(IKE(slots_dir.get_slot(*it)), IKE({ key1 })) == std::partial_ordering::equivalent);

    }
}

