// tests/test_page_ranges.cpp
#include "tests.hpp"
#include "fulla/page/header.hpp"
#include "fulla/page/slot_page.hpp"
#include "fulla/page/ranges.hpp"
#include "fulla/codec/prop.hpp"
#include "fulla/codec/data_view.hpp"

using namespace fulla::core;
using namespace fulla::page;
using namespace fulla::codec;
using namespace fulla::codec::prop;

static std::vector<byte> make_blank_page(std::size_t ps) {
    std::vector<byte> buf(ps);
    auto* hdr = reinterpret_cast<page_header*>(buf.data());
    hdr->init(page_kind::heap, ps, 1);
    return buf;
}

TEST_SUITE("page/ranges") {
    TEST_CASE("lower_bound over slots with projection & record_less") {
        auto buf = make_blank_page(4096);
        page_view pv{buf};

        // Insert sorted keys: "a", "b", "d"
        auto ra = make_record(str{"a"});
        auto rb = make_record(str{"b"});
        auto rd = make_record(str{"d"});

        CHECK(pv.insert(ra.view()).ok);
        CHECK(pv.insert(rb.view()).ok);
        CHECK(pv.insert(rd.view()).ok);

        auto slots = pv.slot_dir_view();
        CHECK(slots.size() == 3);

        const auto av = pv.get_slot(slots[0]);
        const auto bv = pv.get_slot(slots[1]);
        const auto dv = pv.get_slot(slots[2]);

        auto proj  = make_slot_projection(pv);
        auto less  = make_record_less();

        // Search for "c" -> should point to "d"
        auto key = make_record(str{"c"});
        auto it = std::ranges::lower_bound(slots, key.view(), less, proj);
        CHECK(it != slots.end());
        CHECK(fulla::codec::data_view::compare(pv.get_slot(*it), rd.view()) == std::partial_ordering::equivalent);

        // Search for "b" -> should point to "b"
        auto key2 = make_record(str{"b"});
        auto it2 = std::ranges::lower_bound(slots, key2.view(), less, proj);
        CHECK(it2 != slots.end());
        CHECK(fulla::codec::data_view::compare(pv.get_slot(*it2), rb.view()) == std::partial_ordering::equivalent);
    }
}
