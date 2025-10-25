// tests/test_slot_page.cpp
#include "tests.hpp"

#include "fulla/core/bytes.hpp"
#include "fulla/page/header.hpp"
#include "fulla/page/slot_page.hpp"
#include "fulla/codec/serializer.hpp"
#include "fulla/codec/data_serializer.hpp"
#include "fulla/codec/prop.hpp"
#include "fulla/codec/data_view.hpp"

#include <vector>
#include <compare>

using namespace fulla::core;
using namespace fulla::page;
using namespace fulla::codec;
using namespace fulla::codec::prop;

static std::vector<byte> make_blank_page(std::size_t page_size, page_kind kind, std::uint32_t self, std::size_t subhdr = 0) {
    std::vector<byte> buf(page_size);
    auto* hdr = reinterpret_cast<page_header*>(buf.data());
    hdr->init(kind, page_size, self, subhdr);
    return buf;
}

TEST_SUITE("page/slot_page") {

    TEST_CASE("init + empty validate") {
        auto buf = make_blank_page(4096, page_kind::heap, 123);
        page_view pv{ buf };
        CHECK(pv.validate());
        CHECK(pv.slot_count() == 0);
        CHECK(pv.free_beg() == sizeof(page_header));
        CHECK(pv.free_end() == 4096);
        CHECK(pv.free_space() == 4096 - sizeof(page_header));
    }

    TEST_CASE("insert simple records") {
        auto buf = make_blank_page(4096, page_kind::heap, 1);
        page_view pv{ buf };

        auto r1 = prop::make_record(str{ "A" });
        auto r2 = prop::make_record(ui32{ 42 });
        auto r3 = prop::make_record(str{ "BBBB" });

        auto i1 = pv.insert(r1.view()); CHECK(i1.ok);
        auto i2 = pv.insert(r2.view()); CHECK(i2.ok);
        auto i3 = pv.insert(r3.view()); CHECK(i3.ok);

        CHECK(pv.slot_count() == 3);
        CHECK(pv.validate());

        // Read back
        auto v2 = pv.get_slot(i2.slot_id);
        CHECK(v2.size() == r2.view().size());
        CHECK(data_view::compare(v2, r2.view()) == std::partial_ordering::equivalent);
    }

    TEST_CASE("insert_at_dense reorders slots") {
        auto buf = make_blank_page(2048, page_kind::heap, 2);
        page_view pv{ buf };

        auto a = prop::make_record(ui32{ 1 });
        auto b = prop::make_record(ui32{ 2 });
        auto c = prop::make_record(ui32{ 3 });

        auto ia = pv.insert(a.view()); CHECK(ia.ok);
        auto ib = pv.insert(b.view()); CHECK(ib.ok);

        auto ic = pv.insert_at_dense(1, c.view()); CHECK(ic.ok);
        CHECK(ic.slot_id == 1);
        CHECK(pv.slot_count() == 3);

        // Now slots: [a, c, b]
        CHECK(data_view::compare(pv.get_slot(0), a.view()) == std::partial_ordering::equivalent);
        CHECK(data_view::compare(pv.get_slot(1), c.view()) == std::partial_ordering::equivalent);
        CHECK(data_view::compare(pv.get_slot(2), b.view()) == std::partial_ordering::equivalent);
        CHECK(pv.validate());
    }

    TEST_CASE("erase_dense shifts left") {
        auto buf = make_blank_page(2048, page_kind::heap, 3);
        page_view pv{ buf };

        auto a = prop::make_record(ui32{ 10 });
        auto b = prop::make_record(ui32{ 20 });
        auto c = prop::make_record(ui32{ 30 });

        pv.insert(a.view());
        pv.insert(b.view());
        pv.insert(c.view());
        CHECK(pv.slot_count() == 3);

        pv.erase_dense(1);
        CHECK(pv.slot_count() == 2);
        CHECK(data_view::compare(pv.get_slot(0), a.view()) == std::partial_ordering::equivalent);
        CHECK(data_view::compare(pv.get_slot(1), c.view()) == std::partial_ordering::equivalent);
        CHECK(pv.validate());
    }

    TEST_CASE("try_update_in_place vs update_dense") {
        auto buf = make_blank_page(4096, page_kind::heap, 4);
        page_view pv{ buf };

        auto s_long = prop::make_record(str{ "ABCDEFGHIJ" }); // 10 chars
        auto s_short = prop::make_record(str{ "A" });          // 1 char
        auto s_long2 = prop::make_record(str{ "ABCDEFGHIJKL" }); // 12 chars

        auto ins = pv.insert(s_long.view()); CHECK(ins.ok);

        // shrink in place should succeed (padded new <= padded old)
        CHECK(pv.try_update_in_place(ins.slot_id, s_short.view()));
        CHECK(data_view::compare(pv.get_slot(ins.slot_id), s_short.view()) == std::partial_ordering::equivalent);

        // grow beyond old padded — fallback to update_dense (append at end)
        CHECK(pv.update_dense(ins.slot_id, s_long2.view()));
        CHECK(data_view::compare(pv.get_slot(ins.slot_id), s_long2.view()) == std::partial_ordering::equivalent);
        CHECK(pv.validate());
    }

    TEST_CASE("compact and free_space_if_compacted") {
        auto buf = make_blank_page(4096, page_kind::heap, 5);
        page_view pv{ buf };

        std::vector<prop::rec> recs;
        for (int i = 0; i < 10; ++i) {
            recs.push_back(prop::make_record(ui32{ static_cast<std::uint32_t>(i) }));
            auto r = pv.insert(recs.back().view()); CHECK(r.ok);
        }
        // delete even slots to create holes
        for (int i = 0; i < 10; i += 2) {
            pv.erase_dense(static_cast<std::uint16_t>(i / 2));
        }
        CHECK(pv.validate());

        const auto fs_before = pv.free_space();
        const auto fs_can = pv.free_space_if_compacted();
        CHECK(fs_can >= fs_before);

        pv.compact();
        CHECK(pv.free_space() >= fs_before);
        CHECK(pv.validate());
    }
}
