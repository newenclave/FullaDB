// tests/test_page_ranges.cpp
#include "tests.hpp"
#include "fulla/page/header.hpp"
#include "fulla/page/page_view.hpp"
#include "fulla/page/ranges.hpp"
#include "fulla/slots/directory.hpp"
#include "fulla/codec/prop.hpp"
#include "fulla/codec/data_view.hpp"
#include "fulla/slots/directory.hpp"

using namespace fulla::core;
using namespace fulla::page;
using namespace fulla::codec;
using namespace fulla::codec::prop;
using namespace fulla::slots;

namespace {
    static std::vector<byte> make_blank_page(std::size_t ps, std::size_t subhdr_size = 4) {
        std::vector<byte> buf(ps);
        auto* hdr = reinterpret_cast<page_header*>(buf.data());
        hdr->init(static_cast<std::uint16_t>(page_kind::heap), static_cast<std::uint32_t>(ps), 1, subhdr_size);
        return buf;
    }

    struct key_container {
        byte_buffer buf{10};
        byte_view view() const {
            return byte_view{buf};
        }
    };

    template <typename ...Args>
    key_container make_key_with_header(Args &&...args) {
        auto res = make_record(std::forward<Args>(args)...);
        key_container kc;
        kc.buf.insert(kc.buf.end(), res.view().begin(), res.view().end());
        return kc;
    }

    struct container_extrctor {
        auto operator ()(byte_view val) const {
            return extract_key(val);
        }
        byte_view extract_key(byte_view val) const {
            return val.subspan(10);
        }
    };
}

TEST_SUITE("page/ranges") {

    TEST_CASE("check view data") {
        using slot_dir_type = variadic_directory_view<>;
        auto buf = make_blank_page(500, 12);
        page_view<slot_dir_type> pv{ buf };

        const auto expected_base = static_cast<std::uint16_t>(sizeof(page_header) + 12);
        const auto expected_cap = buf.size() - expected_base;
        CHECK(pv.base_off() == expected_base);
        CHECK(pv.base_ptr() == buf.data() + expected_base);
        CHECK(pv.capacity() == expected_cap);
        
        auto slots = pv.get_slots_dir();
        slots.init();
        CHECK(slots.available() == (pv.capacity() - sizeof(slot_dir_type::directory_header)));
    }

    TEST_CASE("lower_bound over slots with projection & record_less") {
        using slot_dir_type = variadic_directory_view<>;
        auto buf = make_blank_page(4096);
        page_view<slot_dir_type> pv{buf};
        pv.get_slots_dir().init();

        // Insert sorted keys: "a", "b", "d"
        auto ra = make_record(str{"a"});
        auto rb = make_record(str{"b"});
        auto rd = make_record(str{"d"});

        auto slots_dir = pv.get_slots_dir();

        CHECK(slots_dir.insert(slots_dir.size(), ra.view()));
        CHECK(slots_dir.insert(slots_dir.size(), rb.view()));
        CHECK(slots_dir.insert(slots_dir.size(), rd.view()));

        auto slots = slots_dir.view();
        CHECK(slots.size() == 3);

        const auto av = slots_dir.get_slot(slots[0]);
        const auto bv = slots_dir.get_slot(slots[1]);
        const auto dv = slots_dir.get_slot(slots[2]);

        auto proj  = make_slot_projection(pv);
        auto less  = make_record_less();

        // Search for "c" -> should point to "d"
        auto key = make_record(str{"c"});
        auto it = std::ranges::lower_bound(slots, key.view(), less, proj);
        CHECK(it != slots.end());
        CHECK(fulla::codec::data_view::compare(slots_dir.get_slot(*it), rd.view()) == std::partial_ordering::equivalent);

        // Search for "b" -> should point to "b"
        auto key2 = make_record(str{"b"});
        auto it2 = std::ranges::lower_bound(slots, key2.view(), less, proj);
        CHECK(it2 != slots.end());
        CHECK(fulla::codec::data_view::compare(slots_dir.get_slot(*it2), rb.view()) == std::partial_ordering::equivalent);
    }

    TEST_CASE("lower_bound over slots with projection & record_less & custom key format") {
        using slot_dir_type = variadic_directory_view<>;
        const container_extrctor CE{};
        auto buf = make_blank_page(4096);
        page_view<slot_dir_type> pv{buf};
        pv.get_slots_dir().init();

        auto ra = make_key_with_header(str{"a"});
        auto rb = make_key_with_header(str{"b"});
        auto rd = make_key_with_header(str{"d"});

        auto slots_dir = pv.get_slots_dir();

        CHECK(slots_dir.insert(slots_dir.size(), ra.view()));
        CHECK(slots_dir.insert(slots_dir.size(), rb.view()));
        CHECK(slots_dir.insert(slots_dir.size(), rd.view()));

        auto slots = slots_dir.view();
        CHECK(slots.size() == 3);

        auto proj  = make_slot_projection_with_extracor<container_extrctor>(pv);
        auto less  = make_record_less();

        auto key = make_record(str{"c"});
        auto it = std::ranges::lower_bound(slots, key.view(), less, proj);
        CHECK(it != slots.end());

        CHECK(fulla::codec::data_view::compare(CE(slots_dir.get_slot(*it)), CE(rd.view())) == std::partial_ordering::equivalent);

        auto key2 = make_record(str{ "b" });
        auto it2 = std::ranges::lower_bound(slots, key2.view(), less, proj);
        CHECK(it2 != slots.end());
        CHECK(fulla::codec::data_view::compare(CE(slots_dir.get_slot(*it2)), CE(rb.view())) == std::partial_ordering::equivalent);

    }
}
