// tests/test_prop.cpp
#include "tests.hpp"

#include "fulla/core/bytes.hpp"
#include "fulla/codec/serializer.hpp"
#include "fulla/codec/data_serializer.hpp"
#include "fulla/codec/prop_types.hpp"
#include "fulla/codec/data_view.hpp"
#include "fulla/codec/prop.hpp"

#include <compare>
#include <vector>

using namespace fulla::core;
using namespace fulla::codec;
using namespace fulla::codec::prop;

TEST_SUITE("codec: prop") {

    TEST_CASE("make_record: single values") {
        auto r1 = make_record(ui32{ 123 });
        auto r2 = make_record(str{ "abc" });
        auto r3 = make_record(fp64{ 3.5 });

        CHECK(r1.view().size() > 0);
        CHECK(r2.view().size() > 0);
        CHECK(r3.view().size() > 0);

        // Type ordering: string vs ui32 should compare by type id
        auto ord = data_view::compare(r2.view(), r1.view());
        bool ok = (ord == std::partial_ordering::less) ||
            (ord == std::partial_ordering::unordered);
        CHECK(ok);
    }

    TEST_CASE("make_record: sequence of values") {
        auto rec1 = tuple{ str{ "id" }, ui32{ 42 }, fp32{ 1.0f } };
        auto rec2 = tuple{ str{ "id" }, ui32{ 43 }, fp32{ 1.0f } };

        CHECK(std::is_lt(data_view::compare(rec1.view(), rec2.view())));
        CHECK(std::is_gt(data_view::compare(rec2.view(), rec1.view())));
    }

    TEST_CASE("make_record: sequence of values (concat records)") {
        using namespace fulla::codec::prop;
        auto rec1 = make_record(str{ "id" }, ui32{ 42 }, fp32{ 1.0f });
        auto rec2 = make_record(str{ "id" }, ui32{ 43 }, fp32{ 1.0f });

        CHECK(std::is_lt(data_view::compare_sequence(rec1.view(), rec2.view())));
        CHECK(std::is_gt(data_view::compare_sequence(rec2.view(), rec1.view())));
    }

    TEST_CASE("tuple: embedded, then record around it") {
        tuple t{ i32{10}, str{"aaa"}, ui64{5} };
        auto rec1 = tuple{ str{ "key" }, t };
        auto rec2 = tuple{ str{ "key" }, tuple{ i32{10}, str{"aab"}, ui64{5} } };

        // compare goes into tuple lexicographically (second element differs)
        CHECK(std::is_lt(data_view::compare(rec1.view(), rec2.view())));
    }

    TEST_CASE("blob: byte-wise compare and size accounting") {
        std::uint8_t a[4]{ 1,2,3,4 };
        std::uint8_t b[4]{ 1,2,3,5 };

        auto r1 = make_record(blob{ byte_view{ reinterpret_cast<const byte*>(a), 4 } });
        auto r2 = make_record(blob{ byte_view{ reinterpret_cast<const byte*>(b), 4 } });

        CHECK(std::is_lt(data_view::compare(r1.view(), r2.view())));

        // size sanity with header
        CHECK(data_view::get_size(r1.view()) == r1.view().size());
    }
}
