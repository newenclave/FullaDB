// tests/test_data_view.cpp
#include "tests.hpp"

#include <fulla/core/bytes.hpp>
#include <fulla/codec/serializer.hpp>
#include <fulla/codec/prop.hpp>
#include <fulla/codec/data_serializer.hpp>
#include <fulla/codec/data_view.hpp>

#include <array>
#include <string>
#include <vector>
#include <cstdint>

using namespace fulla::core;
using namespace fulla::codec;

static byte_buffer make_record_string(const std::string& s) {
    data_serializer ds;
    ds.store(s);
    byte_buffer out(ds.data(), ds.data() + ds.size());
    return out;
}

static byte_buffer make_record_u32(std::uint32_t v) {
    data_serializer ds;
    ds.store(v);
    return byte_buffer(ds.data(), ds.data() + ds.size());
}

static byte_buffer make_record_i32(std::int32_t v) {
    data_serializer ds;
    ds.store(v);
    return byte_buffer(ds.data(), ds.data() + ds.size());
}

static byte_buffer make_record_u64(std::uint64_t v) {
    data_serializer ds;
    ds.store(v);
    return byte_buffer(ds.data(), ds.data() + ds.size());
}

static byte_buffer make_record_blob(const std::uint8_t* p, std::size_t n) {
    data_serializer ds;
    ds.store_blob(reinterpret_cast<const byte*>(p), n, data_type::blob);
    return byte_buffer(ds.data(), ds.data() + ds.size());
}

static byte_buffer make_record_tuple(std::initializer_list<byte_buffer> items) {
    // tuple payload: u32(total_len_with_u32) + concat(items...)
    data_serializer tmp;
    for (auto& rec : items) {
        tmp.append(rec.data(), rec.size());
    }
    const std::uint32_t total = static_cast<std::uint32_t>(sizeof(std::uint32_t) + tmp.size());

    byte_buffer buf;
    buf.resize(sizeof(serialized_data_header) + sizeof(std::uint32_t) + tmp.size());
    auto* hdr = reinterpret_cast<serialized_data_header*>(buf.data());
    hdr->type = static_cast<std::uint16_t>(data_type::tuple);
    serializer<std::uint32_t>::store(total, hdr->data());
    std::memcpy(hdr->data() + sizeof(std::uint32_t), tmp.data(), tmp.size());
    return buf;
}

TEST_SUITE("codec:data_view") {

    TEST_CASE("compare: integers (i32/ui64)") {
        auto a = make_record_i32(-10);
        auto b = make_record_i32(42);
        CHECK(std::is_lt(data_view::compare(a, b)));
        CHECK(std::is_gt(data_view::compare(b, a)));

        auto u1 = make_record_u64(100);
        auto u2 = make_record_u64(100);
        CHECK(data_view::compare(u1, u2) == std::partial_ordering::equivalent);
    }

    TEST_CASE("compare: strings lexicographical") {
        auto a = make_record_string("abc");
        auto b = make_record_string("abd");
        auto c = make_record_string("abc");

        CHECK(std::is_lt(data_view::compare(a, b)));;
        CHECK(std::is_gt(data_view::compare(b, a)));;
        CHECK(data_view::compare(a, c) == std::partial_ordering::equivalent);
    }

    TEST_CASE("compare: blob byte-wise (no null terminator)") {
        std::uint8_t r1[3]{ 1,2,3 };
        std::uint8_t r2[3]{ 1,2,4 };
        auto a = make_record_blob(r1, 3);
        auto b = make_record_blob(r2, 3);
        CHECK(std::is_lt(data_view::compare(a, b)));;
        CHECK(std::is_gt(data_view::compare(b, a)));;
    }

    TEST_CASE("get_size(): matches serialized length for each record") {
        data_serializer ds;
        ds.store(std::string("hey")).store(std::uint32_t(123));

        byte_view view{ ds.data(), ds.size() };
        // 1) string
        {
            const auto sz = data_view::get_size(view);
            CHECK(sz > sizeof(serialized_data_header));
            view = view.subspan(sz);
        }
        // 2) ui32
        {
            const auto sz = data_view::get_size(view);
            CHECK(sz == sizeof(serialized_data_header) + sizeof(std::uint32_t));
            view = view.subspan(sz);
        }
        CHECK(view.size() == 0);
    }

    TEST_CASE("tuple: lexicographic compare of elements") {
        auto i1 = make_record_i32(10);
        auto s1 = make_record_string("zzz");
        auto t1 = make_record_tuple({ i1, s1 }); // (10, "zzz")

        auto i2 = make_record_i32(10);
        auto s2 = make_record_string("zzzz");
        auto t2 = make_record_tuple({ i2, s2 }); // (10, "zzzz")

        CHECK(std::is_lt(data_view::compare(t1, t2)));
        CHECK(std::is_gt(data_view::compare(t2, t1)));

        auto t3 = make_record_tuple({ i1, s1 });
        CHECK(data_view::compare(t1, t3) == std::partial_ordering::equivalent);
    }

    TEST_CASE("tuple: iteration via for_each_in_tuple") {
        // tuple ( "a", 1u, "b" )
        auto sA = make_record_string("a");
        auto u1 = make_record_u32(1);
        auto sB = make_record_string("b");
        auto tup = make_record_tuple({ sA, u1, sB });

        byte_view rec{ tup.data(), tup.size() };

        const auto sz = data_view::get_size(rec);
        CHECK(sz == tup.size());
        auto* hdr = reinterpret_cast<const serialized_data_header*>(rec.data());
        CHECK(static_cast<data_type>(static_cast<std::uint16_t>(hdr->type)) == data_type::tuple);

        auto [total, lsz] = serializer<std::uint32_t>::load(hdr->data(), rec.size() - sizeof(serialized_data_header));
        byte_view payload = byte_view{ hdr->data(), total }.subspan(lsz, total - lsz);

        std::vector<data_type> types;
        std::vector<std::size_t> sizes;

        bool ok = true;
        {
            byte_view rest = payload;
            while (rest.size() != 0) {
                REQUIRE(rest.size() >= sizeof(serialized_data_header));
                const auto cur_sz = data_view::get_size(rest);
                REQUIRE(cur_sz != 0);
                REQUIRE(cur_sz <= rest.size());

                const auto* h = reinterpret_cast<const serialized_data_header*>(rest.data());
                types.push_back(static_cast<data_type>(static_cast<std::uint16_t>(h->type)));
                sizes.push_back(cur_sz);

                rest = rest.subspan(cur_sz);
            }
        }

        CHECK(ok);
        CHECK(types.size() == 3);
        CHECK(types[0] == data_type::string);
        CHECK(types[1] == data_type::ui32);
        CHECK(types[2] == data_type::string);
        CHECK(sizes[1] == sizeof(serialized_data_header) + sizeof(std::uint32_t));
    }
}
