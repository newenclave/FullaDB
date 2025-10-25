// tests/test_codec.cpp
#include "tests.hpp"

#include "fulla/core/bytes.hpp"
#include "fulla/codec/prop_types.hpp"
#include "fulla/codec/serializer.hpp"
#include "fulla/codec/data_serializer.hpp"

#include <array>
#include <string>
#include <variant>
#include <vector>
#include <cstring>

using namespace fulla::core;
using namespace fulla::codec;

TEST_SUITE("codec: prop/serializer/data_serializer") {

    TEST_CASE("serialized_data_header: layout & helpers") {
        serialized_data_header hdr{};
        CHECK(serialized_data_header::header_size() == sizeof(serialized_data_header));
        const byte* base = reinterpret_cast<const byte*>(&hdr);
        CHECK(hdr.data() == base + sizeof(serialized_data_header));
        CHECK(static_cast<serialized_data_header::word_type>(hdr.type) == 0);
    }

    TEST_CASE("serializer<string>/byte_view size & roundtrip accounting") {
        std::array<byte, 128> buf{};
        auto* p = buf.data();

        SUBCASE("string: \"abc\"") {
            const std::string s = "abc";
            const std::size_t need = serializer<std::string>::size(s); // 4 + 3 + 1 = 8
            CHECK(need == 8);
            const auto written = serializer<std::string>::store(s, p);
            CHECK(written == need);
            auto [back, used] = serializer<std::string>::load(p, written);
            CHECK(used == written);
            CHECK(back == s);
        }

        SUBCASE("byte_view: 5 bytes") {
            std::uint8_t raw[5]{ 1,2,3,4,5 };
            byte_view v{ reinterpret_cast<const byte*>(raw), 5 };
            const std::size_t need = serializer<byte_view>::size(v); // 4 + 5 = 9
            CHECK(need == 9);
            const auto written = serializer<byte_view>::store(v, p);
            CHECK(written == need);
            auto [back, used] = serializer<byte_view>::load(p, written);
            CHECK(used == written);
            CHECK(back.size() == v.size());
            for (std::size_t i = 0; i < v.size(); ++i)
                CHECK(static_cast<std::uint8_t>(back[i]) == raw[i]);
        }
    }

    TEST_CASE("data_serializer: store primitives with type tags") {
        data_serializer ds;

        const std::string s = "hello";
        const std::int32_t  i32 = -123456;
        const std::uint32_t ui32 = 0xDEADBEEF;
        const std::int64_t  i64 = -0x123456789ABCLL;
        const std::uint64_t ui64 = 0x1122334455667788ULL;
        const float  f32 = 1234.5f;
        const double f64 = -98765.125;

        ds.store(s)
            .store(i32)
            .store(ui32)
            .store(i64)
            .store(ui64)
            .store(f32)
            .store(f64);

        const byte* buf = ds.data();
        std::size_t off = 0;

        auto next_hdr = [&](const byte* base, std::size_t ofs) -> const serialized_data_header* {
            return reinterpret_cast<const serialized_data_header*>(base + ofs);
            };

        // 1) string
        {
            auto* hdr = next_hdr(buf, off);
            CHECK(static_cast<data_type>(static_cast<serialized_data_header::word_type>(hdr->type)) == data_type::string);
            auto [val, used] = serializer<std::string>::load(hdr->data(), ds.size() - off);
            CHECK(val == s);
            off += serialized_data_header::header_size() + used;
        }

        // 2) i32
        {
            auto* hdr = next_hdr(buf, off);
            CHECK(static_cast<data_type>(static_cast<serialized_data_header::word_type>(hdr->type)) == data_type::i32);
            auto [val, used] = serializer<std::int32_t>::load(hdr->data(), ds.size() - off);
            CHECK(val == i32);
            CHECK(used == sizeof(std::int32_t));
            off += serialized_data_header::header_size() + used;
        }

        // 3) ui32
        {
            auto* hdr = next_hdr(buf, off);
            CHECK(static_cast<data_type>(static_cast<serialized_data_header::word_type>(hdr->type)) == data_type::ui32);
            auto [val, used] = serializer<std::uint32_t>::load(hdr->data(), ds.size() - off);
            CHECK(val == ui32);
            CHECK(used == sizeof(std::uint32_t));
            off += serialized_data_header::header_size() + used;
        }

        // 4) i64
        {
            auto* hdr = next_hdr(buf, off);
            CHECK(static_cast<data_type>(static_cast<serialized_data_header::word_type>(hdr->type)) == data_type::i64);
            auto [val, used] = serializer<std::int64_t>::load(hdr->data(), ds.size() - off);
            CHECK(val == i64);
            CHECK(used == sizeof(std::int64_t));
            off += serialized_data_header::header_size() + used;
        }

        // 5) ui64
        {
            auto* hdr = next_hdr(buf, off);
            CHECK(static_cast<data_type>(static_cast<serialized_data_header::word_type>(hdr->type)) == data_type::ui64);
            auto [val, used] = serializer<std::uint64_t>::load(hdr->data(), ds.size() - off);
            CHECK(val == ui64);
            CHECK(used == sizeof(std::uint64_t));
            off += serialized_data_header::header_size() + used;
        }

        // 6) fp32
        {
            auto* hdr = next_hdr(buf, off);
            CHECK(static_cast<data_type>(static_cast<serialized_data_header::word_type>(hdr->type)) == data_type::fp32);
            auto [val, used] = serializer<float>::load(hdr->data(), ds.size() - off);
            CHECK(val == f32);
            CHECK(used == sizeof(std::uint32_t));
            off += serialized_data_header::header_size() + used;
        }

        // 7) fp64
        {
            auto* hdr = next_hdr(buf, off);
            CHECK(static_cast<data_type>(static_cast<serialized_data_header::word_type>(hdr->type)) == data_type::fp64);
            auto [val, used] = serializer<double>::load(hdr->data(), ds.size() - off);
            CHECK(val == f64);
            CHECK(used == sizeof(std::uint64_t));
            off += serialized_data_header::header_size() + used;
        }

        CHECK(off == ds.size());
    }

    TEST_CASE("data_serializer: store_blob with explicit types (blob/tuple)") {
        data_serializer ds;

        // blob
        std::uint8_t raw_blob[6]{ 10,20,30,40,50,60 };
        ds.store_blob(reinterpret_cast<const byte*>(raw_blob), std::size_t{ 6 }, data_type::blob);

        std::uint8_t raw_tuple[3]{ 0xAA, 0xBB, 0xCC };
        ds.store_blob(reinterpret_cast<const byte*>(raw_tuple), std::size_t{ 3 }, data_type::tuple);

        const byte* buf = ds.data();
        std::size_t off = 0;

        // ---- check blob ----
        {
            auto* hdr = reinterpret_cast<const serialized_data_header*>(buf + off);
            CHECK(static_cast<data_type>(static_cast<serialized_data_header::word_type>(hdr->type)) == data_type::blob);
            auto [view, used] = serializer<byte_view>::load(hdr->data(), ds.size() - off);
            CHECK(view.size() == 6);
            for (std::size_t i = 0; i < 6; ++i)
                CHECK(static_cast<std::uint8_t>(view[i]) == raw_blob[i]);
            off += serialized_data_header::header_size() + used;
        }

        // ---- check tuple ----
        {
            auto* hdr = reinterpret_cast<const serialized_data_header*>(buf + off);
            CHECK(static_cast<data_type>(static_cast<serialized_data_header::word_type>(hdr->type)) == data_type::tuple);
            auto [view, used] = serializer<byte_view>::load(hdr->data(), ds.size() - off);
            CHECK(view.size() == 3);
            for (std::size_t i = 0; i < 3; ++i)
                CHECK(static_cast<std::uint8_t>(view[i]) == raw_tuple[i]);
            off += serialized_data_header::header_size() + used;
        }

        CHECK(off == ds.size());
    }

    TEST_CASE("data_serializer: size accounting for each record") {
        data_serializer ds;
        const std::string s = "abc";
        ds.store(s);

        const byte* buf = ds.data();
        std::size_t off = 0;

        auto* hdr = reinterpret_cast<const serialized_data_header*>(buf + off);
        CHECK(static_cast<data_type>(static_cast<serialized_data_header::word_type>(hdr->type)) == data_type::string);

        const auto header_sz = serialized_data_header::header_size();
        auto [back, used_payload] = serializer<std::string>::load(hdr->data(), ds.size() - off);
        CHECK(back == s);

        CHECK(header_sz + used_payload == ds.size()); 
    }

    TEST_CASE("data_type enum mapping (updated values)") {
        CHECK(static_cast<std::underlying_type_t<data_type>>(data_type::undefined) == 0);
        CHECK(static_cast<std::underlying_type_t<data_type>>(data_type::nill) == 1);
        CHECK(static_cast<std::underlying_type_t<data_type>>(data_type::string) == 2);
        CHECK(static_cast<std::underlying_type_t<data_type>>(data_type::i32) == 3);
        CHECK(static_cast<std::underlying_type_t<data_type>>(data_type::ui32) == 4);
        CHECK(static_cast<std::underlying_type_t<data_type>>(data_type::i64) == 5);
        CHECK(static_cast<std::underlying_type_t<data_type>>(data_type::ui64) == 6);
        CHECK(static_cast<std::underlying_type_t<data_type>>(data_type::fp32) == 7);
        CHECK(static_cast<std::underlying_type_t<data_type>>(data_type::fp64) == 8);
        CHECK(static_cast<std::underlying_type_t<data_type>>(data_type::blob) == 9);
        CHECK(static_cast<std::underlying_type_t<data_type>>(data_type::tuple) == 10);
    }
}
