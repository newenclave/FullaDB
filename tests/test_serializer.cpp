
// tests/test_serializer.cpp
#include <array>
#include <string>
#include <random>
#include <limits>
#include <type_traits>

#include "tests.hpp"
#include "fulla/codec/serializer.hpp"
#include "fulla/core/bytes.hpp"

using fulla::core::byte;
using fulla::core::byte_view;
using namespace fulla::codec;

template <typename T>
static void roundtrip_scalar_le(T value) {
    std::array<byte, sizeof(T)> buf{};
    auto* p = buf.data();
    const std::size_t written = serializer<T>::store(value, p);
    CHECK(written == sizeof(T));
    auto [back, used] = serializer<T>::load(p, written);
    CHECK(used == sizeof(T));
    CHECK(back == value);
    CHECK(serializer<T>::size(value) == sizeof(T));
}

TEST_SUITE("serializer") {

    TEST_CASE("integers: signed/unsigned roundtrip") {
        SUBCASE("i16/u16") { roundtrip_scalar_le<std::int16_t>(-12345); roundtrip_scalar_le<std::uint16_t>(0xBEEF); }
        SUBCASE("i32/u32") { roundtrip_scalar_le<std::int32_t>(-0x1020304); roundtrip_scalar_le<std::uint32_t>(0xDEADBEEF); }
        SUBCASE("i64/u64") { roundtrip_scalar_le<std::int64_t>(-0x123456789ABCLL); roundtrip_scalar_le<std::uint64_t>(0x1122334455667788ULL); }
    }

    TEST_CASE("floats: float/double roundtrip (LE)") {
        SUBCASE("float") { roundtrip_scalar_le<float>(1234.5f);  roundtrip_scalar_le<float>(-0.0f); }
        SUBCASE("double") { roundtrip_scalar_le<double>(-98765.125); roundtrip_scalar_le<double>(0.0); }
    }

    TEST_CASE("string: size-prefixed with null terminator") {
        // format: [len_with_header:u32][bytes...][0]
        std::array<byte, 256> buf{};
        auto* p = buf.data();

        const std::string s = "hello, fulla!";
        const std::size_t need = serializer<std::string>::size(s);
        CHECK(need == (sizeof(std::uint32_t) + s.size() + 1));

        const std::size_t written = serializer<std::string>::store(s, p);
        CHECK(written == need);

        auto [back, used] = serializer<std::string>::load(p, written);
        CHECK(used == written);
        CHECK(back == s);
    }

    TEST_CASE("byte_view: length-prefixed blob") {
        // format: [len_with_header:u32][blob...]
        std::array<std::uint8_t, 32> raw{};
        for (std::size_t i = 0; i < raw.size(); ++i) raw[i] = static_cast<std::uint8_t>(i ^ 0x5A);

        byte_view v{ reinterpret_cast<const byte*>(raw.data()), raw.size() };

        std::array<byte, 64> buf{};
        auto* p = buf.data();

        const std::size_t need = serializer<byte_view>::size(v);
        CHECK(need == (sizeof(std::uint32_t) + v.size()));
        const std::size_t written = serializer<byte_view>::store(v, p);
        CHECK(written == need);

        auto [back_view, used] = serializer<byte_view>::load(p, written);
        CHECK(used == written);
        CHECK(back_view.size() == v.size());
		// every byte check
        for (std::size_t i = 0; i < v.size(); ++i) {
            CHECK(static_cast<const std::uint8_t>(back_view[i]) == raw[i]);
        }
    }

    TEST_CASE("fuzz: integers random LE roundtrip via serializer") {
        std::mt19937_64 rng{ 0xC0FFEE123456789ULL };

        auto fuzz_signed = [&](auto tag) {
            using T = decltype(tag);
            std::uniform_int_distribution<long long> dist(
                (long long)std::numeric_limits<T>::lowest(),
                (long long)std::numeric_limits<T>::max()
            );
            std::array<byte, sizeof(T)> buf{};
            for (int i = 0; i < 3000; ++i) {
                T v = static_cast<T>(dist(rng));
                const std::size_t written = serializer<T>::store(v, buf.data());
                auto [back, used] = serializer<T>::load(buf.data(), written);
                CHECK(used == written);
                CHECK(back == v);
            }
            };

        auto fuzz_unsigned = [&](auto tag) {
            using T = decltype(tag);
            std::uniform_int_distribution<unsigned long long> dist(
                0ULL,
                static_cast<unsigned long long>(
                    std::min<unsigned long long>(
                        std::numeric_limits<T>::max(),
                        static_cast<unsigned long long>(std::numeric_limits<long long>::max())
                    )
                    )
            );
            std::array<byte, sizeof(T)> buf{};
            for (int i = 0; i < 3000; ++i) {
                T v = static_cast<T>(dist(rng));
                const std::size_t written = serializer<T>::store(v, buf.data());
                auto [back, used] = serializer<T>::load(buf.data(), written);
                CHECK(used == written);
                CHECK(back == v);
            }
            };

        SUBCASE("signed") { fuzz_signed(std::int16_t{}); fuzz_signed(std::int32_t{}); fuzz_signed(std::int64_t{}); }
        SUBCASE("unsigned") { fuzz_unsigned(std::uint16_t{}); fuzz_unsigned(std::uint32_t{}); fuzz_unsigned(std::uint64_t{}); }
    }
}
