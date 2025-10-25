// tests/test_byteorder.cpp
#include "tests.hpp"
#include <array>
#include <random>
#include <limits>
#include <type_traits>

#include "fulla/core/byteorder.hpp"

using namespace fulla::core::byteorder;

TEST_SUITE("byteorder (signed/unsigned)") {

    template <typename T>
    void check_roundtrip_le(T value) {
        std::array<fulla::core::byte, sizeof(T)> buf{};
        native_to_le<T>(value, buf.data());
        T back = le_to_native<T>(buf.data());
        CHECK(back == value);
    }

    template <typename T>
    void check_roundtrip_be(T value) {
        std::array<fulla::core::byte, sizeof(T)> buf{};
        native_to_be<T>(value, buf.data());
        T back = be_to_native<T>(buf.data());
        CHECK(back == value);
    }

    TEST_CASE("fixed patterns LE/BE unsigned") {
        SUBCASE("uint16") {
            check_roundtrip_le<std::uint16_t>(0x1122u);
            check_roundtrip_be<std::uint16_t>(0x1122u);
        }
        SUBCASE("uint32") {
            check_roundtrip_le<std::uint32_t>(0x11223344u);
            check_roundtrip_be<std::uint32_t>(0x11223344u);
        }
        SUBCASE("uint64") {
            check_roundtrip_le<std::uint64_t>(0x1122334455667788ull);
            check_roundtrip_be<std::uint64_t>(0x1122334455667788ull);
        }
    }

    TEST_CASE("fixed patterns LE/BE signed") {
        SUBCASE("int16") {
            check_roundtrip_le<std::int16_t>(-12345);
            check_roundtrip_be<std::int16_t>(-12345);
            check_roundtrip_le<std::int16_t>(+12345);
        }
        SUBCASE("int32") {
            check_roundtrip_le<std::int32_t>(-0x1020304);
            check_roundtrip_be<std::int32_t>(-0x1020304);
            check_roundtrip_le<std::int32_t>(+0x1020304);
        }
        SUBCASE("int64") {
            check_roundtrip_le<std::int64_t>(-0x123456789ABCDELL);
            check_roundtrip_be<std::int64_t>(-0x123456789ABCDELL);
            check_roundtrip_le<std::int64_t>(+0x123456789ABCDELL);
        }
    }

    TEST_CASE("word_le / word_be wrappers still consistent") {
        SUBCASE("word_le<int32_t>") {
            word_le<std::int32_t> a = -123456;
            CHECK(static_cast<std::int32_t>(a) == -123456);
            a = 7890123;
            CHECK(static_cast<std::int32_t>(a) == 7890123);
        }

        SUBCASE("word_be<int64_t>") {
            word_be<std::int64_t> b = -9876543210LL;
            CHECK(static_cast<std::int64_t>(b) == -9876543210LL);
            b = 1122334455667788LL;
            CHECK(static_cast<std::int64_t>(b) == 1122334455667788LL);
        }
    }

    TEST_CASE("fuzz roundtrip all integer types") {
        std::mt19937_64 rng{ 987654321ULL };

        auto fuzz = [&](auto tag, bool is_le) {
            using T = decltype(tag);
            std::array<fulla::core::byte, sizeof(T)> buf{};

            if constexpr (std::is_signed_v<T>) {
                std::uniform_int_distribution<long long> dist(
                    (long long)std::numeric_limits<T>::lowest(),
                    (long long)std::numeric_limits<T>::max());

                for (int i = 0; i < 3000; ++i) {
                    T val = static_cast<T>(dist(rng));
                    if (is_le) {
                        native_to_le<T>(val, buf.data());
                        T back = le_to_native<T>(buf.data());
                        CHECK(back == val);
                    }
                    else {
                        native_to_be<T>(val, buf.data());
                        T back = be_to_native<T>(buf.data());
                        CHECK(back == val);
                    }
                }
            }
            else {
                std::uniform_int_distribution<unsigned long long> dist(
                    0ULL,
                    static_cast<unsigned long long>(
                        std::min<unsigned long long>(
                            std::numeric_limits<T>::max(),
                            static_cast<unsigned long long>(std::numeric_limits<long long>::max())
                        )
                        )
                );

                for (int i = 0; i < 3000; ++i) {
                    T val = static_cast<T>(dist(rng));
                    if (is_le) {
                        native_to_le<T>(val, buf.data());
                        T back = le_to_native<T>(buf.data());
                        CHECK(back == val);
                    }
                    else {
                        native_to_be<T>(val, buf.data());
                        T back = be_to_native<T>(buf.data());
                        CHECK(back == val);
                    }
                }
            }
            };


        SUBCASE("LE unsigned") {
            fuzz(std::uint16_t{}, true);
            fuzz(std::uint32_t{}, true);
            fuzz(std::uint64_t{}, true);
        }
        SUBCASE("BE unsigned") {
            fuzz(std::uint16_t{}, false);
            fuzz(std::uint32_t{}, false);
            fuzz(std::uint64_t{}, false);
        }
        SUBCASE("LE signed") {
            fuzz(std::int16_t{}, true);
            fuzz(std::int32_t{}, true);
            fuzz(std::int64_t{}, true);
        }
        SUBCASE("BE signed") {
            fuzz(std::int16_t{}, false);
            fuzz(std::int32_t{}, false);
            fuzz(std::int64_t{}, false);
        }
    }
}

