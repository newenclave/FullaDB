#include "tests.hpp"
#include "fulla/page/header.hpp"

using namespace fulla::page;

TEST_SUITE("page/header") {

    TEST_CASE("page_header::init layout & pointers") {
        constexpr std::size_t PS = 4096;

        alignas(64) std::array<fulla::core::byte, PS> buf{};
        auto* hdr = reinterpret_cast<page_header*>(buf.data());
        hdr->init(static_cast<std::uint16_t>(page_kind::bpt_leaf), PS, /*self*/123, /*subhdr_size*/12);

        CHECK(sizeof(page_header) == page_header::header_size());
        CHECK(hdr->kind.get() == static_cast<std::uint16_t>(page_kind::bpt_leaf));
        CHECK(static_cast<std::uint16_t>(hdr->metadata_size) == 0);

        const auto base = static_cast<std::uint16_t>(sizeof(page_header) + 12);
        const auto expected_capacity = (PS - base);
        CHECK(static_cast<std::uint16_t>(hdr->base()) == base);
        CHECK(static_cast<std::uint16_t>(hdr->capacity()) == expected_capacity);
        CHECK(static_cast<std::uint16_t>(hdr->page_end) == PS);
        CHECK(static_cast<std::uint32_t>(hdr->self_pid) == 123);
        //CHECK(static_cast<std::uint32_t>(hdr->crc) == 0);

        CHECK(hdr->data() == reinterpret_cast<fulla::core::byte*>(hdr));
        CHECK(page_header::header_size() == 16);
    }

    TEST_CASE("page_header::init layout & pointers & metadata") {
        constexpr std::size_t PS = 4096;

        alignas(64) std::array<fulla::core::byte, PS> buf{};
        auto* hdr = reinterpret_cast<page_header*>(buf.data());
        hdr->init(777, PS, 
            /*self*/123, /*subhdr_size*/12, /*metadata*/ 32);

        CHECK(sizeof(page_header) == page_header::header_size());
        CHECK(hdr->kind.get() == 777);
        CHECK(static_cast<std::uint16_t>(hdr->metadata_size) == 32);

        const auto base = static_cast<std::uint16_t>(sizeof(page_header) + 12 + 32);
        const auto expected_capacity = (PS - base);
        auto metadata = hdr->metadata();

        CHECK(metadata.size() == 32);
        CHECK(static_cast<std::uint16_t>(hdr->base()) == base);
        CHECK(static_cast<std::uint16_t>(hdr->capacity()) == expected_capacity);
        CHECK(static_cast<std::uint16_t>(hdr->page_end) == PS);
        CHECK(static_cast<std::uint32_t>(hdr->self_pid) == 123);
        //CHECK(static_cast<std::uint32_t>(hdr->crc) == 0);

        CHECK(hdr->data() == reinterpret_cast<fulla::core::byte*>(hdr));
        CHECK(page_header::header_size() == 16);
    }
}
