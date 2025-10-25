#include "tests.hpp"
#include "fulla/page/header.hpp"

using namespace fulla::page;

TEST_SUITE("page/header") {

    TEST_CASE("page_header::init layout & pointers") {
        constexpr std::size_t PS = 4096;

        alignas(64) std::array<fulla::core::byte, PS> buf{};
        auto* hdr = reinterpret_cast<page_header*>(buf.data());
        hdr->init(page_kind::btree_leaf, PS, /*self*/123, /*subhdr_size*/12);

        CHECK(sizeof(page_header) == page_header::header_size());
        CHECK(static_cast<page_kind>(static_cast<std::uint16_t>(hdr->kind)) == page_kind::btree_leaf);
        CHECK(static_cast<std::uint16_t>(hdr->slots) == 0);

        const auto base = static_cast<std::uint16_t>(sizeof(page_header) + 12);
        CHECK(static_cast<std::uint16_t>(hdr->slots_offset) == base);
        CHECK(static_cast<std::uint16_t>(hdr->free_beg) == base);
        CHECK(static_cast<std::uint16_t>(hdr->free_end) == PS);
        CHECK(static_cast<std::uint32_t>(hdr->self_pid) == 123);
        CHECK(static_cast<std::uint32_t>(hdr->crc) == 0);

        CHECK(hdr->data() == reinterpret_cast<fulla::core::byte*>(hdr));
        CHECK(page_header::header_size() == 20);
    }
}
