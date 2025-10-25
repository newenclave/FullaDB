// tests/test_buffer_manager.cpp
#include "tests.hpp"

#include <fulla/core/bytes.hpp>
#include <fulla/page/header.hpp>
#include <fulla/storage/device.hpp>
#include <fulla/storage/file_device.hpp>
#include <fulla/storage/buffer_manager.hpp>

#include <filesystem>
#include <vector>
using namespace fulla::core;
using namespace fulla::storage;
using namespace fulla::page;

static std::filesystem::path temp_file(const char* stem) {
    namespace fs = std::filesystem;
    auto p = fs::temp_directory_path() / (std::string(stem) + "_" + std::to_string(std::rand()) + ".bin");
    return p;
}


TEST_SUITE("storage/buffer_manager") {
    TEST_CASE("create/fetch/flush cycle") {

        namespace fs = std::filesystem;
        auto path = temp_file("bm");

        {
            file_device dev(path, 1024);
            std::vector<byte> arena(1024 * 2);
            using BM = buffer_manager<file_device>;
            std::vector<typename BM::frame> frames(2);

            BM bm(dev, byte_span{arena.data(), arena.size()},
                  std::span{frames.data(), frames.size()});

            // create a page and write header
            auto ph = bm.create();
            CHECK(ph);
            {
                auto pg = ph.rw_span();
                auto* hdr = reinterpret_cast<page_header*>(pg.data());
                hdr->init(page_kind::heap, 1024, /*self*/0);
            }
            ph.reset(); // unpin

            // flush all
            bm.flush_all(true);

            // fetch back page 0
            auto fh = bm.fetch(0);
            CHECK(fh);
            {
                auto pg = fh.ro_span();
                auto* hdr = reinterpret_cast<const page_header*>(pg.data());
                CHECK(static_cast<page_kind>(static_cast<std::uint16_t>(hdr->kind)) == page_kind::heap);
            }
        }

        CHECK(std::filesystem::remove(path));
    }

    TEST_CASE("eviction under pressure (2 frames, 3 pages)") {
        namespace fs = std::filesystem;
        auto path = temp_file("bm_evict");

        {
            file_device dev(path, 1024);
            std::vector<byte> arena(1024 * 2);
            using BM = buffer_manager<file_device>;
            std::vector<typename BM::frame> frames(2);
            BM bm(dev, byte_span{arena.data(), arena.size()},
                  std::span{frames.data(), frames.size()});

            auto p0 = bm.create(); 
            auto id0 = p0.id(); 
            p0.reset();
            
            auto p1 = bm.create(); 
            auto id1 = p1.id(); 
            p1.reset();
            bm.flush_all(true);

            // touching p0 and p1 to set ref bits
            auto h0 = bm.fetch(id0); 
            auto h1 = bm.fetch(id1);
            h0.reset(); 
            h1.reset();

            // third page forces eviction
            auto p2 = bm.create(); 
            [[maybe_unused]] auto id2 = p2.id();
            p2.reset();
            bm.flush_all(true);

            CHECK(bm.resident_pages() <= 2);
            auto st = bm.get_stats();
            CHECK(st.evictions >= 1);
        }

        CHECK(std::filesystem::remove(path));
    }
}
