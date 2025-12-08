// tests/test_file_device.cpp
#include "tests.hpp"

#include <filesystem>
#include <vector>
#include <random>
#include <compare>

#include "fulla/core/bytes.hpp"
#include "fulla/storage/device.hpp"
#include "fulla/storage/file_device.hpp"
#include "fulla/storage/file_block_device.hpp"

using namespace fulla::core;
using namespace fulla::storage;

static std::filesystem::path make_temp_file(const char* stem) {
    namespace fs = std::filesystem;
    fs::path dir = fs::temp_directory_path();
    // use timestamp-based suffix to avoid collisions
    auto now = std::chrono::high_resolution_clock::now().time_since_epoch().count();
    fs::path p = dir / (std::string(stem) + "_" + std::to_string(now) + ".bin");
    return p;
}

TEST_SUITE("storage/file_device") {

    TEST_CASE("open/create + size is zero") {
        namespace fs = std::filesystem;
        auto path = make_temp_file("fulla_fd");

        {
            file_device dev(path, /*block_size=*/4096);
            CHECK(dev.is_open());

            // New file should be empty.
            auto sz = dev.get_file_size();
            CHECK(sz == 0);
        }

        // Clean up
        CHECK(fs::exists(path));
        CHECK(fs::remove(path));
    }

    TEST_CASE("write_at_start / read_at_offset roundtrip") {
        namespace fs = std::filesystem;
        auto path = make_temp_file("fulla_fd_io");

        {
            file_device dev(path, 4096);
            CHECK(dev.is_open());

            // Prepare payload
            std::vector<byte> wbuf(32);
            for (std::size_t i = 0; i < wbuf.size(); ++i) {
                wbuf[i] = static_cast<byte>(i ^ 0x5Au);
            }

            // Write at start
            CHECK(dev.write_at_start(wbuf.data(), wbuf.size()));

            // Read back
            std::vector<byte> rbuf(wbuf.size());
            CHECK(dev.read_at_offset(0, rbuf.data(), rbuf.size()));

            CHECK(rbuf.size() == wbuf.size());
            for (std::size_t i = 0; i < wbuf.size(); ++i) {
                CHECK(static_cast<unsigned char>(rbuf[i]) ==
                      static_cast<unsigned char>(wbuf[i]));
            }

            // File size should be at least written bytes
            auto sz = dev.get_file_size();
            CHECK(sz >= wbuf.size());
        }

        CHECK(fs::remove(path));
    }

    TEST_CASE("append returns correct position and grows the file") {
        namespace fs = std::filesystem;
        auto path = make_temp_file("fulla_fd_append");

        {
            file_device dev(path, 4096);
            CHECK(dev.is_open());

            std::vector<byte> a(10, static_cast<byte>(0xAA));
            std::vector<byte> b(20, static_cast<byte>(0xBB));

            auto pos_a = dev.append(a.data(), a.size());
            CHECK(pos_a == 0); // first append at empty file starts at 0

            auto pos_b = dev.append(b.data(), b.size());
            CHECK(pos_b == a.size()); // next append starts after A

            auto sz = dev.get_file_size();
            CHECK(sz == a.size() + b.size());

            // Read back B and verify
            std::vector<byte> rb(b.size());
            CHECK(dev.read_at_offset(pos_b, rb.data(), rb.size()));
            for (std::size_t i = 0; i < b.size(); ++i) {
                CHECK(static_cast<unsigned char>(rb[i]) == 0xBB);
            }
        }

        CHECK(fs::remove(path));
    }


    TEST_CASE("append returns correct position and grows the file_block") {
        namespace fs = std::filesystem;
        auto path = make_temp_file("fulla_fd_append");

        {
            file_block_device dev(path, 4096);
            CHECK(dev.is_open());

            std::vector<byte> a(10, static_cast<byte>(0xAA));
            std::vector<byte> b(20, static_cast<byte>(0xBB));

            auto pos_a = dev.append(a.data(), a.size());
            CHECK(pos_a == 0); // first append at empty file starts at 0

            auto pos_b = dev.append(b.data(), b.size());
            CHECK(pos_b == 1); // next append starts next block

            auto sz = dev.blocks_count();
            CHECK(sz == 2);

            // Read back B and verify
            std::vector<byte> rb(b.size());
            CHECK(dev.read_block(1, rb.data(), rb.size()));
            for (std::size_t i = 0; i < b.size(); ++i) {
                CHECK(static_cast<unsigned char>(rb[i]) == 0xBB);
            }
        }

        CHECK(fs::remove(path));
    }

    TEST_CASE("allocate_block aligns to block size and extends file") {
        namespace fs = std::filesystem;
        auto path = make_temp_file("fulla_fd_alloc");

        constexpr std::size_t BS = 4096;

        {
            file_device dev(path, BS);
            CHECK(dev.is_open());

            // Initially size is 0
            CHECK(dev.get_file_size() == 0);

            // Allocate first block
            auto pos0 = dev.allocate_block();
            CHECK(pos0 % BS == 0);
            auto size0 = dev.get_file_size();
            CHECK(size0 >= pos0 + BS);

            // Append a few bytes after the block
            std::vector<byte> junk(17, static_cast<byte>(0xCC));
            auto apos = dev.append(junk.data(), junk.size());
            CHECK(apos == size0); // append starts at current end

            auto size1 = dev.get_file_size();
            CHECK(size1 == size0 + junk.size());

            // Allocate next block (should align to next multiple of BS)
            auto pos1 = dev.allocate_block();
            CHECK(pos1 % BS == 0);
            CHECK(pos1 >= size1); // aligned at or after the previous end

            auto size2 = dev.get_file_size();
            CHECK(size2 >= pos1 + BS);
        }

        CHECK(fs::remove(path));
    }

    TEST_CASE("read/write at offset inside allocated block") {
        namespace fs = std::filesystem;
        auto path = make_temp_file("fulla_fd_rwblk");

        {
            file_device dev(path, 1024);
            CHECK(dev.is_open());

            auto base = dev.allocate_block();
            CHECK(base % 1024 == 0);

            // Prepare page-sized buffer
            std::vector<byte> page(1024);
            for (std::size_t i = 0; i < page.size(); ++i) {
                page[i] = static_cast<byte>((i * 7u) & 0xFFu);
            }

            // Write and read back
            CHECK(dev.write_at_offset(base, page.data(), page.size()));

            std::vector<byte> back(page.size());
            CHECK(dev.read_at_offset(base, back.data(), back.size()));

            CHECK(page.size() == back.size());
            for (std::size_t i = 0; i < page.size(); ++i) {
                CHECK(static_cast<unsigned char>(page[i]) ==
                      static_cast<unsigned char>(back[i]));
            }
        }

        CHECK(fs::remove(path));
    }
}
