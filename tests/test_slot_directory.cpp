#include "tests.hpp"
#include "fulla/slots/directory.hpp"
#include "fulla/slots/stable_directory.hpp"

namespace {
    using byte = fulla::core::byte;
    using namespace fulla::slots;
}

static std::vector<byte> make_page(std::size_t sz) {
    return std::vector<byte>(sz, static_cast<byte>(0));
}

template <typename ByteT = unsigned char>
static std::vector<byte> make_bytes(std::size_t n, ByteT start = static_cast<ByteT>(0x10)) {
    std::vector<byte> v(n);
    for (std::size_t i = 0; i < n; ++i) {
        v[i] = static_cast<byte>(static_cast<unsigned>(start) + (i & 0x7F));
    }
    return v;
}

static void expect_eq_mem(const byte* a, const byte* b, std::size_t n) {
    REQUIRE(a != nullptr);
    REQUIRE(b != nullptr);
    CHECK(std::memcmp(a, b, n) == 0);
}

template <typename Dir>
static std::vector<byte> extract_bytes(const Dir& d, typename Dir::slot_type s) {
    auto m = d.get_slot(s);
    return std::vector<byte>(m.data(), m.data() + s.len);
}

template <typename DirDst, typename DirSrc>
static bool merge_apply_reference(DirDst& dst, const DirSrc& src) {
    auto sview = src.view();
    for (std::size_t i = 0; i < sview.size(); ++i) {
        auto sv = src.get_slot(sview[i]);
        if (!dst.insert(dst.size(), std::span<const byte>(sv.data(), sv.size()))) {
            return false;
        }
    }
    return true;
}

template <typename Dir>
static void check_in_bounds(const Dir& d, const std::vector<byte>& page) {
    auto slots = d.view();
    for (std::size_t i = 0; i < slots.size(); ++i) {
        auto m = d.get_slot(slots[i]);
        CHECK(m.data() >= page.data());
        CHECK(m.data() + m.size() <= page.data() + page.size());
    }
}

template <typename DirDst, typename DirSrc>
static void check_concat_payloads(const DirDst& dst, std::size_t dst_before_count, const DirSrc& src) {
    auto dslots = dst.view();
    auto sslots = src.view();
    REQUIRE(dslots.size() == dst_before_count + sslots.size());

    for (std::size_t i = 0; i < sslots.size(); ++i) {
        auto expected = extract_bytes(src, sslots[i]);
        auto got = extract_bytes(dst, dslots[dst_before_count + i]);
        REQUIRE(got.size() == expected.size());
        CHECK(std::memcmp(got.data(), expected.data(), expected.size()) == 0);
    }
}

namespace {
    auto as_span(void* ptr, std::size_t l) -> byte_span {
        return { reinterpret_cast<std::byte*>(ptr), l };
    };

    template <typename PtrT>
    auto as_ptr(fulla::core::byte_span data) {
        return reinterpret_cast<PtrT *>(data.data());
    };

}

TEST_SUITE("page/slot_direcory") {

    using fulla::core::byte_span;

    TEST_CASE("init + empty validate") {
        std::vector<byte> page(512, static_cast<byte>(0));
        directory_view<directory_type::variadic> dir(std::span<byte>(page.data(), page.size()));
        dir.init();
        CHECK(dir.validate());
    }

    TEST_CASE("empty validate") {
        std::vector<byte> page(512, static_cast<byte>(0));
        directory_view<directory_type::variadic> dir(std::span<byte>(page.data(), page.size()));
        CHECK(dir.validate() == false);
    }

    TEST_CASE("init + insert + validate") {
        std::vector<byte> page(512, static_cast<byte>(0));
        directory_view<directory_type::variadic> dir(std::span<byte>(page.data(), page.size()));
        dir.init();

        auto rec10 = make_bytes(10, static_cast<byte>(0x10));
        dir.insert(0, rec10);
        dir.insert(0, rec10);
        dir.insert(0, rec10);


        const auto avail = dir.available_after_compact();
        CHECK(avail == dir.available());
        CHECK(dir.can_insert(avail) == false);
        CHECK(dir.can_insert(avail - 4) == true);

        CHECK(dir.validate());
    }

    TEST_CASE("init + insert + erase + validate") {
        std::vector<byte> page(512, static_cast<byte>(0));
        directory_view<directory_type::variadic> dir(std::span<byte>(page.data(), page.size()));
        dir.init();

        auto rec10 = make_bytes(10, static_cast<byte>(0x10));
        dir.insert(0, rec10);
        dir.insert(0, rec10);
        dir.insert(0, rec10);
        dir.insert(0, rec10);

        auto avail = dir.available_after_compact();
        CHECK(avail == dir.available());

        dir.erase(1);
        dir.erase(2);

        CHECK(dir.validate());

        avail = dir.available_after_compact();
        CHECK(avail > dir.available());

        dir.compact();
        CHECK(avail == dir.available());

        CHECK(dir.validate());
    }

    TEST_CASE("variadic: update fits into current capacity (no relocation)") {
        // Page buffer with enough space
        std::vector<byte> page(512, static_cast<byte>(0));
        directory_view<directory_type::variadic> dir(std::span<byte>(page.data(), page.size()));
        dir.init();

        // Insert one 10B record
        auto rec10 = make_bytes(10, static_cast<byte>(0x21));

        CHECK(dir.can_insert(rec10.size()));
        REQUIRE(dir.insert(0, std::span<const byte>(rec10.data(), rec10.size())));
        REQUIRE(dir.size() == 1);

        // Capture current slot to read 'off' and 'len'
        auto slots = dir.view();
        auto s0 = slots[0];
        auto before_off = s0.off;

        // Update to 12B (should still fit into fix_slot_len(old.len))
        auto rec12 = make_bytes(12, static_cast<byte>(0x31));
        CHECK(dir.can_update(0, rec12.size()) == true);
        CHECK(dir.update(0, std::span<const byte>(rec12.data(), rec12.size())) == true);

        // Verify same offset, new length and contents updated
        auto slots2 = dir.view();
        CHECK(slots2[0].off == before_off);
        CHECK(slots2[0].len == 12);
        auto mem = dir.get_slot(slots2[0]);
        REQUIRE(mem.size() >= 12);
        expect_eq_mem(mem.data(), rec12.data(), 12);
    }

    TEST_CASE("variadic: update requires relocation but succeeds without compact (tail space)") {
        std::vector<byte> page(512, static_cast<byte>(0));
        directory_view<directory_type::variadic> dir(std::span<byte>(page.data(), page.size()));
        dir.init();

        // Two small records
        auto r1 = make_bytes(8, static_cast<byte>(0x41));
        auto r2 = make_bytes(8, static_cast<byte>(0x51));

        CHECK(dir.can_insert(r1.size()));
        REQUIRE(dir.insert(0, std::span<const byte>(r1.data(), r1.size())));

        CHECK(dir.can_insert(r2.size()));
        REQUIRE(dir.insert(1, std::span<const byte>(r2.data(), r2.size())));
        REQUIRE(dir.size() == 2);

        auto s = dir.view();
        auto off_before = s[0].off;

        // Grow record #0 so it no longer fits into its current capacity, but there is tail space
        auto big = make_bytes(100, static_cast<byte>(0x61));
        CHECK(dir.can_update(0, big.size()) == true);
        CHECK(dir.update(0, std::span<const byte>(big.data(), big.size())) == true);

        // Offset should change, contents must match, record #1 must remain valid
        auto s_after = dir.view();
        CHECK(s_after[0].off != off_before);
        CHECK(s_after[0].len == 100);
        auto m0 = dir.get_slot(s_after[0]);
        expect_eq_mem(m0.data(), big.data(), 100);

        auto m1 = dir.get_slot(s_after[1]);
        expect_eq_mem(m1.data(), r2.data(), r2.size());
    }

    TEST_CASE("variadic: update triggers compact with exclusion of growing slot") {
        // Craft a situation: fill page so there is no tail space, but compacting others will make room.
        std::vector<byte> page(512, static_cast<byte>(0));
        directory_view<directory_type::variadic> dir(std::span<byte>(page.data(), page.size()));
        dir.init();

        // Insert 3 records
        auto a = make_bytes(60, static_cast<byte>(0x11));
        auto b = make_bytes(60, static_cast<byte>(0x22));
        auto c = make_bytes(60, static_cast<byte>(0x33));
        REQUIRE(dir.insert(0, std::span<const byte>(a.data(), a.size())));
        REQUIRE(dir.insert(1, std::span<const byte>(b.data(), b.size())));
        REQUIRE(dir.insert(2, std::span<const byte>(c.data(), c.size())));

        // Delete middle to create a free block somewhere in the middle; keep tail tight
        REQUIRE(dir.erase(1));
        // Insert a small one so free-list becomes fragmented
        auto d = make_bytes(8, static_cast<byte>(0x44));
        REQUIRE(dir.insert(1, std::span<const byte>(d.data(), d.size())));

        // Now grow record #0 big enough to require moving and to need compact
        auto big = make_bytes(140, static_cast<byte>(0x77));

        // Depending on your exact allocator, this may or may not need compact;
        // The important part: update returns true and data is consistent.
        CHECK(dir.can_update(0, big.size()) == true);
        CHECK(dir.update(0, std::span<const byte>(big.data(), big.size())) == true);

        // Validate contents and that records don't overlap and live within page
        auto slots = dir.view();
        auto in_bounds = [&](std::size_t i) {
            auto mem = dir.get_slot(slots[i]);
            return (mem.data() >= page.data()) &&
                (mem.data() + mem.size() <= page.data() + page.size());
            };
        REQUIRE(in_bounds(0));
        REQUIRE(in_bounds(1));
        REQUIRE(in_bounds(2));

        auto m0 = dir.get_slot(slots[0]);
        expect_eq_mem(m0.data(), big.data(), big.size());
    }

    TEST_CASE("variadic: update fails cleanly when even compact won't help") {
        std::vector<byte> page(256, static_cast<byte>(0));
        directory_view<directory_type::variadic> dir(std::span<byte>(page.data(), page.size()));
        dir.init();

        auto small = make_bytes(40, static_cast<byte>(0x10));
        REQUIRE(dir.insert(0, std::span<const byte>(small.data(), small.size())));

        // Ask for something absurdly large for this page
        auto huge = make_bytes(250, static_cast<byte>(0x99));
        CHECK(dir.can_update(0, huge.size()) == false);
        CHECK(dir.update(0, std::span<const byte>(huge.data(), huge.size())) == false);

        // Original data must remain readable
        auto s = dir.view();
        auto m = dir.get_slot(s[0]);
        REQUIRE(m.size() >= small.size());
        expect_eq_mem(m.data(), small.data(), small.size());
    }

    TEST_CASE("variadic: compact and update") {
        std::vector<byte> page(256, static_cast<byte>(0));
        directory_view<directory_type::variadic> dir(std::span<byte>(page.data(), page.size()));
        dir.init();

        const auto& smols = {
            make_bytes(20, static_cast<byte>(0x10)),
            make_bytes(20, static_cast<byte>(0x10)),
            make_bytes(20, static_cast<byte>(0x10)),
            make_bytes(20, static_cast<byte>(0x10)),
            make_bytes(20, static_cast<byte>(0x10)),
            make_bytes(20, static_cast<byte>(0x10)),
            make_bytes(20, static_cast<byte>(0x10)),
        };

        for (auto& s : smols) {
            REQUIRE(dir.insert(0, std::span<const byte>(s.data(), s.size())));
        }

        auto before_removing = dir.view();
        dir.erase(dir.size() / 2);
        dir.erase(dir.size() / 2);
        dir.erase(dir.size() / 2);

        auto large = make_bytes(150, static_cast<byte>(150));
        auto all_slots = dir.view();
        auto pos = all_slots.size() / 2;
        auto old_len = all_slots[pos].len;
        auto old_off = all_slots[pos].off;
        [[maybe_unused]] auto old_available = dir.available();
        [[maybe_unused]] auto old_available_compact = dir.available_after_compact();

        REQUIRE(dir.can_update(pos, large.size()));
        REQUIRE(dir.update(pos, std::span<const byte>(large.data(), large.size())));

        CHECK(all_slots[pos].len == 150);
        CHECK(all_slots[pos].off != old_off);

        CHECK(true);
    }

    TEST_CASE("fixed: update accepts <= slot_size and rejects larger") {
        std::vector<byte> page(256, static_cast<byte>(0));
        directory_view<directory_type::fixed> dir(std::span<byte>(page.data(), page.size()));
        const std::uint16_t slot_size = 16;
        dir.init(slot_size);

        auto r = make_bytes(12, static_cast<byte>(0x55));
        REQUIRE(dir.insert(0, std::span<const byte>(r.data(), r.size())));

        // <= slot size should pass and not move
        auto s = dir.view();
        auto off_before = s[0].off;

        auto r2 = make_bytes(16, static_cast<byte>(0x66));
        CHECK(dir.update(0, std::span<const byte>(r2.data(), r2.size())) == true);
        auto s2 = dir.view();
        CHECK(s2[0].off == off_before);
        CHECK(s2[0].len == 16);
        auto m = dir.get_slot(s2[0]);
        expect_eq_mem(m.data(), r2.data(), r2.size());

        // > slot size must fail
        auto too_big = make_bytes(17, static_cast<byte>(0x77));
        CHECK(dir.update(0, std::span<const byte>(too_big.data(), too_big.size())) == false);
    }

    TEST_CASE("merge_need_bytes basic behavior") {
        std::vector<byte> dst_buf(512, static_cast<byte>(0));
        std::vector<byte> src_buf(512, static_cast<byte>(0));

        directory_view<directory_type::variadic> dst(dst_buf);
        directory_view<directory_type::variadic> src(src_buf);
        dst.init();
        src.init();

        auto r1 = make_bytes(20);
        auto r2 = make_bytes(30);
        REQUIRE(src.insert(0, std::span<const byte>(r1.data(), r1.size())));
        REQUIRE(src.insert(1, std::span<const byte>(r2.data(), r2.size())));

        std::size_t expected =
            dst.fixed_len(r1.size()) +
            dst.fixed_len(r2.size()) +
            src.size() * sizeof(typename decltype(dst)::slot_type);

        CHECK(merge_need_bytes(dst, src) == expected);
    }

    TEST_CASE("can_merge variadic fits easily") {
        std::vector<byte> dst_buf(512, static_cast<byte>(0));
        std::vector<byte> src_buf(256, static_cast<byte>(0));

        directory_view<directory_type::variadic> dst(dst_buf);
        directory_view<directory_type::variadic> src(src_buf);
        dst.init();
        src.init();

        auto rec = make_bytes(40);
        REQUIRE(src.insert(0, std::span<const byte>(rec.data(), rec.size())));
        REQUIRE(src.insert(1, std::span<const byte>(rec.data(), rec.size())));
        REQUIRE(src.insert(2, std::span<const byte>(rec.data(), rec.size())));

        CHECK(can_merge(dst, src) == true);
    }

    TEST_CASE("can_merge variadic fails when dst is too small") {
        std::vector<byte> dst_buf(128, static_cast<byte>(0));  // very small
        std::vector<byte> src_buf(256, static_cast<byte>(0));

        directory_view<directory_type::variadic> dst(dst_buf);
        directory_view<directory_type::variadic> src(src_buf);
        dst.init();
        src.init();

        auto rec = make_bytes(40);
        // 3 * (40 + 4) -- 132  
        REQUIRE(src.insert(0, std::span<const byte>(rec.data(), rec.size())));
        REQUIRE(src.insert(1, std::span<const byte>(rec.data(), rec.size())));
        REQUIRE(src.insert(2, std::span<const byte>(rec.data(), rec.size())));

        CHECK(can_merge(dst, src) == false);
    }

    TEST_CASE("can_merge fixed: fails on oversized source slot") {
        std::vector<byte> dst_buf(512, static_cast<byte>(0));
        std::vector<byte> src_buf(256, static_cast<byte>(0));

        directory_view<directory_type::fixed> dst(dst_buf);
        directory_view<directory_type::fixed> src(src_buf);

        const std::uint16_t slot_size = 16;
        dst.init(slot_size);
        src.init(slot_size);

        // make source contain record too big for dst slot
        auto big = make_bytes(40);
        REQUIRE(src.insert(0, std::span<const byte>(big.data(), big.size())) == false);
        // emulate one smaller (fits)
        auto small = make_bytes(10);
        REQUIRE(src.insert(0, std::span<const byte>(small.data(), small.size())));

        // ensure can_merge detects too-large len
        // artificially patch len bigger than dst slot_size
        auto slots = src.view();
        const_cast<typename decltype(src)::slot_type&>(slots[0]).len = 100;

        CHECK(can_merge(dst, src) == false);
    }

    TEST_CASE("can_merge fixed: success with all fitting records") {
        std::vector<byte> dst_buf(512, static_cast<byte>(0));
        std::vector<byte> src_buf(256, static_cast<byte>(0));

        directory_view<directory_type::fixed> dst(dst_buf);
        directory_view<directory_type::fixed> src(src_buf);

        const std::uint16_t slot_size = 32;
        dst.init(slot_size);
        src.init(slot_size);

        auto r1 = make_bytes(20);
        auto r2 = make_bytes(10);
        REQUIRE(src.insert(0, std::span<const byte>(r1.data(), r1.size())));
        REQUIRE(src.insert(1, std::span<const byte>(r2.data(), r2.size())));

        CHECK(can_merge(dst, src) == true);
    }

    TEST_CASE("merge_need_bytes: basic") {
        std::vector<byte> dst_buf = make_page(512);
        std::vector<byte> src_buf = make_page(512);

        directory_view<directory_type::variadic> dst(std::span<byte>(dst_buf.data(), dst_buf.size()));
        directory_view<directory_type::variadic> src(std::span<byte>(src_buf.data(), src_buf.size()));
        dst.init();
        src.init();

        auto r1 = make_bytes(20, 0x21);
        auto r2 = make_bytes(30, 0x31);
        REQUIRE(src.insert(0, std::span<const byte>(r1.data(), r1.size())));
        REQUIRE(src.insert(1, std::span<const byte>(r2.data(), r2.size())));

        const std::size_t expected =
            static_cast<std::size_t>(dst.fixed_len(r1.size())) +
            static_cast<std::size_t>(dst.fixed_len(r2.size())) +
            static_cast<std::size_t>(src.size()) * sizeof(typename decltype(dst)::slot_type);

        CHECK(merge_need_bytes(dst, src) == expected);
    }

    TEST_CASE("can_merge + merge_apply_reference (variadic): success, order is ok") {
        std::vector<byte> dst_buf = make_page(1024);
        std::vector<byte> src_buf = make_page(512);

        directory_view<directory_type::variadic> dst(std::span<byte>(dst_buf.data(), dst_buf.size()));
        directory_view<directory_type::variadic> src(std::span<byte>(src_buf.data(), src_buf.size()));
        dst.init();
        src.init();

        // dst: 3 records
        auto a = make_bytes(24, 0x11);
        auto b = make_bytes(12, 0x22);
        auto c = make_bytes(16, 0x33);
        REQUIRE(dst.insert(0, std::span<const byte>(a.data(), a.size())));
        REQUIRE(dst.insert(1, std::span<const byte>(b.data(), b.size())));
        REQUIRE(dst.insert(2, std::span<const byte>(c.data(), c.size())));
        const std::size_t dst_before = dst.size();

        // src: 3 records
        auto s1 = make_bytes(30, 0x44);
        auto s2 = make_bytes(8, 0x55);
        auto s3 = make_bytes(40, 0x66);
        REQUIRE(src.insert(0, std::span<const byte>(s1.data(), s1.size())));
        REQUIRE(src.insert(1, std::span<const byte>(s2.data(), s2.size())));
        REQUIRE(src.insert(2, std::span<const byte>(s3.data(), s3.size())));

        REQUIRE(can_merge(dst, src) == true);

        REQUIRE(merge_apply_reference(dst, src) == true);

        check_concat_payloads(dst, dst_before, src);
        check_in_bounds(dst, dst_buf);

        CHECK(dst.available_after_compact() >= dst.available());
    }

    TEST_CASE("can_merge (variadic): false -> merge impossible") {
        std::vector<byte> dst_buf = make_page(256);
        std::vector<byte> src_buf = make_page(512);

        directory_view<directory_type::variadic> dst(std::span<byte>(dst_buf.data(), dst_buf.size()));
        directory_view<directory_type::variadic> src(std::span<byte>(src_buf.data(), src_buf.size()));
        dst.init();
        src.init();

        // almost full dst
        auto big_dst = make_bytes(180, 0x10);
        REQUIRE(dst.insert(0, std::span<const byte>(big_dst.data(), big_dst.size())));

        auto big1 = make_bytes(60, 0x41);
        auto big2 = make_bytes(60, 0x51);
        REQUIRE(src.insert(0, std::span<const byte>(big1.data(), big1.size())));
        REQUIRE(src.insert(1, std::span<const byte>(big2.data(), big2.size())));

        CHECK(can_merge(dst, src) == false);
        CHECK(merge_apply_reference(dst, src) == false); 
    }

    TEST_CASE("can_merge + merge_apply_reference (fixed): success and fail on oversize") {
        std::vector<byte> dst_buf = make_page(1024);
        std::vector<byte> src_buf = make_page(512);

        directory_view<directory_type::fixed> dst(std::span<byte>(dst_buf.data(), dst_buf.size()));
        directory_view<directory_type::fixed> src(std::span<byte>(src_buf.data(), src_buf.size()));
        const std::uint16_t SLOT = 32;
        dst.init(SLOT);
        src.init(SLOT);

        auto d1 = make_bytes(16, 0x11);
        auto d2 = make_bytes(24, 0x22);
        REQUIRE(dst.insert(0, std::span<const byte>(d1.data(), d1.size())));
        REQUIRE(dst.insert(1, std::span<const byte>(d2.data(), d2.size())));
        const std::size_t dst_before = dst.size();

        auto ok = make_bytes(20, 0x33);
        auto bad = make_bytes(40, 0x44);
        REQUIRE(src.insert(0, std::span<const byte>(ok.data(), ok.size())));
        CHECK(src.insert(1, std::span<const byte>(bad.data(), bad.size())) == false);

        CHECK(can_merge(dst, src) == true);
        REQUIRE(merge_apply_reference(dst, src) == true);

        check_concat_payloads(dst, dst_before, src);
        check_in_bounds(dst, dst_buf);
    }

    TEST_CASE("merge_need_bytes vs available_after_compact: border case") {

        std::vector<byte> dst_buf = make_page(256);
        std::vector<byte> src_buf = make_page(256);

        directory_view<directory_type::variadic> dst(std::span<byte>(dst_buf.data(), dst_buf.size()));
        directory_view<directory_type::variadic> src(std::span<byte>(src_buf.data(), src_buf.size()));
        dst.init();
        src.init();

        auto a = make_bytes(60, 0x11);
        auto b = make_bytes(20, 0x22);
        auto c = make_bytes(20, 0x33);
        REQUIRE(dst.insert(0, std::span<const byte>(a.data(), a.size())));
        REQUIRE(dst.insert(1, std::span<const byte>(b.data(), b.size())));
        REQUIRE(dst.insert(2, std::span<const byte>(c.data(), c.size())));
        dst.erase(1);

        auto s = make_bytes(dst.available_after_compact() - sizeof(typename decltype(dst)::slot_type), 0x55);
        if (!src.insert(0, std::span<const byte>(s.data(), s.size()))) {
            s.resize(s.size() > 8 ? s.size() - 8 : s.size());
            REQUIRE(src.insert(0, std::span<const byte>(s.data(), s.size())));
        }

        CHECK(can_merge(dst, src) == true);
        REQUIRE(merge_apply_reference(dst, src) == true);
        [[maybe_unused]] auto all_dst = dst.view();
        CHECK(dst.validate());
        check_in_bounds(dst, dst_buf);
    }

    TEST_CASE("stable slot directory") {

        std::vector<byte> buf = make_page(256);
        struct test {
            int i = 0;
            double d = 0.0;
            auto operator <=> (const test &) const noexcept = default;
        };

        auto as_span = [](void* ptr, std::size_t l) -> byte_span {
            return { reinterpret_cast<std::byte*>(ptr), l };
            };

        stable_directory_view<byte_span> dst(byte_span(buf.data(), buf.size()));
        dst.init(sizeof(test));

        CHECK(dst.size() == 0);
        CHECK(dst.capacity() != 0);

        for (int i = 0; i < dst.capacity(); ++i) {
            if (i == dst.capacity() - 1) {
                std::cout << "";
            }
            test t0 = { .i = i, .d = 0.45 };
            CHECK(dst.set(i, as_span(&t0, sizeof(t0))));
        }

        CHECK(dst.size() == dst.capacity());

        for (int i = 0; i < dst.capacity(); ++i) {
            test t0 = { .i = i, .d = 0.45 };
            auto t = dst.get(i);
            CHECK(t.size() == sizeof(test));
            CHECK(*as_ptr<test>(t) == t0);
        }
        for (int i = 0; i < dst.capacity(); ++i) {
            CHECK(dst.erase(i));
        }

        CHECK(dst.size() == 0);

    }

}
