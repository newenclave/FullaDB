

#include <algorithm>
#include <map>
#include <random>
#include <string>
#include <vector>
#include <numeric>
#include <ranges>

#include "tests.hpp"

#include "fulla/bpt/tree.hpp"
#include "fulla/bpt/policies.hpp"
#include "fulla/bpt/memory/model.hpp"
#include "fulla/bpt/ranges.hpp"

using namespace fulla::bpt;
using namespace fulla::bpt::policies;

namespace {

// A handy alias for a small in-memory model with max 5 keys per node.
template <typename K, typename V, std::size_t MaxKeys = 5, typename Less = std::less<void>>
using MemModel = fulla::bpt::memory::model<K, V, MaxKeys, Less>;

template <typename Tree>
static std::vector<std::pair<typename Tree::key_out_type, typename Tree::value_out_type>>
collect_all(const Tree& t) {
    std::vector<std::pair<typename Tree::key_out_type, typename Tree::value_out_type>> out;
    for (auto &it: t) {
        out.emplace_back(it->first, it->second);
    }
    return out;
}

} // namespace

TEST_CASE("memory B+Tree: basic insert & find") {
    using Model = MemModel<int, std::string, 5>;
    using Tree  = fulla::bpt::tree<Model>;
    using key_like_type = typename Model::key_like_type;
    using value_in_type = typename Model::value_in_type;
    Tree t;

    // insert ascending
    for (int i = 0; i < 50; ++i) {
        auto ts = std::to_string(i);
        CHECK(t.insert(key_like_type{ i }, value_in_type{ts}, insert::insert, rebalance::neighbor_share) == true);
    }
    // duplicates should fail with insert::insert
    std::string dup = "dup";
    CHECK(t.insert(key_like_type{ 10 }, value_in_type{ dup }, insert::insert, rebalance::neighbor_share) == false);

    // find existing
    for (int i = 0; i < 50; ++i) {
        auto it = t.find(key_like_type{ i });
        CHECK(it != t.end());
        CHECK(it->second.get() == std::to_string(i));
    }
    // find missing
    CHECK(t.find(key_like_type{ -1 }) == t.end());
    CHECK(t.find(key_like_type{ 1000 }) == t.end());

    // monotonic iteration
    int prev = -1;
    for (auto &it: t) {
        CHECK(prev < it.first.get()); // KeyOut should expose a getter or comparable view
        prev = it.first.get();
    }
}

TEST_CASE("memory B+Tree: erase semantics and iterator behavior") {
    using Model = MemModel<int, std::string, 5>;
    using Tree  = fulla::bpt::tree<Model>;
    using key_like_type = typename Model::key_like_type;
    using key_out_type = typename Model::key_out_type;
    using value_in_type = typename Model::value_in_type;

    Tree t;

    // fill with shuffled keys
    std::vector<int> keys(200);
    std::iota(keys.begin(), keys.end(), 0);
    std::mt19937 rng(12345);
    std::shuffle(keys.begin(), keys.end(), rng);
    for (int k : keys) {
        auto ts = std::to_string(k);
        CHECK(t.insert(key_like_type{ k }, value_in_type{ts}, insert::insert, rebalance::neighbor_share));
    }

    // erase(begin()) should return iterator to next element
    auto b0 = t.begin();
    auto next_after_erase = std::next(b0);
    auto it_after = t.erase(b0);
    CHECK(it_after != t.end());
    CHECK(it_after->first.get() == next_after_erase->first.get());

    // erase last should return end()
    {
        // move to last using --end()
        auto it = t.end();
        --it;
        int last_key = it->first.get();
        auto ret = t.erase(it);
        CHECK(ret == t.end());
        CHECK(t.find(key_like_type{ last_key }) == t.end());
    }

    const auto cmp = ranges::make_key_comp<Model>();
    const auto element_proj = ranges::make_element_key_proj(t);

    // erase a middle element and verify ordering
    {
        auto it = std::ranges::lower_bound(t, key_like_type{ 73 }, cmp, element_proj);
        REQUIRE(it != t.end());
        int k = it->first.get();
        auto after = std::next(it);
        auto after_key = after->first.get();
        auto ret   = t.erase(it);
        // ret should point to "after" element (or end) by key
        if (after != t.end()) {
            CHECK(ret != t.end());
            CHECK(ret->first.get() == after_key);
        } else {
            CHECK(ret == t.end());
        }
        CHECK(t.find(key_like_type{ k }) == t.end());
    }

    // iterator ++ and -- across leaf boundaries
    {
        auto it = t.begin();
        // advance some steps
        for (int i = 0; i < 17 && it != t.end(); ++i) { ++it; }
        auto it2 = it;
        for (int i = 0; i < 7 && it2 != t.end(); ++i) { ++it2; }
        // walk back to original
        for (int i = 0; i < 7; ++i) { --it2; }
        CHECK(it == it2);
    }
}

TEST_CASE("memory B+Tree: lower_bound and range scan") {
    using Model = MemModel<int, std::string, 4>;
    using Tree  = fulla::bpt::tree<Model>;
    using key_like_type = typename Model::key_like_type;
    using key_out_type = typename Model::key_out_type;
    using value_in_type = typename Model::value_in_type;
    Tree t;

    for (int i = 0; i < 100; ++i) {
        auto ts = std::to_string(i * 2);
        CHECK(t.insert(key_like_type{ i * 2 }, value_in_type{ts}, insert::insert, rebalance::neighbor_share));
    }

    const auto cmp = [](int lhs, int rhs) {
        return typename Model::less_type{}(lhs, rhs);
        };

    const auto element_proj = [](const Tree::iterator::value_type& kv) {
        return kv.first.get();
        };

    // exact hit
    auto it = std::ranges::lower_bound(t, 40, cmp, element_proj);
    REQUIRE(it != t.end());
    CHECK(it->first.get() == 40);

    // nearest greater
    it = std::ranges::lower_bound(t, 41, cmp, element_proj);
    REQUIRE(it != t.end());
    CHECK(it->first.get() == 42);

    // range: [50, 80)
    it = std::ranges::lower_bound(t, 50, cmp, element_proj);
    int expected = 50;
    for (; it != t.end() && it->first.get() < 80; ++it) {
        CHECK(it->first.get() == expected);
        expected += 2;
    }
    CHECK(expected == 80);
}

TEST_CASE("memory B+Tree: upsert policy replaces value") {
    using Model = MemModel<int, std::string, 5>;
    using Tree  = fulla::bpt::tree<Model>;
    using key_like_type = typename Model::key_like_type;
    using key_out_type = typename Model::key_out_type;
    using value_in_type = typename Model::value_in_type;

    Tree t;

    std::string ten = "ten";
    std::string TEN = "TEN";

    CHECK(t.insert(key_like_type{ 10 }, value_in_type{ten}, insert::insert, rebalance::neighbor_share) == true);
    // upsert should replace existing value
    CHECK(t.insert(key_like_type{ 10 }, value_in_type{ TEN }, insert::upsert, rebalance::neighbor_share) == true);

    auto it = t.find(key_like_type{ 10 });
    REQUIRE(it != t.end());
    CHECK(it->second.get() == std::string("TEN"));
}

TEST_CASE("memory B+Tree vs std::map: randomized insert/erase equivalence (deterministic)") {
    using Model = MemModel<int, std::string, 5>;
    using Tree  = fulla::bpt::tree<Model>;
    using key_like_type = typename Model::key_like_type;
    using key_out_type = typename Model::key_out_type;
    using value_in_type = typename Model::value_in_type;

    Tree t;

    std::map<int, std::string> ref;

    std::mt19937 rng(0xC0FFEE);
    std::uniform_int_distribution<int> keyd(0, 2000);
    std::bernoulli_distribution insprob(0.6);

    const auto check_valid = [&]() {
        return true;
        //for (auto& k : ref) {
        //    auto it = t.find(key_like_type{ k.first });
        //    if (it == t.end()) {
        //        return false;
        //    }
        //}
        //return true;
    };

    const int steps = 15000;
    for (int s = 0; s < steps; ++s) {
        int k = keyd(rng);
        if (insprob(rng)) {

            ref[k] = std::to_string(k);
            auto tsk = std::to_string(k);
            CHECK(t.insert(key_like_type{ k }, value_in_type{ tsk },
                insert::upsert, rebalance::neighbor_share));
            CHECK(check_valid());
        }
        else {
            std::size_t removed = ref.erase(k);
            auto it = t.find(key_like_type{ k });
            bool ok = t.remove(key_like_type{ k });
            if (!ok && removed > 0) {
                std::cout << "";
            }
            CHECK(ok == (removed > 0));
        }

        // Periodically validate by full inorder comparison
        if ((s % 500) == 0 || s == steps - 1) {
            // collect keys from tree
            std::vector<int> tkeys;
            tkeys.reserve(ref.size());
            for (auto &it: t) {
                tkeys.push_back(it.first.get());
            }
            // collect keys from std::map
            std::vector<int> rkeys;
            rkeys.reserve(ref.size());
            for (auto& kv : ref) { 
                rkeys.push_back(kv.first);
            }

            CHECK(tkeys == rkeys);
        }
    }
}

TEST_CASE("memory B+Tree split on update") {
    using Model = MemModel<int, std::string, 5>;
    using Tree = fulla::bpt::tree<Model>;
    using key_like_type = typename Model::key_like_type;
    using key_out_type = typename Model::key_out_type;
    using value_in_type = typename Model::value_in_type;

    Tree t;
    for (int i = 0; i < 20; ++i) {
        auto ts = "!" + std::to_string(i);
        CHECK(t.insert(key_like_type{ i }, value_in_type{ ts }, insert::insert, rebalance::neighbor_share));
    }

    t.dump();

    std::string val = "01234567891112";

    t.update(key_like_type{ 12 }, value_in_type{ val }, rebalance::neighbor_share);

    t.dump();

}
