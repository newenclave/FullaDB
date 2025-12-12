/*
 * File: policies.hpp
 * Author: newenclave
 * GitHub: https://github.com/newenclave
 * Created: 2025-11-01
 * License: MIT
 */

#pragma once
namespace fulla::bpt::policies {

    enum class rebalance {
        force_split,
        neighbor_share,
        local_rebalance,
    };

    enum class insert {
        insert,
        upsert,
    };

} // namespace fulla::bpt::policies
