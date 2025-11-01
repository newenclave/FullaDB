/*
 * File: ranges.hpp
 * Author: newenclave
 * GitHub: https://github.com/newenclave
 * Created: 2025-11-01
 * License: MIT
 */

#pragma once

#include "fulla/bpt/concepts.hpp"

namespace fulla::bpt::ranges {
    template <concepts::BptModel ModelT>
    [[nodiscard]] inline auto make_key_comp() {
        using less_type = typename ModelT::less_type;
        using key_like_type = typename ModelT::key_like_type;
        return [](const key_like_type& a, const key_like_type& b) {
            return less_type{}(a, b);
        };
    }

    // Project element (pair<key_out_type, value_out_type>) -> KeyT via model
    template <class Tree>
    [[nodiscard]] inline auto make_element_key_proj(const Tree& t) {
        using model_type = typename Tree::model_type;
        return [&t](const typename Tree::iterator::value_type& kv) {
            return t.get_model().key_out_as_like(kv.first);
        };
    }

} // namespace fulla::bpt::ranges