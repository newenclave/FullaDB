/*
 * File: prop.hpp
 * Author: newenclave
 * GitHub: https://github.com/newenclave
 * Created: 2025-10-25
 * License: MIT
 */

#pragma once

#include <string>
#include <vector>
#include <cstring>
#include <type_traits>

#include "fulla/core/bytes.hpp"
#include "fulla/codec/serializer.hpp"
#include "fulla/codec/prop_types.hpp"
#include "fulla/codec/data_serializer.hpp"

namespace fulla::codec::prop {

    // Re-export core byte types for convenience.
    using core::byte;
    using core::byte_view;
    using core::byte_span;
    using byte_buffer = std::vector<byte>;

    // ----- Value wrappers (lightweight "property" types) -----
    struct ui32 { std::uint32_t v = 0; };
    struct ui64 { std::uint64_t v = 0; };
    struct i32  { std::int32_t  v = 0; };
    struct i64  { std::int64_t  v = 0; };
    struct fp32 { float  v = 0.0f; };
    struct fp64 { double v = 0.0; };
    struct str  { std::string v{}; };
    struct blob { byte_view  v{}; };

    struct tuple; // forward

    // A fully serialized record (concatenation of one or more typed values).
    struct rec {
        byte_buffer buf{};
        byte_view view() const { return { buf.data(), buf.size() }; }
    };

    // ----- Append helpers: serialize a single value into data_serializer -----
    inline void append_to(data_serializer& ds, const ui32& x) { ds.store<std::uint32_t>(x.v); }
    inline void append_to(data_serializer& ds, const ui64& x) { ds.store<std::uint64_t>(x.v); }
    inline void append_to(data_serializer& ds, const i32&  x) { ds.store<std::int32_t>(x.v); }
    inline void append_to(data_serializer& ds, const i64&  x) { ds.store<std::int64_t>(x.v); }
    inline void append_to(data_serializer& ds, const fp32& x) { ds.store<float>(x.v); }
    inline void append_to(data_serializer& ds, const fp64& x) { ds.store<double>(x.v); }
    inline void append_to(data_serializer& ds, const str&  x) { ds.store<std::string>(x.v); }
    inline void append_to(data_serializer& ds, const blob& x) { ds.store_blob(x.v.data(), x.v.size(), data_type::blob); }

    // A serialized tuple: len-prefixed payload with {record, record, ...} and type tag = tuple.
    struct tuple {
        byte_buffer buf{};

        template <class... Ts>
        explicit tuple(Ts&&... xs) {
            data_serializer inner;
            (append_to(inner, std::forward<Ts>(xs)), ...);

            data_serializer outer;
            outer.store_blob(inner.data(), inner.size(), data_type::tuple);

            buf.resize(outer.size());
            std::memcpy(buf.data(), outer.data(), outer.size());
        }

        byte_view view() const { return { buf.data(), buf.size() }; }
    };

    inline void append_to(data_serializer& ds, const tuple& t) {
        ds.append(t.buf.data(), t.buf.size()); // payload already contains type-tagged tuple
    }

    // Build a full record from a sequence of values (ui32, str, tuple, ...). NOT A TUPLE
    template <typename... Ts>
    rec make_record(Ts&&... xs) {
        data_serializer ds;
        (append_to(ds, std::forward<Ts>(xs)), ...);

        rec r;
        r.buf.resize(ds.size());
        std::memcpy(r.buf.data(), ds.data(), ds.size());
        return r;
    }

} // namespace fulla::codec::prop
