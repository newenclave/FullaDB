/*
 * File: prop.hpp
 * Author: newenclave
 * GitHub: https://github.com/newenclave
 * Created: 2025-10-25
 * License: MIT
 */

#pragma once

#include "fulla/core/types.hpp"
#include "fulla/core/byteorder.hpp"
#include "fulla/core/pack.hpp"

namespace fulla::codec {

    using core::byte;
    using core::byte;
    using core::byte_view;
    using core::byte_span;
    using core::word_u16;

	enum class data_type : word_u16::word_type {
		undefined = 0,
        nill = 1,
		string = 2,
		i32 = 3,
		ui32 = 4,
		i64 = 5,
		ui64 = 6,
		fp32 = 7,
		fp64 = 8,
		blob = 9,
		tuple = 10,
	};

	FULLA_PACKED_STRUCT_BEGIN
    struct serialized_data_header {
		using word_type = word_u16::word_type;
		word_u16 type = {0}; // data_type
		word_u16 reserved = {0};
		core::byte* data() { return reinterpret_cast<core::byte*>(this) + header_size(); }
		const core::byte* data() const { return reinterpret_cast<const core::byte*>(this) + header_size(); }
		static constexpr std::size_t header_size() noexcept { return sizeof(serialized_data_header); }
	} FULLA_PACKED;
	FULLA_PACKED_STRUCT_END

} // namespace fulla::codec