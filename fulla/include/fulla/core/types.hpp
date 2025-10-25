/*
 * File: types.hpp
 * Author: newenclave
 * GitHub: https://github.com/newenclave
 * Created: 2025-10-25
 * License: MIT
 */

#pragma once

#include "fulla/core/byteorder.hpp"

namespace fulla::core {
	using word_u16 = byteorder::word_le<std::uint16_t>;
	using word_u32 = byteorder::word_le<std::uint32_t>;
	using word_u64 = byteorder::word_le<std::uint64_t>;
	using word_i16 = byteorder::word_le<std::int16_t>;
	using word_i32 = byteorder::word_le<std::int32_t>;
	using word_i64 = byteorder::word_le<std::int64_t>;
} // namespace fulla::db::core::types
