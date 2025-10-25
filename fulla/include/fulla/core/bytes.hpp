/*
 * File: bytes.hpp
 * Author: newenclave
 * GitHub: https://github.com/newenclave
 * Created: 2025-10-25
 * License: MIT
 */
#pragma once

#include <cstdint>
#include <vector>
#include <span>
#include <concepts>

namespace fulla::core {
	
    using byte = std::byte;
	using byte_buffer = std::vector<byte>;
	using byte_view = std::span<const byte>;
	using byte_span = std::span<byte>;

	template <typename T>
	constexpr inline T align_up(T value, std::size_t align)
		requires std::unsigned_integral<T>
	{
		return (value + static_cast<T>(align - 1)) & ~static_cast<T>(align - 1);
	}
	
	template <typename T>
	constexpr inline T align_down(T value, std::size_t align)
		requires std::unsigned_integral<T>
	{
		return value & ~static_cast<std::uintptr_t>(align - 1u);
	}    

} // namespace fulla::db::core
