/*
 * File: packed.hpp
 * Author: newenclave
 * GitHub: https://github.com/newenclave
 * Created: 2025-10-25
 * License: MIT
 */

#pragma once

#if defined(_MSC_VER)
#	define FULLA_PACKED_STRUCT_BEGIN __pragma(pack(push, 1))
#	define FULLA_PACKED_STRUCT_END   __pragma(pack(pop))
#	define FULLA_PACKED
#elif defined(__GNUC__) || defined(__clang__)
#	define FULLA_PACKED_STRUCT_BEGIN
#	define FULLA_PACKED_STRUCT_END
#	define FULLA_PACKED __attribute__((packed))
#else
#	define FULLA_PACKED_STRUCT_BEGIN
#	define FULLA_PACKED_STRUCT_END
#	define FULLA_PACKED
#	warning "Packed structs may not be fully supported"
#endif
