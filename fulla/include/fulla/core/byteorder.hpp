/*
 * File: byteorder.hpp
 * Author: newenclave
 * GitHub: https://github.com/newenclave
 * Created: 2025-10-25
 * License: MIT
 */

#pragma once

#include <bit>
#include "fulla/core/bytes.hpp"

namespace fulla::core::byteorder {

	template <typename T>
	concept SignedWord = std::is_signed_v<T> &&
		((sizeof(T) == 2) || (sizeof(T) == 4) || (sizeof(T) == 8));

	template <typename T>
	concept UnsignedWord = std::is_unsigned_v<T> &&
		((sizeof(T) == 2) || (sizeof(T) == 4) || (sizeof(T) == 8));

	template <typename T>
	concept Word = SignedWord<T> || UnsignedWord<T>;

	template <UnsignedWord WordT>
	constexpr inline WordT le_to_native_unsigned(const core::byte* mem) {

		if constexpr (std::endian::native != std::endian::little) {

			WordT result = static_cast<std::uint32_t>(mem[0])
				| (static_cast<std::uint32_t>(mem[1]) << 8);

			if constexpr (sizeof(WordT) > 2) {

				result |= (static_cast<std::uint32_t>(mem[2]) << 16)
					| (static_cast<std::uint32_t>(mem[3]) << 24);

				if constexpr (sizeof(WordT) > 4) {
					result |= (static_cast<std::uint64_t>(mem[4]) << 32)
						| (static_cast<std::uint64_t>(mem[5]) << 40)
						| (static_cast<std::uint64_t>(mem[6]) << 48)
						| (static_cast<std::uint64_t>(mem[7]) << 56);
				}
			}
			return result;
		}
		else {
			WordT result;
			std::memcpy(&result, mem, sizeof(WordT));
			return result;
		}
	}

	template <UnsignedWord WordT>
	constexpr inline void native_to_le_unsigned(WordT val, core::byte* mem) {
		if constexpr (std::endian::native != std::endian::little) {
			mem[0] = static_cast<core::byte>(val & 0xFF);
			mem[1] = static_cast<core::byte>((val >> 8) & 0xFF);

			if constexpr (sizeof(WordT) > 2) {
				mem[2] = static_cast<core::byte>((val >> 16) & 0xFF);
				mem[3] = static_cast<core::byte>((val >> 24) & 0xFF);
				if constexpr (sizeof(WordT) > 4) {
					mem[4] = static_cast<core::byte>((val >> 32) & 0xFF);
					mem[5] = static_cast<core::byte>((val >> 40) & 0xFF);
					mem[6] = static_cast<core::byte>((val >> 48) & 0xFF);
					mem[7] = static_cast<core::byte>((val >> 56) & 0xFF);
				}
			}
		}
		else {
			std::memcpy(mem, &val, sizeof(WordT));
		}
	}

	template <UnsignedWord WordT>
	constexpr inline WordT be_to_native_unsigned(const core::byte* mem) {

		if constexpr (std::endian::native != std::endian::big) {
			WordT result = static_cast<std::uint32_t>(mem[sizeof(WordT) - 1])
				| (static_cast<std::uint32_t>(mem[sizeof(WordT) - 2]) << 8);
			if constexpr (sizeof(WordT) > 2) {
				result |= (static_cast<std::uint32_t>(mem[sizeof(WordT) - 3]) << 16)
					| (static_cast<std::uint32_t>(mem[sizeof(WordT) - 4]) << 24);
				if constexpr (sizeof(WordT) > 4) {
					result |= (static_cast<std::uint64_t>(mem[sizeof(WordT) - 5]) << 32)
						| (static_cast<std::uint64_t>(mem[sizeof(WordT) - 6]) << 40)
						| (static_cast<std::uint64_t>(mem[sizeof(WordT) - 7]) << 48)
						| (static_cast<std::uint64_t>(mem[sizeof(WordT) - 8]) << 56);
				}
			}
			return result;
		}
		else {
			WordT result;
			std::memcpy(&result, mem, sizeof(WordT));
			return result;
		}
	}

	template <UnsignedWord WordT>
	constexpr inline void native_to_be_unsigned(WordT val, core::byte* mem) {
		if constexpr (std::endian::native != std::endian::big) {
			if constexpr (sizeof(WordT) > 4) {
				mem[0] = static_cast<core::byte>((val >> 56) & 0xFF);
				mem[1] = static_cast<core::byte>((val >> 48) & 0xFF);
				mem[2] = static_cast<core::byte>((val >> 40) & 0xFF);
				mem[3] = static_cast<core::byte>((val >> 32) & 0xFF);
			}
			if constexpr (sizeof(WordT) > 2) {
				mem[sizeof(WordT) - 4] = static_cast<core::byte>((val >> 24) & 0xFF);
				mem[sizeof(WordT) - 3] = static_cast<core::byte>((val >> 16) & 0xFF);
			}
			mem[sizeof(WordT) - 2] = static_cast<core::byte>((val >> 8) & 0xFF);
			mem[sizeof(WordT) - 1] = static_cast<core::byte>(val & 0xFF);
		}
		else {
			std::memcpy(mem, &val, sizeof(WordT));
		}
	}

	template <SignedWord WordT>
	constexpr inline WordT le_to_native_signed(const core::byte* mem) {
		using unsigned_type = std::make_unsigned_t<WordT>;
		const unsigned_type uns = le_to_native_unsigned<unsigned_type>(mem);
		return std::bit_cast<WordT>(uns);
	}
	
	template <SignedWord WordT>
	constexpr inline void native_to_le_signed(WordT val, core::byte* mem) {
		using unsigned_type = std::make_unsigned_t<WordT>;
		const unsigned_type uns = std::bit_cast<unsigned_type>(val);
		native_to_le_unsigned<unsigned_type>(uns, mem);
	}

	template <SignedWord WordT>
	constexpr inline WordT be_to_native_signed(const core::byte* mem) {
		using unsigned_type = std::make_unsigned_t<WordT>;
		const unsigned_type uns = be_to_native_unsigned<unsigned_type>(mem);
		return std::bit_cast<WordT>(uns);
	}

	template <SignedWord WordT>
	constexpr inline void native_to_be_signed(WordT val, core::byte* mem) {
		using unsigned_type = std::make_unsigned_t<WordT>;
		const unsigned_type uns = std::bit_cast<unsigned_type>(val);
		native_to_be_unsigned<unsigned_type>(uns, mem);
	}

	template <Word WordT> 
	constexpr inline WordT le_to_native(const core::byte* mem) {
		if constexpr (std::is_unsigned_v<WordT>) {
			return le_to_native_unsigned<WordT>(mem);
		}
		else {
			return le_to_native_signed<WordT>(mem);
		}
	}

	template <Word WordT>
	constexpr inline void native_to_le(WordT val, core::byte* mem) {
		if constexpr (std::is_unsigned_v<WordT>) {
			native_to_le_unsigned<WordT>(val, mem);
		}
		else {
			native_to_le_signed<WordT>(val, mem);
		}
	}

	template <Word WordT>
	constexpr inline WordT be_to_native(const core::byte* mem) {
		if constexpr (std::is_unsigned_v<WordT>) {
			return be_to_native_unsigned<WordT>(mem);
		}
		else {
			return be_to_native_signed<WordT>(mem);
		}
	}

	template <Word WordT>
	constexpr inline void native_to_be(WordT val, core::byte* mem) {
		if constexpr (std::is_unsigned_v<WordT>) {
			native_to_be_unsigned<WordT>(val, mem);
		}
		else {
			native_to_be_signed<WordT>(val, mem);
		}
	}

	template <typename WordT = std::uint32_t>
	class word_le {
	public:

		using word_type = WordT;

		word_le() = default;
		word_le(word_type val) {
			from_native(val);
		}
		word_le(word_le&&) = default;
		word_le& operator = (word_le&&) = default;
		word_le(const word_le&) = default;
		word_le& operator = (const word_le&) = default;

		operator word_type() const {
			return to_native();
		}

		word_le& operator = (word_type val) {
			from_native(val);
			return *this;
		}

		word_type get() const {
			return to_native();
		}

		constexpr static auto max() {
			return std::numeric_limits<word_type>::max();
		}

		constexpr static auto min() {
			return std::numeric_limits<word_type>::min();
		}

	private:

		constexpr word_type to_native() const {
			if constexpr (std::is_unsigned_v<word_type>) {
				return byteorder::le_to_native_unsigned<word_type>(reinterpret_cast<const core::byte*>(&bytes_[0]));
			}
			else {
				return byteorder::le_to_native_signed<word_type>(reinterpret_cast<const core::byte*>(&bytes_[0]));
			}
		}

		constexpr void from_native(word_type val) {
			if constexpr (std::is_unsigned_v<word_type>) {
				byteorder::native_to_le_unsigned<word_type>(val, reinterpret_cast<core::byte*>(&bytes_[0]));
			}
			else {
				byteorder::native_to_le_signed<word_type>(val, reinterpret_cast<core::byte*>(&bytes_[0]));
			}
		}

		core::byte bytes_[sizeof(word_type)];
	};

	template <typename WordT = std::uint32_t>
	class word_be {
		public:
		using word_type = WordT;
		word_be() = default;
		word_be(word_type val) {
			from_native(val);
		}
		word_be(word_be&&) = default;
		word_be& operator = (word_be&&) = default;
		word_be(const word_be&) = default;
		word_be& operator = (const word_be&) = default;
		operator word_type() const {
			return to_native();
		}
		word_be& operator = (word_type val) {
			from_native(val);
			return *this;
		}

		word_type get() const {
			return to_native();
		}

		constexpr static auto max() {
			return std::numeric_limits<word_type>::max();
		}

		constexpr static auto min() {
			return std::numeric_limits<word_type>::min();
		}

	private:
		constexpr word_type to_native() const {
			if constexpr (std::is_unsigned_v<word_type>) {
				return byteorder::be_to_native_unsigned<word_type>(reinterpret_cast<const core::byte*>(&bytes_[0]));
			}
			else {
				return byteorder::be_to_native_signed<word_type>(reinterpret_cast<const core::byte*>(&bytes_[0]));
			}
		}

		constexpr void from_native(word_type val) {
			if constexpr (std::is_unsigned_v<word_type>) {
				byteorder::native_to_be_unsigned<word_type>(val, reinterpret_cast<core::byte*>(&bytes_[0]));
			}
			else {
				byteorder::native_to_be_signed<word_type>(val, reinterpret_cast<core::byte*>(&bytes_[0]));
			}
		}
				
		core::byte bytes_[sizeof(word_type)];
	};    
} // namespace fulla::db::core::byteorder