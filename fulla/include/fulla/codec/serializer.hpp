/*
 * File: serializer.hpp
 * Author: newenclave
 * GitHub: https://github.com/newenclave
 * Created: 2025-10-25
 * License: MIT
 */

#pragma once

#include <cstdint>
#include <tuple>

#include "fulla/core/bytes.hpp"
#include "fulla/core/types.hpp"
#include "fulla/core/byteorder.hpp"

namespace fulla::codec {

	namespace byteorder = core::byteorder;

	template <typename T>
	class serializer;

	template <byteorder::Word WordT>
	struct integer_serializer {

		using value_type = WordT;
		constexpr static std::size_t store(value_type val, core::byte* where) {
			byteorder::native_to_le<value_type>(val, where);
			return sizeof(value_type);
		}

		constexpr static std::tuple<value_type, std::size_t> load(const core::byte* where, std::size_t) {
			const auto val = byteorder::le_to_native<value_type>(where);
			return { val, sizeof(value_type) };
		}

		constexpr static std::size_t size(const value_type&) {
			return sizeof(value_type);
		}
	};

	template <typename WordT>
	struct float_serializer {
		using value_type = WordT;
		constexpr static std::size_t store(value_type val, core::byte* where) {
			static_assert(sizeof(WordT) == sizeof(std::uint32_t) || sizeof(WordT) == sizeof(std::uint64_t), "Unsupported float size");
			if constexpr (sizeof(WordT) == sizeof(std::uint32_t)) {
				std::uint32_t int_val;
				std::memcpy(&int_val, &val, sizeof(WordT));
				byteorder::native_to_le<std::uint32_t>(int_val, where);
				return sizeof(std::uint32_t);
			}
			else {
				std::uint64_t int_val;
				std::memcpy(&int_val, &val, sizeof(WordT));
				byteorder::native_to_le<std::uint64_t>(int_val, where);
				return sizeof(std::uint64_t);
			}
		}
		constexpr static std::tuple<value_type, std::size_t> load(const core::byte* where, std::size_t) {
			if constexpr (sizeof(WordT) == sizeof(std::uint32_t)) {
				auto int_val = byteorder::le_to_native<std::uint32_t>(where);
				value_type val;
				std::memcpy(&val, &int_val, sizeof(WordT));
				return { val, sizeof(std::uint32_t) };
			}
			else {
				auto int_val = byteorder::le_to_native<std::uint64_t>(where);
				value_type val;
				std::memcpy(&val, &int_val, sizeof(WordT));
				return { val, sizeof(std::uint64_t) };
			}
		}
		constexpr static std::size_t size(const value_type&) {
			return sizeof(value_type);
		}
	};

	template <>
	struct serializer<std::int16_t> : public integer_serializer<std::int16_t> {};
	template <>
	struct serializer<std::int32_t> : public integer_serializer<std::int32_t> {};
	template <>
	struct serializer<std::int64_t> : public integer_serializer<std::int64_t> {};

	template <>
	struct serializer<std::uint16_t> : public integer_serializer<std::uint16_t> {};
	template <>
	struct serializer<std::uint32_t> : public integer_serializer<std::uint32_t> {};
	template <>
	struct serializer<std::uint64_t> : public integer_serializer<std::uint64_t> {};

	template <>
	struct serializer<float> : public float_serializer<float> {};
	template <>
	struct serializer<double> : public float_serializer<double> {};

	template <>
	struct serializer<std::string> {
		
		using value_type = std::string;
		
		static std::size_t store(const value_type& val, core::byte* where) {
			
			const std::uint32_t size = static_cast<std::uint32_t>(val.size() + 1);
			const auto len_size = serializer<std::uint32_t>::size(size);

			const auto shift = serializer<std::uint32_t>::store(size + len_size, where);
			where += shift;

			std::memcpy(where, val.data(), val.size());
			where[val.size()] = core::byte{0}; // null-terminator
			return shift + val.size() + 1;
		}
		
		static std::tuple<value_type, std::size_t> load(const core::byte* where, std::size_t size) {
			auto [str_size, shift] = serializer<std::uint32_t>::load(where, size);
			const auto len_size = serializer<std::uint32_t>::size(static_cast<std::uint32_t>(size));
			where += shift;
			value_type val(reinterpret_cast<const char*>(where), str_size - len_size - 1); // exclude null-terminator
			return { val, str_size };
		}
		
		static std::size_t size(const value_type& val) {
			const auto size = static_cast<std::uint32_t>(val.size());
			return serializer<std::uint32_t>::size(size) + val.size() + 1;  // +1 for null-terminator
		}
	};

	template <>
	class serializer<core::byte_view> {
		public:
		using value_type = core::byte_view;
		static std::size_t store(const value_type& val, core::byte* where) {
			const std::uint32_t size = static_cast<std::uint32_t>(val.size());
			const auto len_size = serializer<std::uint32_t>::size(size);

			const auto shift = serializer<std::uint32_t>::store(size + len_size, where);
			where += shift;
			std::memcpy(where, val.data(), val.size());
			return shift + val.size();
		}
		static std::tuple<value_type, std::size_t> load(const core::byte* where, std::size_t size) {
			auto [blob_size, shift] = serializer<std::uint32_t>::load(where, size);
			const auto len_size = serializer<std::uint32_t>::size(static_cast<std::uint32_t>(size));
			where += shift;
			value_type val(where, blob_size - len_size);
			return { val, blob_size };
		}

		static std::size_t size(const value_type& val) {
			const auto size = static_cast<std::uint32_t>(val.size());
			const auto len_size = serializer<std::uint32_t>::size(size);
			return len_size + val.size();
		}
	};   
}