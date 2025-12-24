/*
 * File: core/bitset.hpp
 * Author: newenclave
 * GitHub: https://github.com/newenclave
 * Created: 2025-10-25
 * License: MIT
 */

#pragma once

#include <bit>
#include <concepts>
#include <optional>

#include "fulla/core/bytes.hpp"
#include "fulla/core/debug.hpp"

namespace fulla::core {

	using core::byte;
	using core::word_u32;
	using core::word_u64;

	template <typename WordT>
	inline std::tuple<std::size_t, std::size_t> max_objects_by_words(std::size_t capacity, std::size_t object_size)
	{
		if (capacity == 0 || object_size == 0) {
			return { 0, 0 };
		}

		constexpr std::size_t word_bytes = sizeof(WordT);
		constexpr std::size_t bits_per_word = word_bytes * CHAR_BIT;

		const auto ceil = [](auto value, auto to) -> std::size_t { return (value + to - 1) / to; };

		const auto bitmap_size_bytes = [&ceil, bits_per_word, word_bytes](std::size_t n) -> std::size_t {
			if (n == 0) {
				return 0;
			}
			std::size_t words = ceil(n, bits_per_word);
			return words * word_bytes;
		};

		std::size_t low = 0;
		std::size_t high = capacity / object_size;

		std::size_t best = 0;
		while (low <= high) {
			std::size_t mid = low + (high - low) / 2;
			std::size_t total = mid * object_size + bitmap_size_bytes(mid);
			if (total <= capacity) {
				best = mid;
				low = mid + 1;
			}
			else {
				high = mid - 1;
			}
		}

		const std::size_t bitmap_words = (best == 0 ? 0 : ceil(best, bits_per_word));
		return { bitmap_words, best };
	}

	template <typename WordT = core::word_u32, typename SpanT = core::byte_span>
		requires (std::same_as<SpanT, core::byte_view> || std::same_as<SpanT, core::byte_span>) &&
				 (std::same_as<WordT, core::word_u32> || std::same_as<WordT, core::word_u64>)
	class bitset {
	public:
		using data_type = WordT;
		using word_type = typename data_type::word_type;

		constexpr static std::size_t data_bits = sizeof(data_type) * CHAR_BIT;
		using span_type = SpanT;

		using data_container = std::span<data_type>;
		using const_data_container = std::span<const data_type>;

		using data_ptr = std::conditional_t<
			std::same_as<SpanT, core::byte_view>,
			const data_type *,
			data_type *
		>;

		using container_type = std::conditional_t<
			std::same_as<SpanT, core::byte_view>,
			const_data_container,
			data_container
		>;

        bitset() = delete;

		bitset(bitset&&) = default;
		bitset& operator = (bitset&&) = default;
		bitset(const bitset&) = default;
		bitset& operator = (const bitset&) = default;

		bitset(span_type container, std::size_t maximum)
			: buckets_(reinterpret_cast<data_ptr>(container.data()), container.size() / sizeof(data_type))
			, maximum_(std::min(buckets_.size() * data_bits, maximum))
		{
			DB_ASSERT(container.size() % sizeof(data_type) == 0, "Something wrong with the length");
		}

		inline std::size_t bits_count() const noexcept {
			return maximum_;
		}

		inline void set(std::size_t bit_pos) {
			if (bits_count() <= bit_pos) {
				return;
			}
			const auto bucket = bit_pos / data_bits;
			const auto pos = bit_pos % data_bits;
			buckets_[bucket] = buckets_[bucket].get() | (word_type{1} << pos);
		}

		inline void clear(std::size_t bit_pos) {
			if (bits_count() <= bit_pos) {
				return;
			}
			const auto bucket = bit_pos / data_bits;
			const auto pos = bit_pos % data_bits;
			buckets_[bucket] = buckets_[bucket].get() & ~(word_type{1} << pos);
		}

		inline void reset() {
			for (std::size_t b = 0; b < buckets_.size(); ++b) {
				buckets_[b] = 0;
			}
		}

		[[nodiscard]]
		inline bool test(std::size_t bit_pos) const {
			if (bits_count() <= bit_pos) {
				return 0;
			}
			const auto bucket = bit_pos / data_bits;
			const auto pos = bit_pos % data_bits;
			return buckets_[bucket].get() & (word_type{1} << pos);
		}

		std::optional<std::size_t> find_zero_bit() const {
			for (std::size_t b = 0; b < buckets_.size(); ++b) {
				auto bucket = buckets_[b].get();

				if (bucket == static_cast<word_type>(~word_type(0))) {
					continue;
				}

				int first_zero = 0;

				if constexpr (std::is_same_v<word_type, uint32_t>) {

#if defined(__GNUC__)
					first_zero = __builtin_ffs(~bucket) - 1; // __builtin_ffs is 1-based value
#elif defined(_MSC_VER) // for testing purposes
					unsigned long pos;
					_BitScanForward(&pos, ~bucket);
					first_zero = pos;
#else
					for (first_zero = 0; first_zero < data_bits; ++first_zero) {
						if (!(bucket & (word_type{ 1 } << first_zero))) {
							break;
						}
					}
#endif
				}
				else if constexpr (std::is_same_v<word_type, uint64_t>) {
#if defined(__GNUC__)
					first_zero = __builtin_ffsll(~bucket) - 1; // __builtin_ffs is 1-based value
#elif defined(_MSC_VER) // for testing purposes
					unsigned long pos;
					_BitScanForward64(&pos, ~bucket);
					first_zero = pos;
#else
					for (first_zero = 0; first_zero < data_bits; ++first_zero) {
						if (!(bucket & (word_type{ 1 } << first_zero))) {
							break;
						}
					}
#endif
				}
				else {
					for (first_zero = 0; first_zero < data_bits; ++first_zero) {
						if (!(bucket & (word_type{ 1 } << first_zero))) {
							break;
						}
					}
				}
				std::size_t bit_pos = b * data_bits + first_zero;

				if (bit_pos < bits_count()) {
					return { bit_pos };
				}
			}
			return std::nullopt;
		}

		std::optional<std::size_t> find_set_bit() const {
			for (std::size_t b = 0; b < buckets_.size(); ++b) {
				auto bucket = buckets_[b].get();

				if (bucket == 0) {
					continue;
				}

				int first_set = 0;
				if constexpr (std::is_same_v<word_type, std::uint32_t>) {
#if defined(__GNUC__)
					first_set = __builtin_ffs(bucket) - 1; // __builtin_ffs is 1-based value
#elif defined(_MSC_VER)
					unsigned long pos;
					_BitScanForward(&pos, bucket);
					first_set = pos;
#else
					for (first_set = 0; first_set < data_bits; ++first_set) {
						if (bucket & (data_type{ 1 } << first_set)) {
							break;
						}
					}
#endif
				}
				else if constexpr (std::is_same_v<word_type, std::uint64_t>) {
#if defined(__GNUC__)
					first_set = __builtin_ffsll(bucket) - 1; // __builtin_ffs is 1-based value
#elif defined(_MSC_VER)
					unsigned long pos;
					_BitScanForward64(&pos, bucket);
					first_set = pos;
#else
					for (first_set = 0; first_set < data_bits; ++first_set) {
						if (bucket & (word_type{ 1 } << first_set)) {
							break;
						}
					}
#endif
				}
				else {
					for (first_set = 0; first_set < data_bits; ++first_set) {
						if (bucket & (word_type{ 1 } << first_set)) {
							break;
						}
					}
				}
				std::size_t bit_pos = b * data_bits + first_set;

				if (bit_pos < bits_count()) {
					return { bit_pos };
				}
			}
			return std::nullopt;
		}

		bool is_valid(std::size_t pos) const noexcept {
			return (pos < bits_count());
		}

#ifdef __cpp_lib_bitops
		std::size_t popcount() const {
			std::size_t total = 0;
			for (std::size_t b = 0; b < buckets_.size(); ++b) {
				total += std::popcount(buckets_[b].get());
			}
			return total;
		}
#else
		std::size_t popcount() const {
			std::size_t result = 0;

			for (std::size_t b = 0; b < buckets_.size(); ++b) {
				auto bucket = buckets_[b].get();

				if constexpr (std::is_same_v<word_type, uint32_t>) {
#if defined(__GNUC__)
					result += __builtin_popcount(bucket);
#elif defined(_MSC_VER)
					result += __popcnt(bucket);
#else
					while (bucket) {
						bucket &= (bucket - 1);
						++result;
					}
#endif
				}
				else if constexpr (std::is_same_v<word_type, uint64_t>) {
#if defined(__GNUC__)
					result += __builtin_popcountll(bucket);
#elif defined(_MSC_VER)
					result += __popcnt64(bucket);
#else
					while (bucket) {
						bucket &= (bucket - 1);
						++result;
					}
#endif
				}
				else {
					while (bucket) {
						bucket &= (bucket - 1);
						++result;
					}
				}
			}

			return result;
		}
#endif

	private:
		container_type buckets_ = {};
		const std::size_t maximum_ = 0;
	};
} // namespace fulla::core
