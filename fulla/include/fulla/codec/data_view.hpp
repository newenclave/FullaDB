/*
 * File: data_view.hpp
 * Author: newenclave
 * GitHub: https://github.com/newenclave
 * Created: 2025-10-25
 * License: MIT
 */

#pragma once

#include <string>
#include <span>
#include <compare>
#include <format>

#include "fulla/core/types.hpp"
#include "fulla/codec/data_serializer.hpp"

namespace fulla::codec {

    using core::byte;
    using core::byte_view;
    using core::byte_span;
    using core::byte_buffer;

	class data_view {
	public:

		data_view(const byte_buffer& data) : data_(data) {}

		data_view() = default;
		data_view(byte_view data) : data_(data) {}
		data_view(const data_view&) = default;
		data_view& operator = (const data_view&) = default;

		byte_view get() const { return data_; }

		static std::partial_ordering compare(byte_view lhs, byte_view rhs) {
			const auto lhs_type = get_type(lhs);
			const auto rhs_type = get_type(rhs);

			if (lhs_type == data_type::undefined || rhs_type == data_type::undefined) {
				return std::partial_ordering::unordered;
			}

			if (lhs_type != rhs_type) {
				return lhs_type <=> rhs_type;
			}

			const auto left_data = lhs.subspan(sizeof(serialized_data_header), get_size(lhs) - sizeof(serialized_data_header));
			const auto right_data = rhs.subspan(sizeof(serialized_data_header), get_size(rhs) - sizeof(serialized_data_header));

			switch (lhs_type) {
			case data_type::string:
				return compare_string(left_data, right_data);
			case data_type::blob:
				return compare_blob(left_data, right_data);
			case data_type::i32:
				return compare_word<std::int32_t>(left_data, right_data);
			case data_type::i64:
				return compare_word<std::int64_t>(left_data, right_data);
			case data_type::ui32:
				return compare_word<std::uint32_t>(left_data, right_data);
			case data_type::ui64:
				return compare_word<std::uint64_t>(left_data, right_data);
			case data_type::fp32:
				return compare_word<float>(left_data, right_data);
			case data_type::fp64:
				return compare_word<double>(left_data, right_data);
			case data_type::tuple:
				return compare_tuple(left_data, right_data);
			}
			return std::partial_ordering::unordered;
		}

		static std::ostream& debug_print(std::ostream &os, byte_view data, int indent = 0) {

			const auto dtype = get_type(data);
			const auto ldata = data.subspan(sizeof(serialized_data_header), get_size(data) - sizeof(serialized_data_header));

			debug_dump_field(os, dtype, ldata, indent);
			return os;
		}

	//private:

		static void debug_dump_field(std::ostream& os, data_type t, byte_view payload, int indent) {
			auto pad = std::string(indent, ' ');

			switch (t) {
			case data_type::i32: {
				auto [v, sz] = serializer<std::int32_t>::load(payload.data(), payload.size());
				os << pad << "i32: " << v << "\n";
				break;
			}
			case data_type::i64: {
				auto [v, sz] = serializer<std::int64_t>::load(payload.data(), payload.size());
				os << pad << "i64: " << v << "\n";
				break;
			}
			case data_type::ui32: {
				auto [v, sz] = serializer<std::uint32_t>::load(payload.data(), payload.size());
				os << pad << "ui32: " << v << "\n";
				break;
			}
			case data_type::ui64: {
				auto [v, sz] = serializer<std::uint64_t>::load(payload.data(), payload.size());
				os << pad << "ui64: " << v << "\n";
				break;
			}
			case data_type::fp32: {
				auto [v, sz] = serializer<float>::load(payload.data(), payload.size());
				os << pad << "fp32: " << v << "\n";
				break;
			}
			case data_type::fp64: {
				auto [v, sz] = serializer<double>::load(payload.data(), payload.size());
				os << pad << "fp64: " << v << "\n";
				break;
			}
			case data_type::string: {
				auto [s, sz] = serializer<std::string>::load(payload.data(), payload.size());
				os << pad << "string: \"" << s << "\"\n";
				break;
			}
			case data_type::blob: {
				auto [s, sz] = serializer<byte_view>::load(payload.data(), payload.size());
				os << pad << "blob: " << std::format("[len:{}]", s.size()) << "\n";
				break;
			}
			case data_type::tuple: {
				os << pad << "tuple:\n";
				debug_dump_tuple(os, payload, indent + 2);
				break;
			}
			default:
				os << pad << "type=" << static_cast<unsigned>(t)
					<< " payload_len=" << payload.size() << "\n";
				break;
			}
		}

		static std::ostream& debug_dump_tuple(std::ostream& os, byte_view tuple_rec, int indent = 0)
		{
			bool ok = for_each_in_tuple(tuple_rec,
				[&](std::size_t idx, data_type t, byte_view full, byte_view payload) -> bool {
					auto pad = std::string(indent, ' ');
					os << pad << "[" << idx << "] "
						<< "full_size=" << full.size()
						<< " payload=" << payload.size() << "\n";
					debug_dump_field(os, t, payload, indent + 2);
					return true; // continue
				});
			if (!ok) {
				os << std::string(indent, ' ') << "<corrupted tuple>\n";
			}
			return os;
		}
		
		template <typename WordT>
		static std::ostream& debug_word(std::ostream& os, byte_view data) {
			auto [val, sz] = serializer<WordT>::load(data.data(), data.size());
			return (os << std::format("{}", val));
		}

		static std::ostream& debug_string(std::ostream& os, byte_view data) {
			const auto [data_size, data_sz] = serializer<std::uint32_t>::load(data.data(), data.size());
			const auto dview = data.subspan(data_sz, data_size - 1 - data_sz);
			return (os << std::format("'{}'", std::string((const char *)dview.data(), dview.size())));
		}

		template <class Fn>
		static bool for_each_in_tuple(byte_view tuple_rec, Fn&& fn)
		{
			auto [total_len, prefix_sz] = serializer<std::uint32_t>::load(tuple_rec.data(), tuple_rec.size());
			if (total_len < prefix_sz || total_len > tuple_rec.size()) {
				return false;
			}

			byte_view rest = tuple_rec.subspan(prefix_sz, total_len - prefix_sz);

			std::size_t idx = 0;
			while (rest.size() != 0) {
				if (rest.size() < sizeof(serialized_data_header)) {
					return false;
				}

				const auto cur_sz = get_size(rest);
				if (cur_sz == 0 || cur_sz > rest.size()) {
					return false;
				}

				byte_view full = rest.first(cur_sz);
				const auto* hdr = reinterpret_cast<const serialized_data_header*>(full.data());
				const auto  t = static_cast<data_type>(hdr->type.get());

				const std::size_t payload_len = cur_sz - sizeof(serialized_data_header);
				byte_view payload(hdr->data(), payload_len);

				if (!std::forward<Fn>(fn)(idx, t, full, payload)) {
					return true;
				}

				rest = rest.subspan(cur_sz);
				++idx;
			}

			return true;
		}

		static std::partial_ordering compare_sequence(byte_view lhs, byte_view rhs) {

			while (true) {
				const bool l_empty = lhs.empty();
				const bool r_empty = rhs.empty();
				if (l_empty || r_empty) {
					return (!l_empty || !r_empty) ? (l_empty ? std::partial_ordering::less
						: std::partial_ordering::greater)
						: std::partial_ordering::equivalent;
				}

				const auto lsz = get_size(lhs);
				const auto rsz = get_size(rhs);
				if (lsz == 0 || rsz == 0 || lsz > lhs.size() || rsz > rhs.size()) {
					return std::partial_ordering::unordered;
				}

				auto ord = compare(lhs.first(lsz), rhs.first(rsz));
				if (ord != std::partial_ordering::equivalent) {
					return ord;
				}

				lhs = lhs.subspan(lsz);
				rhs = rhs.subspan(rsz);
			}
		}

		static std::partial_ordering compare_tuple(byte_view lhs, byte_view rhs)
		{
			auto [L, lsz] = serializer<std::uint32_t>::load(lhs.data(), lhs.size());
			auto [R, rsz] = serializer<std::uint32_t>::load(rhs.data(), rhs.size());
			lhs = lhs.subspan(lsz, L - lsz);
			rhs = rhs.subspan(rsz, R - rsz);

			while (lhs.empty() == rhs.empty()) {
				if (lhs.empty()) {
					return std::partial_ordering::equivalent;
				}

				const auto lsz1 = get_size(lhs);
				const auto rsz1 = get_size(rhs);
				if (lsz1 == 0 || rsz1 == 0 || lsz1 > lhs.size() || rsz1 > rhs.size()) {
					return std::partial_ordering::unordered;
				}

				byte_view lf = lhs.first(lsz1);
				byte_view rf = rhs.first(rsz1);

				auto res = compare(lf, rf);
				if (res != std::partial_ordering::equivalent) {
					return res;
				}

				lhs = lhs.subspan(lsz1);
				rhs = rhs.subspan(rsz1);
			}

			return lhs.size() <=> rhs.size();
		}

		template <typename WordT>
		static std::partial_ordering compare_word(byte_view lhs, byte_view rhs) {
			auto [left_val, left_sz] = serializer<WordT>::load(lhs.data(), lhs.size());
			auto [right_val, right_sz] = serializer<WordT>::load(rhs.data(), rhs.size());
			return left_val <=> right_val;
		}

		static std::partial_ordering compare_string(byte_view lhs, byte_view rhs) {
			const auto [left_str_size, left_sz] = serializer<std::uint32_t>::load(lhs.data(), lhs.size());
			const auto [right_str_size, right_sz] = serializer<std::uint32_t>::load(rhs.data(), rhs.size());
			const auto left_view = lhs.subspan(left_sz, left_str_size - 1 - left_sz);
			const auto right_view = rhs.subspan(right_sz, right_str_size - 1 - right_sz);
			return std::lexicographical_compare_three_way(
				left_view.begin(), left_view.end(),
				right_view.begin(), right_view.end()
			);
		}

		static std::partial_ordering compare_blob(byte_view lhs, byte_view rhs) {
			const auto [left_str_size, left_sz] = serializer<std::uint32_t>::load(lhs.data(), lhs.size());
			const auto [right_str_size, right_sz] = serializer<std::uint32_t>::load(rhs.data(), rhs.size());
			const auto left_view = lhs.subspan(left_sz, left_str_size - left_sz);
			const auto right_view = rhs.subspan(right_sz, right_str_size - right_sz);
			return std::lexicographical_compare_three_way(
				left_view.begin(), left_view.end(),
				right_view.begin(), right_view.end()
			);
		}

		static std::size_t get_size(byte_view data) {
			const auto hdr = reinterpret_cast<const serialized_data_header*>(data.data());
			const data_type t = get_type(data);
			const auto data_size = data.size() - sizeof(serialized_data_header);
			switch (t) {
			case data_type::blob:
			case data_type::tuple:
			case data_type::string: {
				auto [val, sz] = serializer<std::uint32_t>::load(hdr->data(), data_size);
				return val + sizeof(serialized_data_header);
			}
			case data_type::fp32: {
				auto [val, sz] = serializer<float>::load(hdr->data(), data_size);
				return sz + sizeof(serialized_data_header);
			}
			case data_type::fp64: {
				auto [val, sz] = serializer<double>::load(hdr->data(), data_size);
				return sz + sizeof(serialized_data_header);
			}
            case data_type::i32:  { 
                auto [_, sz] = serializer<std::int32_t>::load(hdr->data(), data_size); 
                return sz + sizeof(serialized_data_header); 
            }
            case data_type::i64:  { 
                auto [_, sz] = serializer<std::int64_t>::load(hdr->data(), data_size); 
                return sz + sizeof(serialized_data_header); 
            }
			case data_type::ui32: {
				auto [val, sz] = serializer<std::uint32_t>::load(hdr->data(), data_size);
				return sz + sizeof(serialized_data_header);
			}
			case data_type::ui64: {
				auto [val, sz] = serializer<std::uint64_t>::load(hdr->data(), data_size);
				return sz + sizeof(serialized_data_header);
			}
			}
			return 0;
		}

		static data_type get_type(byte_view data) {
			if (data.size() < sizeof(serialized_data_header)) {
				return data_type::undefined;
			}
			const auto hdr = reinterpret_cast<const serialized_data_header*>(data.data());
			return static_cast<data_type>(static_cast<std::uint16_t>(hdr->type));
		}

		byte_view data_;
	};


} // namespace fulla::codec
