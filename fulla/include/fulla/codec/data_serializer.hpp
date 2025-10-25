/*
 * File: data_serializer.hpp
 * Author: newenclave
 * GitHub: https://github.com/newenclave
 * Created: 2025-10-25
 * License: MIT
 */

#pragma once

#include "fulla/codec/prop_types.hpp"
#include "fulla/codec/serializer.hpp"

namespace fulla::codec {

    using core::byte;
    using core::byte_buffer;
    using core::byte_span;
    using core::byte_view;

	class data_serializer {
	public:
		template <typename T>
		data_serializer& store(const T& val) {
			const auto sz = serializer<T>::size(val);
			const auto old_size = buffer_.size();
			buffer_.resize(old_size + sz + sizeof(serialized_data_header));
			auto hdr = reinterpret_cast<serialized_data_header*>(buffer_.data() + old_size);
			serializer<T>::store(val, hdr->data());

			if constexpr (std::is_same_v<T, std::string>) {
				hdr->type = static_cast<std::uint16_t>(data_type::string);
			}
			else if constexpr (std::is_same_v<T, std::uint32_t>) {
				hdr->type = static_cast<std::uint16_t>(data_type::ui32);
			}
			else if constexpr (std::is_same_v<T, std::uint64_t>) {
				hdr->type = static_cast<std::uint16_t>(data_type::ui64);
			}
			else if constexpr (std::is_same_v<T, std::int32_t>) {
				hdr->type = static_cast<std::uint16_t>(data_type::i32);
			}
			else if constexpr (std::is_same_v<T, std::int64_t>) {
				hdr->type = static_cast<std::uint16_t>(data_type::i64);
			}
			else if constexpr (std::is_same_v<T, float>) {
				hdr->type = static_cast<std::uint16_t>(data_type::fp32);
			}
			else if constexpr (std::is_same_v<T, double>) {
				hdr->type = static_cast<std::uint16_t>(data_type::fp64);
			}
			else {
				hdr->type = static_cast<std::uint16_t>(data_type::undefined);
			}
			return *this;
		}

		data_serializer& store_blob(const byte* data, std::size_t len, data_type t) {
			const auto old_size = buffer_.size();
			const auto data_view = byte_view(data, len);
			const auto serialized_size = serializer<byte_view>::size(data_view);

			buffer_.resize(old_size + serialized_size + sizeof(serialized_data_header));
			auto hdr = reinterpret_cast<serialized_data_header*>(buffer_.data() + old_size);

			hdr->type = static_cast<std::uint16_t>(t);
			serializer<byte_view>::store(data_view, hdr->data());

			return *this;
		}

		data_serializer& append(const byte* data, std::size_t len) {
			const auto old_size = buffer_.size();
			buffer_.resize(old_size + len);
			std::memcpy(&buffer_[old_size], data, len);
			return *this;
		}

		std::size_t size() const {
			return buffer_.size();
		}

		const byte* data() const { return buffer_.data(); }

		byte_span span() {
			return byte_span(buffer_.data(), buffer_.size());
		}

		byte_view view() const {
			return byte_view(buffer_.data(), buffer_.size());
		}

	private:

		byte_buffer buffer_;
	};

}