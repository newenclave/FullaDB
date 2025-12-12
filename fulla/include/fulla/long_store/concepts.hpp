/*
 * File: long_store/concepts.hpp
 * Author: newenclave
 * GitHub: https://github.com/newenclave
 * Created: 2025-12-09
 * License: MIT
 */

#pragma once

#include <variant>
#include <concepts>

#include "fulla/core/debug.hpp"

#include "fulla/page/header.hpp"
#include "fulla/page/page_view.hpp"
#include "fulla/page/long_store.hpp"
#include "fulla/storage/block_device.hpp"
#include "fulla/storage/buffer_manager.hpp"

namespace fulla::long_store::concepts {

    template <typename T>
    concept LongStoreKinds = requires (T s) {
        { T::header_kind_value } -> std::convertible_to<std::uint16_t>;
        { T::chunk_kind_value } -> std::convertible_to<std::uint16_t>;
    };
    template <typename T>
    concept LongStoreMetadata = requires {
        typename T::header_metadata_type;
        typename T::chunk_metadata_type;
    };

    template <typename T>
    concept LongStoreDescriptor = LongStoreKinds<T> && LongStoreMetadata<T>;
}
