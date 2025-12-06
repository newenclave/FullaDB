/*
 * File: long_store/page.hpp
 * Author: newenclave
 * GitHub: https://github.com/newenclave
 * Created: 2025-12-01
 * License: MIT
 */

#pragma once

#include "fulla/core/pack.hpp"
#include "fulla/core/types.hpp"

namespace fulla::page {
    using core::word_u16;
    using core::word_u32;

FULLA_PACKED_STRUCT_BEGIN
    
    struct data_header {
        word_u16 size{ 0 };
        word_u16 reserved{ 0 };
    } FULLA_PACKED;

    struct long_store_header {
	    word_u32 total_size{ 0 };
		word_u32 last{ word_u32::max() }; // last chunk page id
        word_u32 next{ word_u32::max() };
		data_header data{ 0 };
    } FULLA_PACKED;

    struct long_store_chunk {
        word_u32 prev{ word_u32::max() };
        word_u32 next{ word_u32::max() };
        data_header data{ 0 };
    } FULLA_PACKED;

FULLA_PACKED_STRUCT_END

}

 