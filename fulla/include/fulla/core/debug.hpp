/*
 * File: debug.hpp
 * Author: newenclave
 * GitHub: https://github.com/newenclave
 * Created: 2025-11-23
 * License: MIT
 */


#pragma once

#ifndef DB_ASSERT
#include <cassert>
#define DB_ASSERT(cond, msg) do { static_cast<void>(msg); assert(cond); } while(0)
#endif

#ifdef ENABLE_PRIVATE_TESTS
#define PRIVATE_TESTABLE public
#else
#define PRIVATE_TESTABLE private
#endif
