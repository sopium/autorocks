/*
 * Copyright 2022, The Cozo Project Authors. Licensed under MIT/Apache-2.0/BSD-3-Clause.
 */

#pragma once

#include "common.h"

inline Slice convert_slice(RustBytes d)
{
    return {reinterpret_cast<const char *>(d.data()), d.size()};
}

inline string convert_slice_to_string(RustBytes d)
{
    return {reinterpret_cast<const char *>(d.data()), d.size()};
}

inline RustBytes convert_slice_back(const Slice &s)
{
    return {reinterpret_cast<const std::uint8_t *>(s.data()), s.size()};
}

inline RustBytes convert_pinnable_slice_back(const PinnableSlice &s)
{
    return {reinterpret_cast<const std::uint8_t *>(s.data()), s.size()};
}
