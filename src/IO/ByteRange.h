#pragma once

#include <cstddef>

namespace DB
{

/// A half-open byte interval `[offset, offset + size)` in some logical address
/// space (a logical file, an object). Used by `OffsetMap` and the read
/// pipeline to describe read requests and physical ranges.
struct ByteRange
{
    size_t offset = 0;
    size_t size = 0;
    size_t end() const { return offset + size; }
};

}
