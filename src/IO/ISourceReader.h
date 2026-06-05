#pragma once

#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>
#include <base/types.h>

#include <memory>

namespace DB
{

class ReadBufferFromFileBase;

/// Opens a seekable buffer for reading from a storage object.
class ISourceReader
{
public:
    virtual ~ISourceReader() = default;

    /// Open a seekable buffer for reads from the object. `ReaderExecutor` drives
    /// it with a plain copying `read()`, so the buffer manages its own memory
    /// (normal, non-external mode) — every buffer kind works uniformly.
    virtual std::unique_ptr<ReadBufferFromFileBase> open(const StoredObject & object) = 0;

    virtual String name() const = 0;
};

}
