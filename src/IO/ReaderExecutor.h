#pragma once

#include <IO/OffsetMap.h>
#include <IO/ISourceReader.h>
#include <IO/BufferWithOwnMemory.h>

#include <Common/Logger.h>
#include <base/types.h>

#include <memory>

namespace DB
{

class ReadBufferFromFileBase;

/// Minimal pipeline read executor — the first, deliberately dumb step of the
/// `ReaderExecutor` (experimental, gated by `use_reader_executor`). It maps a
/// logical read position to a `StoredObject` (via `OffsetMap`) and serves bytes
/// straight from an `ISourceReader`, one block at a time, into an owned buffer.
///
/// No caches, no `Rope`, no prefetch, no live-connection pooling, no memory
/// pressure adaptation, no decryption, no stats — those arrive in later steps.
/// One instance per column-stream; not thread-safe.
class ReaderExecutor
{
public:
    static constexpr size_t DEFAULT_BLOCK_SIZE = 1 * 1024 * 1024; /// 1 MiB

    ReaderExecutor(
        std::shared_ptr<ISourceReader> source,
        const StoredObjects & objects,
        size_t block_size = DEFAULT_BLOCK_SIZE);

    /// Out-of-line: `current_buffer` holds a `unique_ptr<ReadBufferFromFileBase>`
    /// (incomplete here).
    ~ReaderExecutor();

    /// A contiguous run of bytes starting at the current position. `data` points
    /// into the executor's own block buffer and stays valid only until the next
    /// `readNextChunk` / `seek` call. `size == 0` means EOF.
    struct Chunk
    {
        const char * data = nullptr;
        size_t size = 0;
        size_t logical_offset = 0;
    };

    /// Read the next block (<= `block_size`, clamped to the current object's end
    /// for known-size objects) starting at the current position, advancing the
    /// position by the bytes read.
    Chunk readNextChunk();

    /// Move the read position. The next `readNextChunk` reads from there.
    void seek(size_t new_position);

    size_t getPosition() const { return position; }

    size_t totalSize() const { return offset_map.totalSize(); }
    bool hasUnknownSize() const { return offset_map.hasUnknownSize(); }

    /// Logical object path for diagnostics (format/decompression errors via
    /// `getFileNameFromReadBuffer`). The front object's `remote_path`; empty
    /// when no objects are configured.
    String getFileName() const { return log_file_path; }

private:
    /// EOF detection: size known -> `position >= totalSize()`; size unknown ->
    /// the source's short return latched `reached_eof`. Seek-backward clears it.
    bool atEnd() const
    {
        return reached_eof || (!offset_map.hasUnknownSize() && position >= totalSize());
    }

    std::shared_ptr<ISourceReader> source;
    OffsetMap offset_map;
    String log_file_path;
    size_t block_size;
    size_t position = 0;
    bool reached_eof = false;

    /// Destination for the bytes served by the latest `readNextChunk`.
    Memory<> block;

    LoggerPtr log = getLogger("ReaderExecutor");
};

}
