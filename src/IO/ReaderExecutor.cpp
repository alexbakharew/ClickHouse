#include <IO/ReaderExecutor.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Common/logger_useful.h>

#include <algorithm>

namespace DB
{

ReaderExecutor::ReaderExecutor(
    std::shared_ptr<ISourceReader> source_,
    const StoredObjects & objects,
    size_t block_size_)
    : source(std::move(source_))
    , block_size(block_size_ ? block_size_ : DEFAULT_BLOCK_SIZE)
{
    offset_map.build(objects);
    log_file_path = objects.empty() ? "" : objects.front().remote_path;
    LOG_DEBUG(log, "Created: source={}, objects={}, total_size={}, block_size={}",
        source ? source->name() : "none", objects.size(), offset_map.totalSize(), block_size);
}

ReaderExecutor::~ReaderExecutor() = default;

ReaderExecutor::Chunk ReaderExecutor::readNextChunk()
{
    if (atEnd())
        return {};

    size_t object_file_offset = 0;
    const StoredObject * object = offset_map.findObjectAt(position, &object_file_offset);
    if (!object)
    {
        /// `position` is at/past the end of a known-size file.
        reached_eof = true;
        return {};
    }

    const size_t object_offset = position - object_file_offset;

    /// How many bytes to read this call: one block, clamped to the object's end
    /// for known-size objects so a chunk never straddles an object boundary —
    /// successive calls walk into the next object.
    size_t want = block_size;
    if (!offset_map.hasUnknownSize())
    {
        const size_t remaining_in_object = object->bytes_size - object_offset;
        want = std::min(block_size, remaining_in_object);
        if (want == 0)
        {
            reached_eof = true;
            return {};
        }
    }

    /// Open a fresh source buffer for every chunk, seek, and read. No buffer is
    /// kept between calls — the simplest possible model. Connection/buffer reuse
    /// is a later step.
    auto buffer = source->open(*object);
    if (object_offset > 0)
        buffer->seek(static_cast<off_t>(object_offset), SEEK_SET);

    block.resize(want);
    /// All source buffers are driven by a plain copying `read()` — the executor
    /// never uses external-buffer (set()+next()) mode in this minimal step, so
    /// every buffer kind (pread, direct-IO, mmap, S3, ...) works uniformly.
    const size_t got = buffer->read(block.data(), want);

    if (got == 0)
    {
        /// Unknown-size source signalling EOF, or a truncated known-size file.
        reached_eof = true;
        return {};
    }

    Chunk chunk{block.data(), got, position};
    position += got;
    LOG_TRACE(log, "readNextChunk: served {} bytes at offset {} (object={}, object_offset={})",
        got, chunk.logical_offset, object->remote_path, object_offset);
    return chunk;
}

void ReaderExecutor::seek(size_t new_position)
{
    LOG_TRACE(log, "seek: {} -> {}", position, new_position);
    position = new_position;
    reached_eof = false;
    /// `current_buffer` is kept; the next `readNextChunk` re-seeks it (or reopens
    /// if the new position lands in a different object).
}

}
