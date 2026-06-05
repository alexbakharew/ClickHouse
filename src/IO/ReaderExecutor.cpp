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
        reached_eof = true;
        return {};
    }

    const size_t object_offset = position - object_file_offset;

    /// Clamp the block to the object boundary so a chunk never straddles two
    /// objects; the next call continues in the next object. Unknown total size
    /// means stream a full block and let a short read mark EOF.
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

    auto buffer = source->open(*object);
    if (object_offset > 0)
        buffer->seek(static_cast<off_t>(object_offset), SEEK_SET);

    /// Bound the request to the chunk so a remote source fetches exactly `want`
    /// bytes rather than an open-ended tail that is then cancelled.
    if (buffer->supportsRightBoundedReads())
        buffer->setReadUntilPosition(object_offset + want);

    block.resize(want);
    const size_t got = buffer->read(block.data(), want);

    if (got == 0)
    {
        /// A short read at unknown total size is the only EOF signal there.
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
}

}
