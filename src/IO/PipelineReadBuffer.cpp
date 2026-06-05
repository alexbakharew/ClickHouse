#include <IO/PipelineReadBuffer.h>
#include <IO/ReaderExecutor.h>
#include <Common/Exception.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
}

PipelineReadBuffer::PipelineReadBuffer(std::unique_ptr<ReaderExecutor> executor_)
    : ReadBufferFromFileBase(0, nullptr, 0)
    , executor(std::move(executor_))
    , read_position(executor->getPosition())
{
    LOG_DEBUG(log, "Created, total_size={}, read_position={}", executor->totalSize(), read_position);
}

PipelineReadBuffer::~PipelineReadBuffer() = default;

bool PipelineReadBuffer::nextImpl()
{
    Stopwatch watch(profile_callback ? clock_type : CLOCK_MONOTONIC);
    auto chunk = executor->readNextChunk();
    if (chunk.size == 0)
    {
        LOG_TRACE(log, "nextImpl: EOF at {}", read_position);
        return false;
    }

    /// Report the read so `MergeTreeReadPool`'s slow-read backoff still sees it.
    if (profile_callback)
    {
        ProfileInfo info{};
        info.bytes_requested = chunk.size;
        info.bytes_read = chunk.size;
        info.nanoseconds = watch.elapsed();
        profile_callback(info);
    }

    /// `chunk.data` is read-only and owned by the executor; we only expose it,
    /// never write through it, so dropping const is safe.
    char * data = const_cast<char *>(chunk.data);
    internal_buffer = Buffer(data, data + chunk.size);
    working_buffer = internal_buffer;
    pos = working_buffer.begin();
    read_position = chunk.logical_offset + chunk.size;
    LOG_TRACE(log, "nextImpl: serving {} bytes at offset {}, read_position advanced to {}",
        chunk.size, chunk.logical_offset, read_position);
    return true;
}

off_t PipelineReadBuffer::seek(off_t off, int whence)
{
    size_t new_pos = 0;
    if (whence == SEEK_SET)
    {
        if (off < 0)
            throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                "PipelineReadBuffer::seek: SEEK_SET with negative offset {}", off);
        new_pos = static_cast<size_t>(off);
    }
    else if (whence == SEEK_CUR)
    {
        off_t cur = getPosition();
        if (off < 0 && static_cast<size_t>(-off) > static_cast<size_t>(cur))
            throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                "PipelineReadBuffer::seek: SEEK_CUR offset {} from position {} would underflow",
                off, cur);
        new_pos = static_cast<size_t>(cur + off);
    }
    else
        throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "PipelineReadBuffer::seek: unsupported whence");

    LOG_DEBUG(log, "seek to {}", new_pos);

    resetWorkingBuffer();
    executor->seek(new_pos);
    read_position = new_pos;
    return new_pos;
}

off_t PipelineReadBuffer::getPosition()
{
    return read_position - available();
}

std::optional<size_t> PipelineReadBuffer::tryGetFileSize()
{
    /// Unknown-size sources (S3 HEAD without Content-Length) must surface as
    /// `nullopt`, not a meaningless `~uint64_t::max()` byte count.
    if (executor->hasUnknownSize())
        return std::nullopt;
    return executor->totalSize();
}

bool PipelineReadBuffer::checkIfActuallySeekable()
{
    /// Unknown-size sources are streamed through `nextImpl`, not seeked: a `true`
    /// answer leads formats to `getFileSizeFromReadBuffer`, which throws.
    return !executor->hasUnknownSize();
}

String PipelineReadBuffer::getFileName() const
{
    /// Surface the object path so format/decompression diagnostics name the
    /// failing object instead of this wrapper.
    String name = executor->getFileName();
    return name.empty() ? "PipelineReadBuffer" : name;
}

}
