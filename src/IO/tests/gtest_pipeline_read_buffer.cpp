#include <IO/PipelineReadBuffer.h>
#include <IO/ReaderExecutor.h>
#include <IO/LocalSourceReader.h>
#include <IO/ReadHelpers.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>

#include <gtest/gtest.h>
#include <fstream>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

using namespace DB;

namespace
{

unsigned char patternByte(size_t i)
{
    return static_cast<unsigned char>(i % 256);
}

class PipelineReadBufferTest : public ::testing::Test
{
protected:
    std::filesystem::path tmp_dir;

    void SetUp() override
    {
        tmp_dir = std::filesystem::temp_directory_path() / "test_pipeline_read_buffer";
        std::filesystem::create_directories(tmp_dir);
    }

    void TearDown() override { std::filesystem::remove_all(tmp_dir); }

    StoredObject makeFile(const std::string & name, size_t size)
    {
        auto path = tmp_dir / name;
        std::ofstream f(path, std::ios::binary);
        for (size_t i = 0; i < size; ++i)
            f.put(static_cast<char>(patternByte(i)));
        f.close();

        StoredObject obj;
        obj.remote_path = path.string();
        obj.bytes_size = size;
        return obj;
    }

    std::unique_ptr<PipelineReadBuffer> makeBuffer(const StoredObjects & objects, size_t block_size = 256)
    {
        auto executor = std::make_unique<ReaderExecutor>(
            std::make_shared<LocalSourceReader>(), objects, block_size);
        return std::make_unique<PipelineReadBuffer>(std::move(executor));
    }
};

TEST_F(PipelineReadBufferTest, ReadWholeFile)
{
    auto buf = makeBuffer({makeFile("a.bin", 1024)});

    EXPECT_EQ(buf->tryGetFileSize(), std::optional<size_t>(1024));

    std::vector<char> data(1024);
    buf->readStrict(data.data(), data.size());
    for (size_t i = 0; i < data.size(); ++i)
        ASSERT_EQ(static_cast<unsigned char>(data[i]), patternByte(i)) << "at offset " << i;
    EXPECT_TRUE(buf->eof());
    EXPECT_EQ(buf->getPosition(), 1024);
}

TEST_F(PipelineReadBufferTest, SpansBlocksAndObjects)
{
    /// Two objects, small block size: data must be continuous across both block
    /// and object boundaries when read through the standard buffer interface.
    auto buf = makeBuffer({makeFile("a.bin", 300), makeFile("b.bin", 200)}, /*block_size=*/64);

    std::vector<char> data(500);
    buf->readStrict(data.data(), data.size());
    for (size_t i = 0; i < 300; ++i)
        ASSERT_EQ(static_cast<unsigned char>(data[i]), patternByte(i)) << "object A at " << i;
    for (size_t i = 0; i < 200; ++i)
        ASSERT_EQ(static_cast<unsigned char>(data[300 + i]), patternByte(i)) << "object B at " << i;
    EXPECT_TRUE(buf->eof());
}

TEST_F(PipelineReadBufferTest, SeekSetAndRead)
{
    auto buf = makeBuffer({makeFile("a.bin", 1024)});

    buf->seek(500, SEEK_SET);
    EXPECT_EQ(buf->getPosition(), 500);

    char c = 0;
    buf->readStrict(&c, 1);
    EXPECT_EQ(static_cast<unsigned char>(c), patternByte(500));
    EXPECT_EQ(buf->getPosition(), 501);
}

TEST_F(PipelineReadBufferTest, SeekBackwardRereads)
{
    auto buf = makeBuffer({makeFile("a.bin", 1024)});

    std::vector<char> head(400);
    buf->readStrict(head.data(), head.size());

    buf->seek(0, SEEK_SET);
    EXPECT_EQ(buf->getPosition(), 0);

    char c = 0;
    buf->readStrict(&c, 1);
    EXPECT_EQ(static_cast<unsigned char>(c), patternByte(0));
}

TEST_F(PipelineReadBufferTest, SeekCurRelative)
{
    auto buf = makeBuffer({makeFile("a.bin", 1024)});

    buf->seek(100, SEEK_SET);
    buf->seek(50, SEEK_CUR);
    EXPECT_EQ(buf->getPosition(), 150);

    char c = 0;
    buf->readStrict(&c, 1);
    EXPECT_EQ(static_cast<unsigned char>(c), patternByte(150));
}

}
