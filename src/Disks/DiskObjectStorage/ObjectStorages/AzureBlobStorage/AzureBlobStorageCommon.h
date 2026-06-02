#pragma once
#include "config.h"

#if USE_AZURE_BLOB_STORAGE

#include <azure/storage/blobs.hpp>
#include <azure/core/response.hpp>
#include <azure/storage/blobs/blob_client.hpp>
#include <azure/storage/blobs/blob_options.hpp>
#include <azure/storage/blobs/blob_service_client.hpp>

#endif

#include <Poco/Util/AbstractConfiguration.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Interpreters/Context_fwd.h>
#include <Common/BlobStorageLogWriter.h>
#if USE_AZURE_BLOB_STORAGE
#include <IO/AzureBlobStorage/isRetryableAzureException.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <base/sleep.h>
#endif

namespace DB
{

struct Settings;

namespace AzureBlobStorage
{

struct RequestSettings
{
    RequestSettings() = default;

    size_t max_single_part_upload_size = 100 * 1024 * 1024; /// NOTE: on 32-bit machines it will be at most 4GB, but size_t is also used in BufferBase for offset
    size_t min_bytes_for_seek = 1024 * 1024;
    size_t max_single_read_retries = 3;
    size_t max_single_download_retries = 3;
    size_t list_object_keys_size = 1000;
    size_t min_upload_part_size = 16 * 1024 * 1024;
    size_t max_upload_part_size = 5ULL * 1024 * 1024 * 1024;
    size_t max_single_part_copy_size = 256 * 1024 * 1024;
    size_t max_unexpected_write_error_retries = 4;
    size_t max_inflight_parts_for_one_file = 20;
    size_t max_blocks_in_multipart_upload = 50000;
    size_t strict_upload_part_size = 0;
    size_t upload_part_size_multiply_factor = 2;
    size_t upload_part_size_multiply_parts_count_threshold = 500;
    size_t sdk_max_retries = 10;
    size_t sdk_retry_initial_backoff_ms = 10;
    size_t sdk_retry_max_backoff_ms = 1000;
    bool use_native_copy = false;
    bool check_objects_after_upload = false;
    bool read_only = false;
    size_t http_keep_alive_timeout = DEFAULT_HTTP_KEEP_ALIVE_TIMEOUT;
    size_t http_keep_alive_max_requests = DEFAULT_HTTP_KEEP_ALIVE_MAX_REQUEST;
};

struct Endpoint
{
    String storage_account_url;
    String account_name;
    String account_key;
    String container_name;
    String prefix;
    String sas_auth;
    String additional_params;
    std::optional<bool> container_already_exists;
    std::optional<bool> add_account_name_to_url;

    String getContainerEndpoint() const
    {
        String url = storage_account_url;
        if (url.ends_with('/'))
          url.pop_back();

        if (!account_name.empty() && add_account_name_to_url.value_or(true))
            url += "/" + account_name;

        if (!container_name.empty())
            url += "/" + container_name;

        if (!sas_auth.empty())
            url += "?" + sas_auth;

        if (!additional_params.empty())
            url += "?" + additional_params;

        return url;
    }

    String getServiceEndpoint() const
    {
        String url = storage_account_url;

        if (!account_name.empty() && add_account_name_to_url.value_or(true))
            url += "/" + account_name;

        if (!sas_auth.empty())
            url += "?" + sas_auth;

        if (!additional_params.empty())
            url += "?" + additional_params;

        return url;
    }
};

#if USE_AZURE_BLOB_STORAGE

using BlobClient = Azure::Storage::Blobs::BlobClient;
using BlockBlobClient = Azure::Storage::Blobs::BlockBlobClient;
using RawContainerClient = Azure::Storage::Blobs::BlobContainerClient;

using Azure::Storage::Blobs::ListBlobsOptions;
using Azure::Storage::Blobs::ListBlobsPagedResponse;
using Azure::Storage::Blobs::BlobContainerBatch;
using BlobContainerPropertiesRespones = Azure::Response<Azure::Storage::Blobs::Models::BlobContainerProperties>;
using BlobBatchResultResponse = Azure::Response<Azure::Storage::Blobs::Models::SubmitBlobBatchResult>;
using DeleteBlobResultDeferredResponse = Azure::Storage::DeferredResponse<Azure::Storage::Blobs::Models::DeleteBlobResult>;

/// A wrapper for ContainerClient that correctly handles the prefix of blobs.
/// See AzureBlobStorageEndpoint and processAzureBlobStorageEndpoint for details.
///
/// This wrapper is the single chokepoint for every Azure SDK call in the
/// ClickHouse Azure object-storage layer. Callers should NOT acquire raw
/// `BlobClient`/`BlockBlobClient` objects and operate on them directly —
/// use the named data-plane methods below (`uploadSinglePartWith…`,
/// `downloadBlobBodyStream…`, `deleteBlobSingleWith…` etc.).
///
/// Common boilerplate (profile-event increments paired with their DiskAzure
/// counterparts, BlobStorageLog event recording with the Stopwatch +
/// error_code + error_message triplet) is absorbed into the helper methods
/// in the "Universal helpers" section, so call sites stay thin.
///
/// Retry policy currently lives at the call sites (see
/// `WriteBufferFromAzureBlobStorage::execWithRetry` and the inline retry
/// loops in `ReadBufferFromAzureBlobStorage`). Wrapper methods are
/// non-retrying: they do exactly one SDK call, log success or failure into
/// BlobStorageLog, and rethrow on failure. T1 (unified retry helper) is a
/// follow-up that can later absorb the retry into the wrapper itself.
class ContainerClientWrapper
{
public:
    ContainerClientWrapper(RawContainerClient client_, String blob_prefix_);

    /// === existing accessors (kept; some now used only internally) ===

    bool IsClientForDisk() const;
    BlobClient GetBlobClient(const String & blob_name) const;
    BlockBlobClient GetBlockBlobClient(const String & blob_name) const;
    BlobContainerPropertiesRespones GetProperties() const;
    ListBlobsPagedResponse ListBlobs(const ListBlobsOptions & options) const;

    BlobContainerBatch CreateBatch() const;
    BlobBatchResultResponse SubmitBatch(const BlobContainerBatch & batch) const;
    String GetBlobPath(const String & blob_name) const;

    /// === Universal tracing helpers (ProfileEvents pairs) ===
    ///
    /// Each `traceAzure<Op>` increments the generic `Azure<Op>` event, and
    /// additionally the `DiskAzure<Op>` counterpart when `IsClientForDisk()`
    /// returns true. Replaces the copy-pasted pair at every call site.

    void traceAzureListObjects(size_t count = 1) const;
    void traceAzureGetProperties() const;
    void traceAzureDeleteObjects(size_t count = 1) const;
    void traceAzureUpload() const;
    void traceAzureStageBlock() const;
    void traceAzureCommitBlockList() const;
    void traceAzureCopyObject() const;
    void traceAzureGetObject() const;

    /// === Universal BlobStorageLog helpers ===
    ///
    /// These replace the `Stopwatch + error_code + error_message +
    /// blob_log->addEvent(...)` triplet that was hand-rolled at every SDK
    /// call site. They are no-ops if `blob_log` is nullptr.

    static void logBlobStorageEventOnSuccess(
        const BlobStorageLogWriterPtr & blob_log,
        BlobStorageLogElement::EventType event_type,
        const String & container_for_logging,
        const String & blob_path_for_logging,
        size_t data_size,
        UInt64 elapsed_microseconds);

    static void logBlobStorageEventOnFailure(
        const BlobStorageLogWriterPtr & blob_log,
        BlobStorageLogElement::EventType event_type,
        const String & container_for_logging,
        const String & blob_path_for_logging,
        size_t data_size,
        UInt64 elapsed_microseconds,
        Int32 status_code,
        const String & error_message);

    /// === Generic error/retry primitives (T1 + Option A) ===
    ///
    /// `executeWithRetryRethrow` is the single retry+rethrow primitive that
    /// replaces today's three distinct retry-loop shapes (the deleted
    /// `WriteBufferFromAzureBlobStorage::execWithRetry`, and the inline
    /// for-loops in `ReadBufferFromAzureBlobStorage::nextImpl / initialize
    /// / readBigAt`). The lambda receives the 0-based attempt number and
    /// owns any ResourceGuard/throttler scoping it needs — keeping the
    /// helper agnostic to IO-write vs IO-read scheduling.
    ///
    /// `executeWithRethrow` is the catch-only sibling: execute one Azure
    /// SDK call and translate any `RequestFailedException` to a ClickHouse
    /// exception via `rethrowAzureException`. No retry, no status-code
    /// swallowing — sites that need to swallow specific codes (e.g.
    /// NotFound → false in `exists`) keep their explicit inline if-check.
    ///
    /// Both are header-only templates because each call site instantiates
    /// them with a unique lambda type; an explicit-instantiation cpp
    /// approach is not practical.

    /// Indirection over `getCurrentExceptionCode() == CANNOT_ALLOCATE_MEMORY`
    /// so the template body can stay in the header without requiring every
    /// TU that includes us to declare the `extern const int` for that code.
    static bool isCannotAllocateMemoryCurrentException();

    template <typename F>
    static auto executeWithRetryRethrow(
        LoggerPtr log,
        std::string_view resource_for_logging,
        size_t num_tries,
        F && func) -> decltype(func(size_t{0}))
    {
        size_t sleep_ms = 100;
        for (size_t i = 0; i < num_tries; ++i)
        {
            try
            {
                return func(i);
            }
            catch (const Azure::Core::RequestFailedException & e)
            {
                if (i + 1 == num_tries || !isRetryableAzureException(e))
                    rethrowAzureException(e, std::string{resource_for_logging});
                LOG_DEBUG(log, "Azure call for `{}` failed at attempt {}/{}: {} {}",
                          resource_for_logging, i + 1, num_tries, e.what(), e.Message);
                sleepForMilliseconds(sleep_ms);
                sleep_ms *= 2;
            }
            catch (...)
            {
                /// CANNOT_ALLOCATE_MEMORY is the only generic exception we
                /// refuse to retry — retrying allocation failures wastes
                /// time and amplifies pressure.
                if (isCannotAllocateMemoryCurrentException())
                    throw;
                if (i + 1 == num_tries)
                    throw;
                LOG_DEBUG(log, "Azure call for `{}` failed at attempt {}/{}: {}",
                          resource_for_logging, i + 1, num_tries,
                          getCurrentExceptionMessage(false));
                sleepForMilliseconds(sleep_ms);
                sleep_ms *= 2;
            }
        }
        UNREACHABLE();
    }

    template <typename F>
    static auto executeWithRethrow(std::string_view resource_for_logging, F && func)
        -> decltype(func())
    {
        try
        {
            return func();
        }
        catch (const Azure::Core::RequestFailedException & e)
        {
            rethrowAzureException(e, std::string{resource_for_logging});
        }
    }

    /// === Data-plane wrappers (excessively named, no overloads) ===
    ///
    /// Each method corresponds to exactly one SDK call shape at exactly
    /// one call site (or a few call sites with identical shape). Variants
    /// that differ in options/context/logging are separate methods rather
    /// than parameter-overloaded forms.

    /// containerExists() probe; checks if the container itself exists.
    BlobContainerPropertiesRespones getContainerPropertiesForExistenceCheck() const;

    /// Paged listing: single SDK call with the blob_prefix prepended and the
    /// returned blob names stripped of that prefix. Callers iterate pages
    /// themselves (either via continuation tokens in `AzureIteratorAsync` or
    /// via `MoveToNextPage()` in `listObjects`) and call
    /// `traceAzureListObjects()` once per page request to record the event
    /// — the wrapper does NOT trace internally because each page is its own
    /// HTTP request and the caller controls when it happens.
    ListBlobsPagedResponse listBlobsPagedWithPrefixAdjustment(const ListBlobsOptions & options) const;

    /// AzureObjectStorage::exists — NotFound → caller returns false.
    Azure::Response<Azure::Storage::Blobs::Models::BlobProperties>
        getBlobPropertiesForExistenceCheck(const String & blob_name) const;
    /// AzureObjectStorage::getObjectMetadata — full property snapshot.
    Azure::Response<Azure::Storage::Blobs::Models::BlobProperties>
        getBlobPropertiesForMetadata(const String & blob_name) const;
    /// ReadBuffer::tryGetFileSize / getRemoteFileSize — size-only path.
    Azure::Response<Azure::Storage::Blobs::Models::BlobProperties>
        getBlobPropertiesForSizeOnly(const String & blob_name) const;
    /// WriteBuffer::finalizeImpl post-upload check — NotFound means we lost
    /// the just-uploaded blob, which the caller treats as a bug.
    Azure::Response<Azure::Storage::Blobs::Models::BlobProperties>
        getBlobPropertiesForUploadVerification(const String & blob_name) const;

    /// ReadBuffer::initialize TTFB download with retry-attempt context.
    /// Per-attempt BlobStorageLog success/failure event and the Read-side
    /// error counter (`ReadBufferFromAzureRequestsErrors`) are recorded
    /// inside this method, symmetric with the upload methods.
    Azure::Response<Azure::Storage::Blobs::Models::DownloadBlobResult>
        downloadBlobBodyStreamWithAttemptContext(
            const String & blob_name,
            const Azure::Storage::Blobs::DownloadBlobOptions & options,
            size_t attempt,
            const BlobStorageLogWriterPtr & blob_log,
            const String & container_for_logging,
            size_t length_or_zero_for_failure_logging) const;
    /// ReadBuffer::readBigAt random-access download (no retry attempt key).
    /// Same per-attempt BlobStorageLog + Read error counter as above.
    Azure::Response<Azure::Storage::Blobs::Models::DownloadBlobResult>
        downloadBlobBodyStreamForReadBigAt(
            const String & blob_name,
            const Azure::Storage::Blobs::DownloadBlobOptions & options,
            const BlobStorageLogWriterPtr & blob_log,
            const String & container_for_logging,
            size_t n_for_logging) const;

    /// WriteBuffer single-part upload path: access conditions, retry-attempt
    /// SDK context, and BlobStorageLog recording (success and failure).
    Azure::Response<Azure::Storage::Blobs::Models::UploadBlockBlobResult>
        uploadSinglePartWithAccessConditionsAndRetryContext(
            const String & blob_name,
            Azure::Core::IO::BodyStream & stream,
            const Azure::Storage::Blobs::UploadBlockBlobOptions & options,
            size_t attempt,
            const BlobStorageLogWriterPtr & blob_log,
            const String & container_for_logging,
            size_t data_size_for_logging) const;
    /// copyAzureBlobStorageFile single-part upload path: no options, no retry
    /// context. Logs only on failure (matches current behaviour: success
    /// log was missing at this site).
    void uploadSinglePartForCopyWithBlobStorageLog(
        const String & blob_name,
        Azure::Core::IO::BodyStream & stream,
        const BlobStorageLogWriterPtr & blob_log,
        const String & container_for_logging,
        size_t data_size_for_logging) const;

    /// WriteBuffer stage-block path: retry-attempt SDK context + log.
    void stageBlockWithRetryContextAndBlobStorageLog(
        const String & blob_name,
        const String & block_id,
        Azure::Core::IO::BodyStream & stream,
        size_t attempt,
        const BlobStorageLogWriterPtr & blob_log,
        const String & container_for_logging,
        size_t data_size_for_logging) const;
    /// copyAzureBlobStorageFile stage-block path: plain, log only.
    void stageBlockForCopyWithBlobStorageLog(
        const String & blob_name,
        const String & block_id,
        Azure::Core::IO::BodyStream & stream,
        const BlobStorageLogWriterPtr & blob_log,
        const String & container_for_logging,
        size_t data_size_for_logging) const;

    /// WriteBuffer commit-block-list path: access conditions + retry context.
    void commitBlockListWithAccessConditionsAndRetryContext(
        const String & blob_name,
        const std::vector<std::string> & block_ids,
        const Azure::Storage::Blobs::CommitBlockListOptions & options,
        size_t attempt,
        const BlobStorageLogWriterPtr & blob_log,
        const String & container_for_logging) const;
    /// copyAzureBlobStorageFile commit-block-list path: plain, log only.
    void commitBlockListForCopyWithBlobStorageLog(
        const String & blob_name,
        const std::vector<std::string> & block_ids,
        const BlobStorageLogWriterPtr & blob_log,
        const String & container_for_logging) const;

    /// AzureObjectStorage::removeObjectImpl single-blob delete with full
    /// BlobStorageLog accounting (success, NotFound-with-if_exists, failure).
    /// Returns true if the blob was actually deleted, false if it didn't
    /// exist and `if_exists` was true.
    void deleteBlobSingleWithBlobStorageLog(
        const String & blob_name,
        bool if_exists,
        const BlobStorageLogWriterPtr & blob_log,
        const String & container_for_logging,
        const String & local_path_for_logging,
        size_t bytes_size_for_logging) const;

    /// AzureObjectStorage::removeObjectsBatchIfExists: enqueue a delete into
    /// a batch built by `CreateBatch()`. Prepends the blob_prefix internally
    /// so callers don't compute it.
    DeleteBlobResultDeferredResponse addDeleteBlobToBatch(
        BlobContainerBatch & batch,
        const String & blob_name) const;

    /// AzureObjectStorage::tagObjects — read-modify-write of blob tags.
    std::map<std::string, std::string> getBlobTagsForUpdate(const String & blob_name) const;
    void setBlobTags(const String & blob_name, const std::map<std::string, std::string> & tags) const;

    /// copyAzureBlobStorageFile native-copy path.
    String getBlobUrlForServerSideCopy(const String & blob_name) const;
    Azure::Response<Azure::Storage::Blobs::Models::CopyBlobFromUriResult>
        copyBlobFromUriSync(
            const String & dest_blob_name,
            const String & source_uri,
            const Azure::Storage::Blobs::CopyBlobFromUriOptions & options) const;
    Azure::Storage::Blobs::StartBlobCopyOperation copyBlobFromUriAsync(
        const String & dest_blob_name,
        const String & source_uri,
        const Azure::Storage::Blobs::StartBlobCopyFromUriOptions & options) const;

private:
    RawContainerClient client;
    String blob_prefix;
};

using ContainerClient = ContainerClientWrapper;
using ServiceClient = Azure::Storage::Blobs::BlobServiceClient;
using BlobClientOptions = Azure::Storage::Blobs::BlobClientOptions;

struct ConnectionParams
{
    Endpoint endpoint;
    AuthMethod auth_method;
    BlobClientOptions client_options;

    String getContainer() const { return endpoint.container_name; }
    String getConnectionURL() const;

    std::unique_ptr<ServiceClient> createForService() const;
    std::unique_ptr<ContainerClient> createForContainer() const;
};

void processURL(const String & url, const String & container_name, Endpoint & endpoint, AuthMethod & auth_method);

std::unique_ptr<ContainerClient> getContainerClient(const ConnectionParams & params, bool readonly);

BlobClientOptions getClientOptions(
    const ContextPtr & context,
    const Settings & settings,
    const RequestSettings & request_settings,
    bool for_disk);

AuthMethod getAuthMethod(const Poco::Util::AbstractConfiguration & config, const String & config_prefix);

#endif

Endpoint processEndpoint(const Poco::Util::AbstractConfiguration & config, const String & config_prefix);

std::unique_ptr<RequestSettings> getRequestSettings(const Settings & query_settings);
std::unique_ptr<RequestSettings> getRequestSettingsForBackup(ContextPtr context, String endpoint, bool use_native_copy);
std::unique_ptr<RequestSettings> getRequestSettings(const Poco::Util::AbstractConfiguration & config, const String & config_prefix, const Settings & settings_ref);

}


/// AzureSettingsByEndpoint contains a map of AzureBlobStorage endpoints and their settings, used in Context level
/// When any endpoint is used, the settings are looked up in this map and applied
class AzureSettingsByEndpoint
{
public:
    void loadFromConfig(
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        const DB::Settings & settings);

    std::optional<AzureBlobStorage::RequestSettings> getSettings(
        const std::string & endpoint) const;

private:
    mutable std::mutex mutex;
    std::map<const String, const AzureBlobStorage::RequestSettings> azure_settings;
};


}
