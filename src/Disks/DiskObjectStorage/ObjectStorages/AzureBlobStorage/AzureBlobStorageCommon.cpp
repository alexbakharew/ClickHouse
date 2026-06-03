#include <Disks/DiskObjectStorage/ObjectStorages/AzureBlobStorage/AzureBlobStorageCommon.h>

#if USE_AZURE_BLOB_STORAGE

#include <azure/identity/managed_identity_credential.hpp>
#include <azure/identity/workload_identity_credential.hpp>
#include <azure/storage/blobs/blob_options.hpp>
#include <azure/storage/blobs/blob_responses.hpp>

#endif

#include <Core/ServerSettings.h>
#include <Common/ProxyConfigurationResolverProvider.h>
#include <IO/AzureBlobStorage/PocoHTTPClient.h>
#include <IO/AzureBlobStorage/isRetryableAzureException.h>
#include <IO/WriteSettings.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/Scheduler/ResourceGuard.h>
#include <Common/re2.h>
#include <Core/Settings.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Interpreters/Context.h>
#include <Common/logger_useful.h>
#include <Common/Throttler.h>

namespace ProfileEvents
{
    extern const Event AzureGetProperties;
    extern const Event DiskAzureGetProperties;
    extern const Event AzureCreateContainer;
    extern const Event DiskAzureCreateContainer;

    extern const Event AzureListObjects;
    extern const Event DiskAzureListObjects;
    extern const Event AzureDeleteObjects;
    extern const Event DiskAzureDeleteObjects;
    extern const Event AzureUpload;
    extern const Event DiskAzureUpload;
    extern const Event AzureStageBlock;
    extern const Event DiskAzureStageBlock;
    extern const Event AzureCommitBlockList;
    extern const Event DiskAzureCommitBlockList;
    extern const Event AzureCopyObject;
    extern const Event DiskAzureCopyObject;
    extern const Event AzureGetObject;
    extern const Event DiskAzureGetObject;

    extern const Event ReadBufferFromAzureRequestsErrors;

    extern const Event AzureGetRequestThrottlerCount;
    extern const Event AzureGetRequestThrottlerBlocked;
    extern const Event AzureGetRequestThrottlerSleepMicroseconds;
    extern const Event AzurePutRequestThrottlerCount;
    extern const Event AzurePutRequestThrottlerBlocked;
    extern const Event AzurePutRequestThrottlerSleepMicroseconds;

    extern const Event DiskAzureGetRequestThrottlerCount;
    extern const Event DiskAzureGetRequestThrottlerBlocked;
    extern const Event DiskAzureGetRequestThrottlerSleepMicroseconds;
    extern const Event DiskAzurePutRequestThrottlerCount;
    extern const Event DiskAzurePutRequestThrottlerBlocked;
    extern const Event DiskAzurePutRequestThrottlerSleepMicroseconds;
}

namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 azure_max_single_part_upload_size;
    extern const SettingsUInt64 azure_max_single_read_retries;
    extern const SettingsUInt64 azure_list_object_keys_size;
    extern const SettingsUInt64 azure_min_upload_part_size;
    extern const SettingsUInt64 azure_max_upload_part_size;
    extern const SettingsUInt64 azure_max_single_part_copy_size;
    extern const SettingsUInt64 azure_max_blocks_in_multipart_upload;
    extern const SettingsUInt64 azure_max_unexpected_write_error_retries;
    extern const SettingsUInt64 azure_max_inflight_parts_for_one_file;
    extern const SettingsUInt64 azure_strict_upload_part_size;
    extern const SettingsUInt64 azure_upload_part_size_multiply_factor;
    extern const SettingsUInt64 azure_upload_part_size_multiply_parts_count_threshold;
    extern const SettingsUInt64 azure_sdk_max_retries;
    extern const SettingsUInt64 azure_sdk_retry_initial_backoff_ms;
    extern const SettingsUInt64 azure_sdk_retry_max_backoff_ms;
    extern const SettingsBool azure_check_objects_after_upload;
    extern const SettingsBool azure_use_adaptive_timeouts;
    extern const SettingsUInt64 azure_max_redirects;
    extern const SettingsUInt64 azure_connect_timeout_ms;
    extern const SettingsUInt64 azure_request_timeout_ms;
    extern const SettingsSeconds tcp_keep_alive_timeout;
    extern const SettingsUInt64 http_max_fields;
    extern const SettingsUInt64 http_max_field_name_size;
    extern const SettingsUInt64 http_max_field_value_size;
    extern const SettingsUInt64 azure_max_get_rps;
    extern const SettingsUInt64 azure_max_get_burst;
    extern const SettingsUInt64 azure_max_put_rps;
    extern const SettingsUInt64 azure_max_put_burst;

}

namespace ServerSetting
{
    extern const ServerSettingsSeconds keep_alive_timeout;
    extern const ServerSettingsUInt64 max_keep_alive_requests;
}


namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int AZURE_BLOB_STORAGE_ERROR;
}

namespace AzureBlobStorage
{

static void validateStorageAccountUrl(const String & storage_account_url)
{
    const auto * storage_account_url_pattern_str = R"(http(()|s)://[a-z0-9-.:]+(()|/)[a-z0-9]*(()|/))";
    static const RE2 storage_account_url_pattern(storage_account_url_pattern_str);

    if (!re2::RE2::FullMatch(storage_account_url, storage_account_url_pattern))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Blob Storage URL is not valid, should follow the format: {}, got: {}", storage_account_url_pattern_str, storage_account_url);
}

static void validateContainerName(const String & container_name)
{
    auto len = container_name.length();
    if (len < 3 || len > 64)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "AzureBlob Storage container name is not valid, should have length between 3 and 64, but has length: {}", len);

    const auto * container_name_pattern_str = R"([a-z][a-z0-9-]+)";
    static const RE2 container_name_pattern(container_name_pattern_str);

    if (!re2::RE2::FullMatch(container_name, container_name_pattern))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "AzureBlob Storage container name is not valid, should follow the format: {}, got: {}",
                        container_name_pattern_str, container_name);
}

#if USE_AZURE_BLOB_STORAGE

static bool isConnectionString(const std::string & candidate)
{
    return !candidate.starts_with("http");
}

/// As ManagedIdentityCredential is related to the machine/pod, it's ok to have it as a singleton.
/// It is beneficial because creating this object can take a lot of time and lead to throttling.
static std::shared_ptr<Azure::Identity::ManagedIdentityCredential> getManagedIdentityCredential()
{
    static auto credential = std::make_shared<Azure::Identity::ManagedIdentityCredential>();
    return credential;
}

ContainerClientWrapper::ContainerClientWrapper(RawContainerClient client_, String blob_prefix_)
    : client(std::move(client_)), blob_prefix(std::move(blob_prefix_))
{
    if (!blob_prefix.empty() && !blob_prefix.ends_with('/'))
        blob_prefix += '/';
}

BlobClient ContainerClientWrapper::GetBlobClient(const String & blob_name) const
{
    return client.GetBlobClient(blob_prefix + blob_name);
}

BlockBlobClient ContainerClientWrapper::GetBlockBlobClient(const String & blob_name) const
{
    return client.GetBlockBlobClient(blob_prefix + blob_name);
}

/// TODO: to use or not to use executeWithRethrow for the methods below?
BlobContainerPropertiesRespones ContainerClientWrapper::GetProperties() const
try
{
    traceAzureGetProperties();
    return client.GetProperties();
}
catch (const Azure::Storage::StorageException & e)
{
    rethrowAzureException(e, blob_prefix);
}

Azure::Response<Azure::Storage::Blobs::Models::BlobProperties> ContainerClientWrapper::GetBlobProperties(const String & blob_name) const
try
{
    traceAzureGetProperties();
    return client.GetBlobClient(blob_prefix + blob_name).GetProperties();
}
catch (const Azure::Storage::StorageException & e)
{
    rethrowAzureException(e, blob_prefix);
}

ListBlobsPagedResponse ContainerClientWrapper::ListBlobs(const ListBlobsOptions & options) const
try
{
    traceAzureListObjects();

    auto new_options = options;
    new_options.Prefix = blob_prefix + options.Prefix.ValueOr("");

    auto response = client.ListBlobs(new_options);

    for (auto & blob : response.Blobs)
    {
        if (!blob.Name.starts_with(blob_prefix))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected prefix '{}' in blob name '{}'", blob_prefix, blob.Name);

        blob.Name = blob.Name.substr(blob_prefix.size());
    }

    return response;
}
catch (const Azure::Storage::StorageException & e)
{
    rethrowAzureException(e, blob_prefix);
}

bool ContainerClientWrapper::IsClientForDisk() const
{
    return client.GetClickhouseOptions().IsClientForDisk;
}

BlobContainerBatch ContainerClientWrapper::CreateBatch() const
{
    return client.CreateBatch();
}

BlobBatchResultResponse ContainerClientWrapper::SubmitBatch(const BlobContainerBatch & batch) const
{
    return client.SubmitBatch(batch);
}

String ContainerClientWrapper::GetBlobPath(const String & blob_name) const
{
    return blob_prefix + blob_name;
}

// ----- Universal tracing helpers (ProfileEvents pairs) -----------------------

void ContainerClientWrapper::traceAzureListObjects(size_t count) const
{
    ProfileEvents::increment(ProfileEvents::AzureListObjects, count);
    if (IsClientForDisk())
        ProfileEvents::increment(ProfileEvents::DiskAzureListObjects, count);
}

void ContainerClientWrapper::traceAzureGetProperties() const
{
    ProfileEvents::increment(ProfileEvents::AzureGetProperties);
    if (IsClientForDisk())
        ProfileEvents::increment(ProfileEvents::DiskAzureGetProperties);
}

void ContainerClientWrapper::traceAzureDeleteObjects(size_t count) const
{
    ProfileEvents::increment(ProfileEvents::AzureDeleteObjects, count);
    if (IsClientForDisk())
        ProfileEvents::increment(ProfileEvents::DiskAzureDeleteObjects, count);
}

void ContainerClientWrapper::traceAzureUpload() const
{
    ProfileEvents::increment(ProfileEvents::AzureUpload);
    if (IsClientForDisk())
        ProfileEvents::increment(ProfileEvents::DiskAzureUpload);
}

void ContainerClientWrapper::traceAzureStageBlock() const
{
    ProfileEvents::increment(ProfileEvents::AzureStageBlock);
    if (IsClientForDisk())
        ProfileEvents::increment(ProfileEvents::DiskAzureStageBlock);
}

void ContainerClientWrapper::traceAzureCommitBlockList() const
{
    ProfileEvents::increment(ProfileEvents::AzureCommitBlockList);
    if (IsClientForDisk())
        ProfileEvents::increment(ProfileEvents::DiskAzureCommitBlockList);
}

void ContainerClientWrapper::traceAzureCopyObject() const
{
    ProfileEvents::increment(ProfileEvents::AzureCopyObject);
    if (IsClientForDisk())
        ProfileEvents::increment(ProfileEvents::DiskAzureCopyObject);
}

void ContainerClientWrapper::traceAzureGetObject() const
{
    ProfileEvents::increment(ProfileEvents::AzureGetObject);
    if (IsClientForDisk())
        ProfileEvents::increment(ProfileEvents::DiskAzureGetObject);
}

// ----- Universal BlobStorageLog helpers --------------------------------------

void ContainerClientWrapper::logBlobStorageEventOnSuccess(
    const BlobStorageLogWriterPtr & blob_log,
    BlobStorageLogElement::EventType event_type,
    const String & container_for_logging,
    const String & blob_path_for_logging,
    size_t data_size,
    UInt64 elapsed_microseconds,
    const String & local_path_for_logging)
{
    if (!blob_log)
        return;
    blob_log->addEvent(
        event_type,
        /* bucket */ container_for_logging,
        /* remote_path */ blob_path_for_logging,
        /* local_path */ local_path_for_logging,
        /* data_size */ data_size,
        elapsed_microseconds,
        /* error_code */ 0,
        /* error_message */ {});
}

void ContainerClientWrapper::logBlobStorageEventOnFailure(
    const BlobStorageLogWriterPtr & blob_log,
    BlobStorageLogElement::EventType event_type,
    const String & container_for_logging,
    const String & blob_path_for_logging,
    size_t data_size,
    UInt64 elapsed_microseconds,
    Int32 status_code,
    const String & error_message,
    const String & local_path_for_logging)
{
    if (!blob_log)
        return;
    blob_log->addEvent(
        event_type,
        /* bucket */ container_for_logging,
        /* remote_path */ blob_path_for_logging,
        /* local_path */ local_path_for_logging,
        /* data_size */ data_size,
        elapsed_microseconds,
        status_code,
        error_message);
}

void ContainerClientWrapper::applyAccessConditions(
    Azure::Storage::Blobs::BlobAccessConditions & access_conditions,
    const WriteSettings & write_settings)
{
    if (!write_settings.object_storage_write_if_none_match.empty())
        access_conditions.IfNoneMatch = Azure::ETag(write_settings.object_storage_write_if_none_match);
    if (!write_settings.object_storage_write_if_match.empty())
        access_conditions.IfMatch = Azure::ETag(write_settings.object_storage_write_if_match);
}

// ----- Data-plane wrappers ---------------------------------------------------

Azure::Response<Azure::Storage::Blobs::Models::DownloadBlobResult>
ContainerClientWrapper::downloadRange(
    const String & blob_name,
    size_t range_offset,
    size_t range_length,
    size_t num_tries,
    LoggerPtr log,
    const BlobStorageLogWriterPtr & blob_log,
    const String & container_for_logging,
    size_t outer_attempt) const
{
    return executeWithRetryRethrow(
        log, blob_name, num_tries,
        [&](size_t /*inner_attempt*/)
        {
            Azure::Storage::Blobs::DownloadBlobOptions options;
            Azure::Nullable<int64_t> length{};
            if (range_length != 0)
                length = {static_cast<int64_t>(range_length)};
            options.Range = {static_cast<int64_t>(range_offset), length};

            traceAzureGetObject();
            /// The SDK retry-context attempt is the caller's outer attempt
            /// (held constant across this helper's inner retries) — preserves
            /// pre-refactor behaviour where `azure_context` was built once
            /// from `attempt` before the inner retry loop.
            auto azure_context = Azure::Core::Context()
                .WithValue(PocoAzureHTTPClient::getSDKContextKeyForBufferRetry(), outer_attempt);

            Stopwatch watch;
            try
            {
                auto response = client.GetBlobClient(blob_prefix + blob_name).Download(options, azure_context);
                logBlobStorageEventOnSuccess(
                    blob_log, BlobStorageLogElement::EventType::Read,
                    container_for_logging, blob_prefix + blob_name,
                    /* data_size */ static_cast<size_t>(response.Value.BodyStream->Length()),
                    watch.elapsedMicroseconds());
                return response;
            }
            catch (const Azure::Core::RequestFailedException & e)
            {
                logBlobStorageEventOnFailure(
                    blob_log, BlobStorageLogElement::EventType::Read,
                    container_for_logging, blob_prefix + blob_name,
                    range_length, watch.elapsedMicroseconds(),
                    static_cast<Int32>(e.StatusCode), e.Message);
                ProfileEvents::increment(ProfileEvents::ReadBufferFromAzureRequestsErrors);
                throw;
            }
        });
}

void ContainerClientWrapper::uploadSinglePart(
    const String & blob_name,
    const uint8_t * data,
    size_t data_size,
    const WriteSettings * write_settings,
    size_t num_tries,
    LoggerPtr log,
    const BlobStorageLogWriterPtr & blob_log,
    const String & container_for_logging) const
{
    executeWithRetryRethrow(
        log, blob_name, num_tries,
        [&](size_t attempt)
        {
            /// WriteBuffer path: scope the IO-write ResourceGuard around the SDK
            /// call. data_size=0 → ResourceGuard is a no-op (zero-cost requests
            /// are ignored), matching today's empty-upload behaviour. Copy path
            /// (write_settings == nullptr) skips the guard entirely.
            std::optional<ResourceGuard> rlock;
            if (write_settings)
                rlock.emplace(ResourceGuard::Metrics::getIOWrite(),
                              write_settings->io_scheduling.write_resource_link,
                              data_size);

            Azure::Core::IO::MemoryBodyStream stream(data, data_size);

            Azure::Storage::Blobs::UploadBlockBlobOptions options;
            Azure::Core::Context azure_context;
            if (write_settings)
            {
                applyAccessConditions(options.AccessConditions, *write_settings);
                azure_context = Azure::Core::Context()
                    .WithValue(PocoAzureHTTPClient::getSDKContextKeyForBufferRetry(), attempt);
            }

            traceAzureUpload();
            auto bbc = client.GetBlockBlobClient(blob_prefix + blob_name);

            Stopwatch watch;
            try
            {
                bbc.Upload(stream, options, azure_context);
                logBlobStorageEventOnSuccess(
                    blob_log, BlobStorageLogElement::EventType::Upload,
                    container_for_logging, blob_prefix + blob_name,
                    data_size, watch.elapsedMicroseconds());
                if (rlock)
                    rlock->unlock(data_size);
            }
            catch (const Azure::Core::RequestFailedException & e)
            {
                logBlobStorageEventOnFailure(
                    blob_log, BlobStorageLogElement::EventType::Upload,
                    container_for_logging, blob_prefix + blob_name,
                    data_size, watch.elapsedMicroseconds(),
                    static_cast<Int32>(e.StatusCode), e.Message);
                throw;
            }
        });
}

void ContainerClientWrapper::stageBlock(
    const String & blob_name,
    const String & block_id,
    const uint8_t * data,
    size_t data_size,
    const WriteSettings * write_settings,
    size_t num_tries,
    LoggerPtr log,
    const BlobStorageLogWriterPtr & blob_log,
    const String & container_for_logging) const
{
    executeWithRetryRethrow(
        log, blob_name, num_tries,
        [&](size_t attempt)
        {
            std::optional<ResourceGuard> rlock;
            if (write_settings)
                rlock.emplace(ResourceGuard::Metrics::getIOWrite(),
                              write_settings->io_scheduling.write_resource_link,
                              data_size);

            Azure::Core::IO::MemoryBodyStream stream(data, data_size);

            Azure::Core::Context azure_context;
            if (write_settings)
                azure_context = Azure::Core::Context()
                    .WithValue(PocoAzureHTTPClient::getSDKContextKeyForBufferRetry(), attempt);

            traceAzureStageBlock();
            auto bbc = client.GetBlockBlobClient(blob_prefix + blob_name);

            Stopwatch watch;
            try
            {
                bbc.StageBlock(block_id, stream, Azure::Storage::Blobs::StageBlockOptions{}, azure_context);
                logBlobStorageEventOnSuccess(
                    blob_log, BlobStorageLogElement::EventType::MultiPartUploadWrite,
                    container_for_logging, blob_prefix + blob_name,
                    data_size, watch.elapsedMicroseconds());
                if (rlock)
                    rlock->unlock(data_size);
            }
            catch (const Azure::Core::RequestFailedException & e)
            {
                logBlobStorageEventOnFailure(
                    blob_log, BlobStorageLogElement::EventType::MultiPartUploadWrite,
                    container_for_logging, blob_prefix + blob_name,
                    data_size, watch.elapsedMicroseconds(),
                    static_cast<Int32>(e.StatusCode), e.Message);
                throw;
            }
        });
}

void ContainerClientWrapper::commitBlockList(
    const String & blob_name,
    const std::vector<std::string> & block_ids,
    const WriteSettings * write_settings,
    size_t num_tries,
    LoggerPtr log,
    const BlobStorageLogWriterPtr & blob_log,
    const String & container_for_logging) const
{
    executeWithRetryRethrow(
        log, blob_name, num_tries,
        [&](size_t attempt)
        {
            Azure::Storage::Blobs::CommitBlockListOptions options;
            Azure::Core::Context azure_context;
            if (write_settings)
            {
                applyAccessConditions(options.AccessConditions, *write_settings);
                azure_context = Azure::Core::Context()
                    .WithValue(PocoAzureHTTPClient::getSDKContextKeyForBufferRetry(), attempt);
            }

            traceAzureCommitBlockList();
            auto bbc = client.GetBlockBlobClient(blob_prefix + blob_name);

            Stopwatch watch;
            try
            {
                bbc.CommitBlockList(block_ids, options, azure_context);
                logBlobStorageEventOnSuccess(
                    blob_log, BlobStorageLogElement::EventType::MultiPartUploadComplete,
                    container_for_logging, blob_prefix + blob_name,
                    /* data_size */ 0, watch.elapsedMicroseconds());
            }
            catch (const Azure::Core::RequestFailedException & e)
            {
                logBlobStorageEventOnFailure(
                    blob_log, BlobStorageLogElement::EventType::MultiPartUploadComplete,
                    container_for_logging, blob_prefix + blob_name,
                    /* data_size */ 0, watch.elapsedMicroseconds(),
                    static_cast<Int32>(e.StatusCode), e.Message);
                throw;
            }
        });
}

void ContainerClientWrapper::deleteBlobSingleWithBlobStorageLog(
    const String & blob_name,
    bool if_exists,
    const BlobStorageLogWriterPtr & blob_log,
    const String & container_for_logging,
    const String & local_path_for_logging,
    size_t bytes_size_for_logging) const
{
    traceAzureDeleteObjects(1);

    const String full_path = blob_prefix + blob_name;
    Stopwatch watch;

    try
    {
        auto delete_info = client.GetBlobClient(full_path).Delete();
        if (!if_exists && !delete_info.Value.Deleted)
            throw Exception(
                ErrorCodes::AZURE_BLOB_STORAGE_ERROR,
                "Failed to delete file (path: {}) in AzureBlob Storage, reason: {}",
                full_path,
                delete_info.RawResponse ? delete_info.RawResponse->GetReasonPhrase() : "Unknown");

        logBlobStorageEventOnSuccess(
            blob_log, BlobStorageLogElement::EventType::Delete,
            container_for_logging, full_path,
            bytes_size_for_logging, watch.elapsedMicroseconds(),
            local_path_for_logging);
    }
    catch (const Azure::Storage::StorageException & e)
    {
        logBlobStorageEventOnFailure(
            blob_log, BlobStorageLogElement::EventType::Delete,
            container_for_logging, full_path,
            bytes_size_for_logging, watch.elapsedMicroseconds(),
            static_cast<Int32>(e.StatusCode), e.Message,
            local_path_for_logging);

        /// `if_exists=true` swallows a NotFound (object already gone is fine).
        if (if_exists && e.StatusCode == Azure::Core::Http::HttpStatusCode::NotFound)
            return;
        if (!if_exists)
            rethrowAzureException(e, full_path);

        tryLogCurrentException(__PRETTY_FUNCTION__);
        rethrowAzureException(e, full_path);
    }
}

DeleteBlobResultDeferredResponse ContainerClientWrapper::addDeleteBlobToBatch(
    BlobContainerBatch & batch,
    const String & blob_name) const
{
    return batch.DeleteBlob(blob_prefix + blob_name);
}

std::map<std::string, std::string> ContainerClientWrapper::getBlobTagsForUpdate(const String & blob_name) const
{
    auto response = client.GetBlobClient(blob_prefix + blob_name).GetTags();
    return std::move(response.Value);
}

void ContainerClientWrapper::setBlobTags(const String & blob_name, const std::map<std::string, std::string> & tags) const
{
    client.GetBlobClient(blob_prefix + blob_name).SetTags(tags);
}

bool ContainerClientWrapper::UpdateBlobTag(
    const String & blob_name,
    const String & tag_key,
    const String & tag_value,
    LoggerPtr log) const
{
    return executeWithRethrow(tag_key, [&]
    {
        auto tags = getBlobTagsForUpdate(blob_name);
        const auto tag_iter = tags.find(tag_key);
        if (tag_iter != tags.end() && tag_iter->second == tag_value)
        {
            LOG_TRACE(log, "Azure blob {} skipped as it already had the tag {}={}",
                      blob_name, tag_key, tag_value);
            return false;
        }
        tags[tag_key] = tag_value;
        setBlobTags(blob_name, tags);
        LOG_TRACE(log, "Tags of Azure blob {} updated", blob_name);
        return true;
    });
}

String ContainerClientWrapper::GetBlobUrl(const String & blob_name) const
{
    return client.GetBlockBlobClient(blob_prefix + blob_name).GetUrl();
}

/// The Azure SDK throws std::logic_error subtypes (std::invalid_argument from
/// std::stoi for malformed ports, std::out_of_range for port overflow) when a
/// connection string or blob URL has malformed components. These are user-input
/// errors but must be translated to DB::Exception, otherwise they propagate to
/// getCurrentExceptionMessageAndPattern, which catches std::logic_error and calls
/// abortOnFailedAssertion in debug/sanitizer builds — turning a user typo into a
/// "Logical error" abort.
/// TODO: rename & simplify
[[noreturn]] static void translateAzureSdkParseError(const std::logic_error & e)
{
    throw Exception(ErrorCodes::BAD_ARGUMENTS,
        "Failed to parse Azure connection string or blob URL: {}", e.what());
}

String ConnectionParams::getConnectionURL() const
{
    if (std::holds_alternative<ConnectionString>(auth_method))
    {
        try
        {
            auto parsed_connection_string = Azure::Storage::_internal::ParseConnectionString(endpoint.storage_account_url);
            return parsed_connection_string.BlobServiceUrl.GetAbsoluteUrl();
        }
        catch (const std::logic_error & e)
        {
            translateAzureSdkParseError(e);
        }
    }

    return endpoint.storage_account_url;
}

std::unique_ptr<ServiceClient> ConnectionParams::createForService() const
{
    try
    {
        return std::visit([this]<typename T>(const T & auth)
        {
            if constexpr (std::is_same_v<T, ConnectionString>)
                return std::make_unique<ServiceClient>(ServiceClient::CreateFromConnectionString(auth.toUnderType(), client_options));
            else
                return std::make_unique<ServiceClient>(endpoint.getServiceEndpoint(), auth, client_options);
        }, auth_method);
    }
    catch (const std::logic_error & e)
    {
        translateAzureSdkParseError(e);
    }
}

std::unique_ptr<ContainerClient> ConnectionParams::createForContainer() const
{
    try
    {
        if (!endpoint.sas_auth.empty())
        {
            RawContainerClient raw_client{endpoint.getContainerEndpoint(), client_options};
            return std::make_unique<ContainerClient>(std::move(raw_client), endpoint.prefix);
        }

        return std::visit([this]<typename T>(const T & auth)
        {
            if constexpr (std::is_same_v<T, ConnectionString>)
            {
                auto raw_client = RawContainerClient::CreateFromConnectionString(auth.toUnderType(), endpoint.container_name, client_options);
                return std::make_unique<ContainerClient>(std::move(raw_client), endpoint.prefix);
            }
            else
            {
                RawContainerClient raw_client{endpoint.getContainerEndpoint(), auth, client_options};
                return std::make_unique<ContainerClient>(std::move(raw_client), endpoint.prefix);
            }
        }, auth_method);
    }
    catch (const std::logic_error & e)
    {
        translateAzureSdkParseError(e);
    }
}

void processURL(const String & url, const String & container_name, Endpoint & endpoint, AuthMethod & auth_method)
{
    endpoint.container_name = container_name;

    if (isConnectionString(url))
    {
        endpoint.storage_account_url = url;
        auth_method = ConnectionString{url};
        return;
    }

    auto pos = url.find('?');

    /// If conneciton_url does not have '?', then its not SAS
    if (pos == std::string::npos)
    {
        endpoint.storage_account_url = url;
        auth_method = std::make_shared<Azure::Identity::WorkloadIdentityCredential>();
    }
    else
    {
        endpoint.storage_account_url = url.substr(0, pos);
        endpoint.sas_auth = url.substr(pos + 1);
        auth_method = getManagedIdentityCredential();
    }
}

static bool containerExists(const ContainerClient & client)
{
    try
    {
        client.GetProperties();
        return true;
    }
    catch (const Azure::Storage::StorageException & e)
    {
        if (e.StatusCode == Azure::Core::Http::HttpStatusCode::NotFound)
            return false;

        /// Our HTTP client wraps transport-level failures (DNS, connection refused, etc.)
        /// as InternalServerError. A transient network error should not prevent the server
        /// from starting — assume the container exists and let actual I/O operations fail
        /// with a clear error later if it doesn't.
        if (e.StatusCode == Azure::Core::Http::HttpStatusCode::InternalServerError)
        {
            LOG_WARNING(getLogger("AzureBlobStorageCommon"),
                "Failed to check container existence: {}. Assuming the container exists.",
                e.Message);
            return true;
        }

        rethrowAzureException(e, "Azure container");
    }
}

std::unique_ptr<ContainerClient> getContainerClient(const ConnectionParams & params, bool readonly)
{
    if (!params.endpoint.sas_auth.empty() || !params.endpoint.additional_params.empty())
        return params.createForContainer();

    if (params.endpoint.container_already_exists.value_or(false) || readonly)
    {
        return params.createForContainer();
    }

    if (!params.endpoint.container_already_exists.has_value())
    {
        auto container_client = params.createForContainer();
        if (containerExists(*container_client))
            return container_client;
    }

    try
    {
        auto service_client = params.createForService();

        /// NOTE: This is a service-level Azure SDK call — `ContainerClientWrapper` is not
        /// in scope yet because we are creating the container that the wrapper will then
        /// represent. Tracing and SDK access stay raw here. A future `ServiceClientWrapper`
        /// could absorb it, but that's out of scope for T2.
        ProfileEvents::increment(ProfileEvents::AzureCreateContainer);
        if (params.client_options.ClickhouseOptions.IsClientForDisk)
            ProfileEvents::increment(ProfileEvents::DiskAzureCreateContainer);

        auto raw_client = service_client->CreateBlobContainer(params.endpoint.container_name).Value;
        return std::make_unique<ContainerClient>(std::move(raw_client), params.endpoint.prefix);
    }
    catch (const Azure::Storage::StorageException & e)
    {
        /// If container_already_exists is not set (in config), ignore already exists error. Conflict - The specified container already exists.
        /// To avoid race with creation of container, handle this error despite that we have already checked the existence of container.
        if (!params.endpoint.container_already_exists.has_value() && e.StatusCode == Azure::Core::Http::HttpStatusCode::Conflict)
            return params.createForContainer();
        rethrowAzureException(e, params.endpoint.container_name);
    }
}

AuthMethod getAuthMethod(const Poco::Util::AbstractConfiguration & config, const String & config_prefix)
{
    if (config.has(config_prefix + ".account_key") && config.has(config_prefix + ".account_name"))
    {
        return std::make_shared<Azure::Storage::StorageSharedKeyCredential>(
            config.getString(config_prefix + ".account_name"),
            config.getString(config_prefix + ".account_key")
        );
    }

    if (config.has(config_prefix + ".connection_string"))
        return ConnectionString{config.getString(config_prefix + ".connection_string")};

    if (config.getBool(config_prefix + ".use_workload_identity", false))
        return std::make_shared<Azure::Identity::WorkloadIdentityCredential>();

    return getManagedIdentityCredential();
}

BlobClientOptions getClientOptions(
    const ContextPtr & context,
    const Settings & settings,
    const RequestSettings & request_settings,
    bool for_disk)
{
    Azure::Core::Http::Policies::RetryOptions retry_options;
    retry_options.MaxRetries = static_cast<Int32>(request_settings.sdk_max_retries);
    retry_options.RetryDelay = std::chrono::milliseconds(request_settings.sdk_retry_initial_backoff_ms);
    retry_options.MaxRetryDelay = std::chrono::milliseconds(request_settings.sdk_retry_max_backoff_ms);
    Azure::Storage::Blobs::BlobClientOptions client_options;
    client_options.Retry = retry_options;
    client_options.ClickhouseOptions = Azure::Storage::Blobs::ClickhouseClientOptions{.IsClientForDisk=for_disk};

    // Initialize HTTP request throttling
    HTTPRequestThrottler request_throttler;

    if (settings[Setting::azure_max_get_rps] > 0 || settings[Setting::azure_max_get_burst] > 0)
    {
        request_throttler.get_throttler = std::make_shared<Throttler>(
            settings[Setting::azure_max_get_rps],
            settings[Setting::azure_max_get_burst],
            ProfileEvents::AzureGetRequestThrottlerCount,
            ProfileEvents::AzureGetRequestThrottlerSleepMicroseconds);
        request_throttler.get_blocked = ProfileEvents::AzureGetRequestThrottlerBlocked;

        // Update additional profile events for DiskAzure
        if (for_disk)
        {
            request_throttler.disk_get_amount = ProfileEvents::DiskAzureGetRequestThrottlerCount;
            request_throttler.disk_get_blocked = ProfileEvents::DiskAzureGetRequestThrottlerBlocked;
            request_throttler.disk_get_sleep_us = ProfileEvents::DiskAzureGetRequestThrottlerSleepMicroseconds;
        }
    }

    if (settings[Setting::azure_max_put_rps] > 0 || settings[Setting::azure_max_put_burst] > 0)
    {
        request_throttler.put_throttler = std::make_shared<Throttler>(
            settings[Setting::azure_max_put_rps],
            settings[Setting::azure_max_put_burst],
            ProfileEvents::AzurePutRequestThrottlerCount,
            ProfileEvents::AzurePutRequestThrottlerSleepMicroseconds);
        request_throttler.put_blocked = ProfileEvents::AzurePutRequestThrottlerBlocked;

        // Update additional profile events for DiskAzure
        if (for_disk)
        {
            request_throttler.disk_put_amount = ProfileEvents::DiskAzurePutRequestThrottlerCount;
            request_throttler.disk_put_blocked = ProfileEvents::DiskAzurePutRequestThrottlerBlocked;
            request_throttler.disk_put_sleep_us = ProfileEvents::DiskAzurePutRequestThrottlerSleepMicroseconds;
        }
    }

    auto http_keep_alive_seconds = static_cast<size_t>(context->getServerSettings()[ServerSetting::keep_alive_timeout].totalSeconds());
    auto tcp_keep_alive_milliseconds = static_cast<size_t>(settings[Setting::tcp_keep_alive_timeout].totalMilliseconds());

    PocoAzureHTTPClientConfiguration conf
    {
        .remote_host_filter = context->getRemoteHostFilter(),
        .max_redirects = settings[Setting::azure_max_redirects],
        .for_disk_azure = for_disk,
        .request_throttler = request_throttler,
        .extra_headers = HTTPHeaderEntries{}, /// No extra headers so far
        .connect_timeout_ms = settings[Setting::azure_connect_timeout_ms],
        .request_timeout_ms = settings[Setting::azure_request_timeout_ms],
        .tcp_keep_alive_interval_ms = tcp_keep_alive_milliseconds,
        .use_adaptive_timeouts = settings[Setting::azure_use_adaptive_timeouts],
        .http_keep_alive_timeout = http_keep_alive_seconds, // Convert seconds to milliseconds
        .http_keep_alive_max_requests = context->getServerSettings()[ServerSetting::max_keep_alive_requests],
        .http_max_fields = settings[Setting::http_max_fields],
        .http_max_field_name_size = settings[Setting::http_max_field_name_size],
        .http_max_field_value_size = settings[Setting::http_max_field_value_size]};

    client_options.Transport.Transport = std::make_shared<PocoAzureHTTPClient>(conf);
    return client_options;
}

#endif

Endpoint processEndpoint(const Poco::Util::AbstractConfiguration & config, const String & config_prefix)
{
    String storage_url;
    String account_name;
    String account_key;
    String container_name;
    String prefix;
    bool endpoint_contains_account_name = false;

    auto get_container_name = [&]
    {
        if (config.has(config_prefix + ".container_name"))
            return config.getString(config_prefix + ".container_name");

        if (config.has(config_prefix + ".container"))
            return config.getString(config_prefix + ".container");

        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Expected either `container` or `container_name` parameter in config");
    };

    if (config.has(config_prefix + ".endpoint"))
    {
        String endpoint = config.getString(config_prefix + ".endpoint");

        /// For some authentication methods account name is not present in the endpoint
        /// 'endpoint_contains_account_name' bool is used to understand how to split the endpoint (default : true)
        endpoint_contains_account_name = config.getBool(config_prefix + ".endpoint_contains_account_name", true);

        size_t pos = endpoint.find("//");
        if (pos == std::string::npos)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected '//' in endpoint");

        if (endpoint_contains_account_name)
        {
            size_t acc_pos_begin = endpoint.find('/', pos + 2);
            if (acc_pos_begin == std::string::npos)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected account_name in endpoint");

            storage_url = endpoint.substr(0, acc_pos_begin);
            size_t acc_pos_end = endpoint.find('/', acc_pos_begin + 1);

            if (acc_pos_end == std::string::npos)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected container_name in endpoint");

            account_name = endpoint.substr(acc_pos_begin + 1, acc_pos_end - acc_pos_begin - 1);

            size_t cont_pos_end = endpoint.find('/', acc_pos_end + 1);

            if (cont_pos_end != std::string::npos)
            {
                container_name = endpoint.substr(acc_pos_end + 1, cont_pos_end - acc_pos_end - 1);
                prefix = endpoint.substr(cont_pos_end + 1);
            }
            else
            {
                container_name = endpoint.substr(acc_pos_end + 1);
            }
        }
        else
        {
            size_t cont_pos_begin = endpoint.find('/', pos + 2);

            if (cont_pos_begin == std::string::npos)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected container_name in endpoint");

            storage_url = endpoint.substr(0, cont_pos_begin);
            size_t cont_pos_end = endpoint.find('/', cont_pos_begin + 1);

            if (cont_pos_end != std::string::npos)
            {
                container_name = endpoint.substr(cont_pos_begin + 1,cont_pos_end - cont_pos_begin - 1);
                prefix = endpoint.substr(cont_pos_end + 1);
            }
            else
            {
                container_name = endpoint.substr(cont_pos_begin + 1);
            }

            /// When the account name is not embedded in the endpoint path, read it
            /// from the explicit `account_name` config key if provided.
            /// It will not be appended to service/container URLs (add_account_name_to_url = false),
            /// but is stored for use by external systems such as delta-kernel-rs.
            if (config.has(config_prefix + ".account_name"))
                account_name = config.getString(config_prefix + ".account_name");
        }
        if (config.has(config_prefix + ".account_key"))
                account_key = config.getString(config_prefix + ".account_key");
        if (config.has(config_prefix + ".endpoint_subpath"))
        {
            String endpoint_subpath = config.getString(config_prefix + ".endpoint_subpath");
            if (!prefix.empty() && !prefix.ends_with('/'))
                prefix += '/';
            prefix += endpoint_subpath;
        }
    }
    else if (config.has(config_prefix + ".connection_string"))
    {
        storage_url = config.getString(config_prefix + ".connection_string");
        if (storage_url.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Azure Blob Storage connection string is empty. "
                "If it is specified via an environment variable, please check that the variable is set");
        container_name = get_container_name();
    }
    else if (config.has(config_prefix + ".storage_account_url"))
    {
        storage_url = config.getString(config_prefix + ".storage_account_url");
        validateStorageAccountUrl(storage_url);
        container_name = get_container_name();

        /// The account name is not part of the URL here; read it from config if provided.
        /// It will not be appended to service/container URLs (add_account_name_to_url defaults
        /// to false for this path), but is stored for use by external systems such as delta-kernel-rs.
        if (config.has(config_prefix + ".account_name"))
            account_name = config.getString(config_prefix + ".account_name");
        if (config.has(config_prefix + ".account_key"))
            account_key = config.getString(config_prefix + ".account_key");
    }
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected either `storage_account_url` or `connection_string` or `endpoint` in config");

    if (!container_name.empty())
        validateContainerName(container_name);

    std::optional<bool> container_already_exists {};
    if (config.has(config_prefix + ".container_already_exists"))
        container_already_exists = {config.getBool(config_prefix + ".container_already_exists")};

    return {storage_url, account_name, account_key, container_name, prefix, /* sas_auth */"", /* additional_params */"", container_already_exists, endpoint_contains_account_name};
}

std::unique_ptr<RequestSettings> getRequestSettings(const Settings & query_settings)
{
    auto settings = std::make_unique<RequestSettings>();

    settings->max_single_part_upload_size = query_settings[Setting::azure_max_single_part_upload_size];
    settings->max_single_read_retries = query_settings[Setting::azure_max_single_read_retries];
    settings->max_single_download_retries = query_settings[Setting::azure_max_single_read_retries];
    settings->list_object_keys_size = query_settings[Setting::azure_list_object_keys_size];
    settings->min_upload_part_size = query_settings[Setting::azure_min_upload_part_size];
    settings->max_upload_part_size = query_settings[Setting::azure_max_upload_part_size];
    settings->max_single_part_copy_size = query_settings[Setting::azure_max_single_part_copy_size];
    settings->max_blocks_in_multipart_upload = query_settings[Setting::azure_max_blocks_in_multipart_upload];
    settings->max_unexpected_write_error_retries = query_settings[Setting::azure_max_unexpected_write_error_retries];
    settings->max_inflight_parts_for_one_file = query_settings[Setting::azure_max_inflight_parts_for_one_file];
    settings->strict_upload_part_size = query_settings[Setting::azure_strict_upload_part_size];
    settings->upload_part_size_multiply_factor = query_settings[Setting::azure_upload_part_size_multiply_factor];
    settings->upload_part_size_multiply_parts_count_threshold = query_settings[Setting::azure_upload_part_size_multiply_parts_count_threshold];
    settings->sdk_max_retries = query_settings[Setting::azure_sdk_max_retries];
    settings->sdk_retry_initial_backoff_ms = query_settings[Setting::azure_sdk_retry_initial_backoff_ms];
    settings->sdk_retry_max_backoff_ms = query_settings[Setting::azure_sdk_retry_max_backoff_ms];
    settings->check_objects_after_upload = query_settings[Setting::azure_check_objects_after_upload];

    return settings;
}

std::unique_ptr<RequestSettings> getRequestSettingsForBackup(ContextPtr context, String endpoint, bool use_native_copy)
{
    auto settings = getRequestSettings(context->getSettingsRef());

    auto endpoint_settings = context->getStorageAzureSettings().getSettings(endpoint);
    if (endpoint_settings)
        settings->use_native_copy = endpoint_settings->use_native_copy;

    if (!use_native_copy)
        settings->use_native_copy = false;

    return settings;
}

std::unique_ptr<RequestSettings> getRequestSettings(const Poco::Util::AbstractConfiguration & config, const String & config_prefix, const Settings & settings_ref)
{
    auto settings = std::make_unique<RequestSettings>();

    settings->min_bytes_for_seek = config.getUInt64(config_prefix + ".min_bytes_for_seek", 1024 * 1024);
    settings->use_native_copy = config.getBool(config_prefix + ".use_native_copy", false);
    settings->read_only = config.getBool(config_prefix + ".readonly", false);

    settings->max_single_part_upload_size = config.getUInt64(config_prefix + ".max_single_part_upload_size", settings_ref[Setting::azure_max_single_part_upload_size]);
    settings->max_single_read_retries = config.getUInt64(config_prefix + ".max_single_read_retries", settings_ref[Setting::azure_max_single_read_retries]);
    settings->max_single_download_retries = config.getUInt64(config_prefix + ".max_single_download_retries", settings_ref[Setting::azure_max_single_read_retries]);
    settings->list_object_keys_size = config.getUInt64(config_prefix + ".list_object_keys_size", settings_ref[Setting::azure_list_object_keys_size]);
    settings->min_upload_part_size = config.getUInt64(config_prefix + ".min_upload_part_size", settings_ref[Setting::azure_min_upload_part_size]);
    settings->max_upload_part_size = config.getUInt64(config_prefix + ".max_upload_part_size", settings_ref[Setting::azure_max_upload_part_size]);
    settings->max_single_part_copy_size = config.getUInt64(config_prefix + ".max_single_part_copy_size", settings_ref[Setting::azure_max_single_part_copy_size]);
    settings->max_blocks_in_multipart_upload = config.getUInt64(config_prefix + ".max_blocks_in_multipart_upload", settings_ref[Setting::azure_max_blocks_in_multipart_upload]);
    settings->max_unexpected_write_error_retries = config.getUInt64(config_prefix + ".max_unexpected_write_error_retries", settings_ref[Setting::azure_max_unexpected_write_error_retries]);
    settings->max_inflight_parts_for_one_file = config.getUInt64(config_prefix + ".max_inflight_parts_for_one_file", settings_ref[Setting::azure_max_inflight_parts_for_one_file]);
    settings->strict_upload_part_size = config.getUInt64(config_prefix + ".strict_upload_part_size", settings_ref[Setting::azure_strict_upload_part_size]);
    settings->upload_part_size_multiply_factor = config.getUInt64(config_prefix + ".upload_part_size_multiply_factor", settings_ref[Setting::azure_upload_part_size_multiply_factor]);
    settings->upload_part_size_multiply_parts_count_threshold = config.getUInt64(config_prefix + ".upload_part_size_multiply_parts_count_threshold", settings_ref[Setting::azure_upload_part_size_multiply_parts_count_threshold]);

    settings->sdk_max_retries = config.getUInt64(config_prefix + ".max_tries", settings_ref[Setting::azure_sdk_max_retries]);
    settings->sdk_retry_initial_backoff_ms = config.getUInt64(config_prefix + ".retry_initial_backoff_ms", settings_ref[Setting::azure_sdk_retry_initial_backoff_ms]);
    settings->sdk_retry_max_backoff_ms = config.getUInt64(config_prefix + ".retry_max_backoff_ms", settings_ref[Setting::azure_sdk_retry_max_backoff_ms]);

    settings->check_objects_after_upload = config.getBool(config_prefix + ".check_objects_after_upload", settings_ref[Setting::azure_check_objects_after_upload]);

    return settings;
}

}


void AzureSettingsByEndpoint::loadFromConfig(
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_prefix,
    const DB::Settings & settings)
{
    std::lock_guard lock(mutex);
    azure_settings.clear();
    if (!config.has(config_prefix))
        return;


    Poco::Util::AbstractConfiguration::Keys config_keys;
    config.keys(config_prefix, config_keys);

    for (const String & key : config_keys)
    {
        if (config.has(config_prefix + "." + key + ".object_storage_type"))
        {
            const auto &object_storage_type = config.getString(config_prefix + "." + key + ".object_storage_type");
            if (object_storage_type != "azure" && object_storage_type != "azure_blob_storage")
            {
                /// Then its not an azure config
                continue;
            }

            const auto key_path = config_prefix + "." + key;
            String endpoint_path = key_path + ".connection_string";

            if (!config.has(endpoint_path))
            {
                endpoint_path = key_path + ".storage_account_url";

                if (!config.has(endpoint_path))
                {
                    endpoint_path = key_path + ".endpoint";

                    if (!config.has(endpoint_path))
                    {
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "URL not provided for azure blob storage disk {}",
                                        object_storage_type);
                    }
                }
            }

            auto endpoint = AzureBlobStorage::processEndpoint(config, key_path);
            auto request_settings = AzureBlobStorage::getRequestSettings(config, key_path, settings);

            azure_settings.emplace(
                    endpoint.storage_account_url,
                    std::move(*request_settings));
        }
    }
}

std::optional<AzureBlobStorage::RequestSettings> AzureSettingsByEndpoint::getSettings(
    const String & endpoint) const
{
    std::lock_guard lock(mutex);
    auto next_prefix_setting = azure_settings.upper_bound(endpoint);

    /// Linear time algorithm may be replaced with logarithmic with prefix tree map.
    for (auto possible_prefix_setting = next_prefix_setting; possible_prefix_setting != azure_settings.begin();)
    {
        std::advance(possible_prefix_setting, -1);
        const auto & [endpoint_prefix, settings] = *possible_prefix_setting;
        if (endpoint.starts_with(endpoint_prefix))
            return possible_prefix_setting->second;
    }

    return {};
}

}
