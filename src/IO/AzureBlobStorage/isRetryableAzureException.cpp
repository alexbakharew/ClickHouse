#include "config.h"

#if USE_AZURE_BLOB_STORAGE
#include <IO/AzureBlobStorage/isRetryableAzureException.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int AZURE_ACCESS_DENIED;
}

bool isRetryableAzureException(const Azure::Core::RequestFailedException & e)
{
    /// Always retry transport errors.
    if (dynamic_cast<const Azure::Core::Http::TransportException *>(&e))
        return true;

    /// Azure Forbidden (403) is thrown at: a) incorrect permission assignment, b) RBAC propagation lag
    /// As it's hard to distinguish at runtime, we retry at this error to mitigate (b)
    if (e.StatusCode == Azure::Core::Http::HttpStatusCode::Forbidden)
        return true;

    /// Retry other 5xx errors just in case.
    return e.StatusCode >= Azure::Core::Http::HttpStatusCode::InternalServerError;
}

bool isAzureForbiddenException(const Azure::Core::RequestFailedException & e)
{
    return e.StatusCode == Azure::Core::Http::HttpStatusCode::Forbidden;
}

[[noreturn]] void rethrowAzureException(
    const Azure::Core::RequestFailedException & e,
    const std::string & resource)
{
    if (isAzureForbiddenException(e))
        throw Exception(
            ErrorCodes::AZURE_ACCESS_DENIED,
            "Azure refused access to `{}`: {} (HTTP {}, request id {})",
            resource,
            e.Message,
            static_cast<int>(e.StatusCode),
            e.RequestId);

    throw;
}

}

#endif
