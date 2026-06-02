#pragma once
#include <Interpreters/PreparedSets.h>
#include <Core/ProtocolDefines.h>

namespace DB
{

class FutureSet;
using FutureSetPtr = std::shared_ptr<FutureSet>;

struct SerializedSetsRegistry
{
    struct Hashing
    {
        UInt64 operator()(const FutureSet::Hash & key) const { return key.low64 ^ key.high64; }
    };

    std::unordered_map<FutureSet::Hash, FutureSetPtr, Hashing> sets;

    UInt64 version = DBMS_QUERY_PLAN_SERIALIZATION_VERSION;
};

class ColumnSet;

struct DeserializedSetsRegistry
{
    struct Hashing
    {
        UInt64 operator()(const FutureSet::Hash & key) const { return key.low64 ^ key.high64; }
    };

    std::unordered_map<FutureSet::Hash, std::list<ColumnSet *>, Hashing> sets;
};

}
