#pragma once

#include <rpcz/rpcz.hpp>
#include "index_server.pb.h"
#include "index_server.rpcz.h"
#include "pimpl/pimpl.h"

namespace indexer {

struct IndexBuilder
    : public IndexBuilderService
    , public boost::noncopyable
    , private pimpl<IndexBuilder>::pointer_semantics
{
    IndexBuilder();
    virtual ~IndexBuilder();

private:
    virtual void createStore(const StoreParameters& request, rpcz::reply<Void> reply);
    virtual void openStore(const StoreParameters& request, rpcz::reply<Void> reply);
    virtual void closeStore(const Void& request, rpcz::reply<Void> reply);
    virtual void feedData(const BuilderData& request, rpcz::reply<Void> reply);
    virtual void buildIndex(const Void& request, rpcz::reply<Void> reply);
};

}

