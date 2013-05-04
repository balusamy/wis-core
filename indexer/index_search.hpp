#pragma once

#include <rpcz/rpcz.hpp>
#include "index_server.pb.h"
#include "index_server.rpcz.h"
#include "pimpl/pimpl.h"

#include "store_manager.hpp"

namespace indexer {

struct IndexSearch
    : public IndexQueryService
    , public boost::noncopyable
    , private pimpl<IndexSearch>::pointer_semantics
{
    IndexSearch(boost::shared_ptr<store_manager> const& store_mgr);
    virtual ~IndexSearch();

private:
    virtual void useStore(const UseStore& request, rpcz::reply<Void> reply);
    virtual void wordQuery(const WordQuery& request, rpcz::reply<QueryResult> reply);
};

}

