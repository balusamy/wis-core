#pragma once

#include "pimpl/pimpl.h"
#include <boost/noncopyable.hpp>
#include <boost/filesystem.hpp>

#include "index_server.pb.h"

struct stage_db
    : private pimpl<stage_db>::pointer_semantics
    , public boost::noncopyable
{
    // TODO: pass comparator
    stage_db(boost::filesystem::path const& path, bool read_only = true);

    void append(indexer::BuilderData const& data);
};
