#pragma once

#include <boost/filesystem.hpp>
#include <boost/shared_ptr.hpp>

#include "index_server.pb.h"
#include "pimpl/pimpl.h"
#include "index.hpp"
#include "value_db.hpp"

namespace indexer {

struct store final
    : private pimpl<store>::pointer_semantics
{
    store(StoreParameters const& parameters);
    store(boost::filesystem::path const& location);
    ~store();

    boost::filesystem::path location() const;
    boost::shared_ptr< ::indexer::index> index() const;
    boost::shared_ptr< ::indexer::value_db> db() const;
};

struct store_manager final
    : public boost::noncopyable
    , private pimpl<store_manager>::pointer_semantics
{
    store_manager();
    ~store_manager();

    typedef boost::shared_ptr<store> store_ptr;

    store_ptr create(StoreParameters const& parameters);
    store_ptr open(boost::filesystem::path const& location);
};

}

