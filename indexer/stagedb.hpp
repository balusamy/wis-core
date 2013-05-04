#pragma once

#include "pimpl/pimpl.h"
#include <boost/noncopyable.hpp>
#include <boost/filesystem.hpp>
#include <boost/utility/string_ref.hpp>

struct stage_db
    : private pimpl<stage_db>::pointer_semantics
    , public boost::noncopyable
{
    stage_db(boost::filesystem::path const& path, bool read_only = true);

    std::string get(boost::string_ref const& key);

    void append(boost::string_ref const& key, boost::string_ref const& data,
            bool transacted = true);
    void rollback();
    void commit();
};
