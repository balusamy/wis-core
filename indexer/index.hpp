#pragma once

#include "pimpl/pimpl.h"
#include <boost/noncopyable.hpp>
#include <boost/filesystem.hpp>
#include <boost/utility/string_ref.hpp>

namespace indexer {

struct index
    : private pimpl<index>::pointer_semantics
    , public boost::noncopyable
{
    index(boost::filesystem::path const& path);

    typedef std::vector<std::string> results_t;

    void insert(boost::string_ref const& data);
    void search(boost::string_ref const& data, size_t k, bool has_transp, results_t& results);
};

}
