#pragma once

#include "pimpl/pimpl.h"
#include <boost/noncopyable.hpp>
#include <boost/filesystem.hpp>
#include <boost/utility/string_ref.hpp>
#include <vector>

struct trie
    : private pimpl<trie>::pointer_semantics
    , public boost::noncopyable
{
    trie(boost::filesystem::path const& path, bool read_only = true);

    typedef std::vector<std::string> results_t;

    void insert(boost::string_ref const& data);
    void search_exact(boost::string_ref const& data, results_t& results);
};
