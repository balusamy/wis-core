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
    void search(boost::string_ref const& data, size_t k, bool has_transp, results_t& results);
    void search_split(boost::string_ref const& data, size_t switch_len,
            size_t k1, bool exact_dist1, size_t k2, bool exact_dist2,
            bool has_transp, results_t& results);
    void search_exact(boost::string_ref const& data, results_t& results);
};
