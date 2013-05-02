#pragma once

#include "pimpl/pimpl.h"
#include <boost/noncopyable.hpp>
#include <boost/filesystem.hpp>
#include <boost/utility/string_ref.hpp>

struct trie
    : private pimpl<trie>::pointer_semantics
    , public boost::noncopyable
{
    trie(boost::filesystem::path const& path, bool read_only = true);

    void insert(boost::string_ref const& data);
};
