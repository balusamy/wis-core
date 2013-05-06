#pragma once

#include "pimpl/pimpl.h"
#include <boost/noncopyable.hpp>
#include <boost/utility/string_ref.hpp>

namespace indexer {

struct value_db
    : private pimpl<value_db>::pointer_semantics
    , public boost::noncopyable
{
    struct transaction
        : private pimpl<transaction>::pointer_semantics
        , public boost::noncopyable
    {
    private:
        friend class value_db;
        transaction();

    public:
        transaction(transaction&&) = default;
        ~transaction();
        void append(boost::string_ref const& key, boost::string_ref const& data);
        void rollback();
        void commit();
    };

    value_db(boost::string_ref const& server, boost::string_ref const& ns);

    std::string get(boost::string_ref const& key) const;
    std::unique_ptr<transaction> start_tx();
};

}
