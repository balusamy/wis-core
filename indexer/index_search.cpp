#include "index_search.hpp"
#include <iostream>
#include <boost/filesystem.hpp>
#include <boost/format.hpp>

#include "exceptions.hpp"
#include "index.hpp"

namespace fs = boost::filesystem;

template <>
struct pimpl<indexer::IndexSearch>::implementation
{
    boost::shared_ptr<indexer::store_manager> store_mgr;
    indexer::store_manager::store_ptr store;
};

namespace indexer {

IndexSearch::IndexSearch(boost::shared_ptr<store_manager> const& store_mgr)
{
    (*this)->store_mgr = store_mgr;
}

IndexSearch::~IndexSearch()
{
}

void IndexSearch::useStore(const UseStore& request, rpcz::reply<Void> reply)
{
    implementation& impl = **this;
    try {
        std::cout << "Got useStore request: '" << request.DebugString() << "'" << std::endl;

        impl.store = impl.store_mgr->open(request.location());

    } RPC_REPORT_EXCEPTIONS(reply)
    reply.send(Void());
}

void IndexSearch::wordQuery(const WordQuery& request, rpcz::reply<QueryResult> reply)
{
    implementation& impl = **this;
    try {
        if (!impl.store)
            BOOST_THROW_EXCEPTION(common_exception()
                << errinfo_rpc_code(::rpc_error::INVALID_STORE)
                << errinfo_message("Store is not open"));

        std::cout << "Searching for '" << request.word() << "', k=" << request.maxcorrections() << std::endl;
        auto index = impl.store->index();
        auto db = impl.store->db();
        ::indexer::index::results_t results;
        index->search(request.word(), request.maxcorrections(), results);
        QueryResult pb_results;
        pb_results.set_exact_total(results.size());
        for (std::string const& result : results) {
            IndexRecord* record = pb_results.add_values();
            std::cout << "  result: " << result << std::endl;
            record->set_key(result);
            std::string const& s = db->get(result);
            record->mutable_value()->ParseFromString(s);
        }
        reply.send(pb_results);
    } RPC_REPORT_EXCEPTIONS(reply)
}

}

