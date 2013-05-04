#include "server.hpp"
#include <iostream>
#include <boost/filesystem.hpp>
#include <boost/filesystem/fstream.hpp>
#include <boost/format.hpp>
#include <boost/iostreams/device/file.hpp>
#include <boost/iostreams/stream.hpp>
#include <leveldb/db.h>

#include "exceptions.hpp"
#include "index.hpp"

namespace fs = boost::filesystem;
namespace io = boost::iostreams;

namespace rpc_error {
static const int STORE_ALREADY_EXISTS = 1;
static const int INVALID_STORE = 2;
static const int STORE_NOT_FOUND = 3;
}

namespace indexer {

static const int STORE_FORMAT = 1;

}

template <>
struct pimpl<indexer::IndexBuilder>::implementation
{
    boost::shared_ptr<indexer::store_manager> store_mgr;
    indexer::store_manager::store_ptr store;
};

namespace indexer {

IndexBuilder::IndexBuilder(boost::shared_ptr<store_manager> const& store_mgr)
{
    (*this)->store_mgr = store_mgr;
}

IndexBuilder::~IndexBuilder()
{
}

void IndexBuilder::createStore(const StoreParameters& request, rpcz::reply<Void> reply)
{
    implementation& impl = **this;
    try {
        std::cout << "Got createStore request: '" << request.DebugString() << "'" << std::endl;

        impl.store = impl.store_mgr->create(request);

    } RPC_REPORT_EXCEPTIONS(reply)
    reply.send(Void());
}

void IndexBuilder::openStore(const StoreParameters& request, rpcz::reply<Void> reply)
{
    implementation& impl = **this;
    try {
        std::cout << "Got openStore request: '" << request.DebugString() << "'" << std::endl;

        impl.store = impl.store_mgr->open(request.location());
    } RPC_REPORT_EXCEPTIONS(reply)
    reply.send(Void());
}

void IndexBuilder::closeStore(const Void& request, rpcz::reply<Void> reply)
{
    implementation& impl = **this;
    try {
        std::cout << "Got closeStore request: '" << request.DebugString() << "'" << std::endl;
        impl.store.reset();

    } RPC_REPORT_EXCEPTIONS(reply)
    reply.send(Void());
}

void IndexBuilder::feedData(const BuilderData& request, rpcz::reply<Void> reply)
{
    implementation& impl = **this;
    try {
        if (!impl.store)
            BOOST_THROW_EXCEPTION(common_exception()
                << errinfo_rpc_code(::rpc_error::INVALID_STORE)
                << errinfo_message("Store is not open"));
        //std::cout << "Feeding " << request.records_size() << " records" << std::endl;
        auto index = impl.store->index();
        for (IndexRecord const& rec : request.records()) {
            index->insert(rec.key());
        }
    } RPC_REPORT_EXCEPTIONS(reply)
    reply.send(Void());
}

void IndexBuilder::buildIndex(const Void& request, rpcz::reply<Void> reply)
{
    try {
        BOOST_THROW_EXCEPTION(common_exception()
                << errinfo_rpc_code(rpcz::application_error::METHOD_NOT_IMPLEMENTED));
        std::cout << "Got request: '" << request.DebugString() << "'" << std::endl;
        std::cout << "Building index!!!" << std::endl;
    } RPC_REPORT_EXCEPTIONS(reply)
    reply.send(Void());
}

}

