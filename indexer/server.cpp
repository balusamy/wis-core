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
}

namespace indexer {

static const int STORE_FORMAT = 1;

}

template <>
struct pimpl<indexer::IndexBuilder>::implementation
{
    void do_create_store(const indexer::StoreParameters& request) {
        fs::path location = request.location();
        if (fs::is_directory(location)) {
            if (!request.overwrite()) {
                BOOST_THROW_EXCEPTION(common_exception()
                        << errinfo_rpc_code(::rpc_error::STORE_ALREADY_EXISTS)
                        << errinfo_message(str(boost::format("Store at %s already exists") % location)));
            } else {
                std::cout << "Recreating store at " << location << std::endl;
                if (this->index)
                    this->index.reset();
                fs::remove_all(location);
            }
        } else {
            std::cout << "Creating store at " << location << std::endl;
        }
        fs::create_directories(location);

        io::stream<io::file_sink> store_format((location / "format").string());
        store_format << indexer::STORE_FORMAT;
        store_format.close();

        io::stream<io::file_sink> store_info((location / "info").string());
        request.format().SerializeToOstream(&store_info);
        store_info.close();
    }

    void do_open_store(const indexer::StoreParameters& request) {
        fs::path location = request.location();

        io::stream<io::file_source> store_format((location / "format").string());
        int format;
        store_format >> format;
        store_format.close();
        if (format != indexer::STORE_FORMAT) {
            BOOST_THROW_EXCEPTION(common_exception()
                    << errinfo_rpc_code(::rpc_error::INVALID_STORE)
                    << errinfo_message(str(boost::format("Store at %s has invalid "
                                "format %d, expecting %d") 
                            % location % format % indexer::STORE_FORMAT)));
        }

        this->index.reset(new indexer::index(location / "index"));

        if (request.has_format()) {
            this->format = request.format();
        } else {
            io::stream<io::file_source> store_info((location / "info").string());
            this->format.ParseFromIstream(&store_info);
            store_info.close();
        }

        this->store_root = location;
    }

    fs::path store_root;
    indexer::IndexFormat format;
    std::unique_ptr<indexer::index> index;
};

namespace indexer {

IndexBuilder::IndexBuilder()
{
}

IndexBuilder::~IndexBuilder()
{
}

void IndexBuilder::createStore(const StoreParameters& request, rpcz::reply<Void> reply)
{
    implementation& impl = **this;
    try {
        std::cout << "Got request: '" << request.DebugString() << "'" << std::endl;

        impl.do_create_store(request);
        impl.do_open_store(request);

    } RPC_REPORT_EXCEPTIONS(reply)
    reply.send(Void());
}

void IndexBuilder::openStore(const StoreParameters& request, rpcz::reply<Void> reply)
{
    implementation& impl = **this;
    try {
        std::cout << "Got request: '" << request.DebugString() << "'" << std::endl;

        impl.do_open_store(request);

    } RPC_REPORT_EXCEPTIONS(reply)
    reply.send(Void());
}

void IndexBuilder::feedData(const BuilderData& request, rpcz::reply<Void> reply)
{
    implementation& impl = **this;
    try {
        if (!impl.index)
            BOOST_THROW_EXCEPTION(common_exception()
                << errinfo_rpc_code(::rpc_error::INVALID_STORE)
                << errinfo_message("Store index is not open"));
        std::cout << "Feeding " << request.records_size() << " records" << std::endl;
        for (IndexRecord const& rec : request.records()) {
            impl.index->insert(rec.key());
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

