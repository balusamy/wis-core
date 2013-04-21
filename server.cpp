#include "server.hpp"
#include <iostream>
#include <boost/filesystem.hpp>
#include <boost/filesystem/fstream.hpp>
#include <boost/format.hpp>
#include <boost/iostreams/device/file.hpp>
#include <boost/iostreams/stream.hpp>
#include <leveldb/db.h>

#include "exceptions.hpp"
#include "stagedb.hpp"

namespace fs = boost::filesystem;
namespace io = boost::iostreams;

template <>
struct pimpl<indexserver::IndexBuilder>::implementation
{
    fs::path store_root;
    indexserver::IndexFormat format;
    size_t dimensions;
    std::unique_ptr<stage_db> db;
};

namespace rpc_error {
static const int STORE_ALREADY_EXISTS = 1;
static const int INVALID_STORE = 2;
}

namespace indexserver {

static const int STORE_FORMAT = 1;

IndexBuilder::IndexBuilder()
{
}

IndexBuilder::~IndexBuilder()
{
}

void IndexBuilder::createStore(const CreateStore& request, rpcz::reply<Void> reply)
{
    implementation& impl = **this;
    try {
        std::cout << "Got request: '" << request.DebugString() << "'" << std::endl;
        fs::path location = request.location();
        if (fs::is_directory(location)) {
            if (!request.overwrite()) {
                reply.Error(::rpc_error::STORE_ALREADY_EXISTS,
                        str(boost::format("Store at %s already exists") % location));
                return;
            } else {
                std::cout << "Recreating store at " << location << std::endl;
                if (impl.db)
                    impl.db.reset();
                fs::remove_all(location);
            }
        } else {
            std::cout << "Creating store at " << location << std::endl;
        }
        fs::create_directories(location);

        io::stream<io::file_sink> store_format((location / "format").string());
        store_format << STORE_FORMAT;
        store_format.close();

        fs::ofstream store_info((location / "info").string());
        request.format().SerializeToOstream(&store_info);
        store_info.close();

        impl.db.reset(new stage_db(location / "staging"));

        impl.store_root = location;
        impl.format = request.format();
        impl.dimensions = impl.format.dimension_size();
    } RPC_REPORT_EXCEPTIONS(reply)
    reply.send(Void());
}

void IndexBuilder::feedData(const BuilderData& request, rpcz::reply<Void> reply)
{
    implementation& impl = **this;
    try {
        if (!impl.db)
            BOOST_THROW_EXCEPTION(common_exception()
                << errinfo_rpc_code(::rpc_error::INVALID_STORE)
                << errinfo_message("Store staging db is not open"));
        std::cout << "Feeding " << request.records_size() << " records" << std::endl;
        impl.db->append(request);
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

