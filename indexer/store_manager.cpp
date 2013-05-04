#include "store_manager.hpp"
#include <iostream>
#include <boost/filesystem.hpp>
#include <boost/filesystem/fstream.hpp>
#include <boost/format.hpp>
#include <boost/iostreams/device/file.hpp>
#include <boost/iostreams/stream.hpp>
#include <boost/unordered_map.hpp>
#include <boost/weak_ptr.hpp>
#include <boost/make_shared.hpp>

#include "exceptions.hpp"

namespace fs = boost::filesystem;
namespace io = boost::iostreams;

namespace rpc_error {
}

namespace indexer {

static const int STORE_FORMAT = 1;

}

template <>
struct pimpl<indexer::store>::implementation
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

    void do_open_store(fs::path const& location) {
        if (!fs::exists(location / "format")) {
            BOOST_THROW_EXCEPTION(common_exception()
                    << errinfo_rpc_code(::rpc_error::STORE_NOT_FOUND)
                    << errinfo_message(str(boost::format("Store at %s does not exist") 
                            % location)));
        }

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

        io::stream<io::file_source> store_info((location / "info").string());
        this->format.ParseFromIstream(&store_info);
        store_info.close();

        this->store_root = location;
    }

    void do_close_store() {
    }

    fs::path store_root;
    indexer::IndexFormat format;
    boost::shared_ptr<indexer::index> index;
};

template <>
struct pimpl<indexer::store_manager>::implementation
{
    boost::unordered_map<fs::path, boost::weak_ptr<indexer::store>> stores;
};

namespace indexer {

store::store(const StoreParameters& parameters)
{
    (*this)->do_create_store(parameters);
    (*this)->do_open_store(fs::path(parameters.location()));
}

store::store(fs::path const& location)
{
    (*this)->do_open_store(location);
}

store::~store()
{
    (*this)->do_close_store();
}

fs::path store::location() const
{
    return (*this)->store_root;
}

boost::shared_ptr<index> store::index() const
{
    return (*this)->index;
}

store_manager::store_manager()
{
}

store_manager::~store_manager()
{
}

store_manager::store_ptr store_manager::create(const StoreParameters& parameters)
{
    implementation& impl = **this;
    auto it = impl.stores.find(parameters.location());
    if (it != impl.stores.end()) {
        auto store = it->second.lock();
        if (store) {
            return store;
        }
    }
    auto result = boost::make_shared<store>(parameters);
    impl.stores[parameters.location()] = result;
    return result;
}

store_manager::store_ptr store_manager::open(fs::path const& location)
{
    implementation& impl = **this;
    auto it = impl.stores.find(location);
    if (it != impl.stores.end()) {
        auto store = it->second.lock();
        if (store) {
            return store;
        }
    }
    auto result = boost::make_shared<store>(location);
    impl.stores[location] = result;
    return result;
}

}

