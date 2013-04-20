#include "server.hpp"
#include <iostream>
#include <boost/filesystem.hpp>

namespace fs = boost::filesystem;

template <>
struct pimpl<indexserver::IndexBuilder>::implementation
{
    fs::path store_root;
};

namespace indexserver {

IndexBuilder::IndexBuilder()
{
}

IndexBuilder::~IndexBuilder()
{
}

void IndexBuilder::createStore(const CreateStore& request, rpcz::reply<Void> reply)
{
    implementation& impl = **this;
    std::cout << "Got request: '" << request.DebugString() << "'" << std::endl;
    impl.store_root = request.location();
    reply.send(Void());
}

void IndexBuilder::buildIndex(const Void& request, rpcz::reply<Void> reply)
{
    std::cout << "Got request: '" << request.DebugString() << "'" << std::endl;
    std::cout << "Building index!!!" << std::endl;
    reply.Error(rpcz::application_error::METHOD_NOT_IMPLEMENTED, "No such method yet");
    //reply.send(Void());
}

}

