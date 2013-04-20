#include "server.hpp"
#include <iostream>

using namespace std;

namespace indexserver {

void IndexBuilder::createStore(const CreateStore& request, rpcz::reply<Void> reply)
{
    cout << "Got request for '" << request.location() << "'" << endl;
    reply.send(Void());
}

void IndexBuilder::buildIndex(const Void& request, rpcz::reply<Void> reply)
{
    cout << "Building index!!!" << endl;
    reply.Error(rpcz::application_error::METHOD_NOT_IMPLEMENTED, "No such method yet");
    //reply.send(Void());
}

}

