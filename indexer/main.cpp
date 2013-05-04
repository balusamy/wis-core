#include <rpcz/rpcz.hpp>
#include "store_manager.hpp"
#include "index_builder.hpp"
#include "index_search.hpp"
#include <boost/make_shared.hpp>

int main() {
    rpcz::application application;
    rpcz::server server(application);

    auto store_mgr = boost::make_shared<indexer::store_manager>();

    indexer::IndexBuilder index_builder_service(store_mgr);
    server.register_service(&index_builder_service);

    indexer::IndexSearch index_search_service(store_mgr);
    server.register_service(&index_search_service);

    std::cout << "Serving requests on port 5555." << std::endl;
    server.bind("tcp://*:5555");
    application.run();
}

