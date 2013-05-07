#include <rpcz/rpcz.hpp>
#include "store_manager.hpp"
#include "index_builder.hpp"
#include "index_search.hpp"
#include <boost/make_shared.hpp>
#include <boost/program_options.hpp>

namespace po = boost::program_options;

int main(int argc, const char** argv) {
    po::options_description desc("Options");
    desc.add_options()
        ("help", "produce this help message")
        ("mongodb-url,m", po::value<std::string>()->default_value("localhost"),
            "set MongoDB URL for the value_db")
        ("mongodb-db", po::value<std::string>()->default_value("index"), 
            "set MongoDB database name for the value_db")
        ;
    
    po::variables_map vm;
    po::store(po::command_line_parser(argc, argv)
            .options(desc).run(), vm);
    
    if (vm.count("help")) {
        std::cout << "Boost.Interprocess managed_mapped_file viewer tool" << std::endl;
        std::cout << "Usage: partstat <file>" << std::endl;
        std::cout << desc << std::endl;
        return EXIT_SUCCESS;
    }

    po::notify(vm);

    rpcz::application application;
    rpcz::server server(application);

    indexer::store_manager::options_t opts;
    opts.mongodb_url = vm["mongodb-url"].as<std::string>();
    opts.mongodb_name = vm["mongodb-db"].as<std::string>();
    auto store_mgr = boost::make_shared<indexer::store_manager>(opts);

    indexer::IndexBuilder index_builder_service(store_mgr);
    server.register_service(&index_builder_service);

    indexer::IndexSearch index_search_service(store_mgr);
    server.register_service(&index_search_service);

    std::cout << "Serving requests on port 5555." << std::endl;
    server.bind("tcp://*:5555");
    application.run();

    return EXIT_SUCCESS;
}

