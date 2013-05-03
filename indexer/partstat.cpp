#include <iostream>
#include <boost/filesystem.hpp>
#include <boost/program_options.hpp>
#include <boost/interprocess/managed_mapped_file.hpp>
#include <boost/format.hpp>
#include <boost/units/detail/utility.hpp>

namespace po = boost::program_options;
namespace fs = boost::filesystem;
namespace ipc = boost::interprocess;

std::string report_size(size_t size)
{
    return str(boost::format("%1% (%2% MB)")
            % size % (static_cast<double>(size) / (1 << 20)));
}

std::string simplify(const char* name)
{
    std::string s = boost::units::detail::demangle(name);
    boost::replace_all(s, "boost::interprocess::", "ipc::");
    return s;
}

int main(int argc, const char** argv)
{
    fs::path input_file;

    po::options_description desc("Options");
    desc.add_options()
        ("help", "produce this help message")
        ("input-file", po::value<fs::path>(&input_file)->required(), "file to examine")
        ("verbose,v", "be very noisy")
        ;

    po::positional_options_description pd;
    pd.add("input-file", 1);
    
    po::variables_map vm;
    po::store(po::command_line_parser(argc, argv)
            .options(desc)
            .positional(pd).run(), vm);
    
    if (vm.size() == 0 || vm.count("help")) {
        std::cout << "Boost.Interprocess managed_mapped_file viewer tool" << std::endl;
        std::cout << "Usage: partstat <file>" << std::endl;
        std::cout << desc << std::endl;
        return EXIT_SUCCESS;
    }

    po::notify(vm);

    if (!fs::exists(input_file)) {
        std::cerr << "Input file " << input_file << " does not exist" << std::endl;
        return EXIT_FAILURE;
    }

    ipc::managed_mapped_file file(ipc::open_only, input_file.string().c_str());

    std::cout << "Reporting info for " << input_file << ":" << std::endl;
    std::cout << "    File size      " << report_size(fs::file_size(input_file)) << std::endl;
    std::cout << "    Segment size   " << report_size(file.get_size()) << std::endl;
    std::cout << "    Free memory    " << report_size(file.get_free_memory()) << std::endl;
    std::cout << "    Consistent     " << (file.check_sanity() ? "true" : "false") << std::endl;
    std::cout << "    Named objects  " << file.get_num_named_objects() << std::endl;
    std::cout << "    Unique objects " << file.get_num_unique_objects() << std::endl;

    if (vm.count("verbose")) {
        typedef ipc::managed_mapped_file::const_named_iterator const_named_it;
        typedef ipc::managed_mapped_file::const_unique_iterator const_unique_it;

        std::cout << std::endl << "Named objects:" << std::endl;
        for (const_named_it it = file.named_begin(); it != file.named_end(); ++it) {
            std::cout << "    '" <<  it->name() << "'" << std::endl;
        }
        
        std::cout << std::endl << "Unique objects:" << std::endl;
        for (const_unique_it it = file.unique_begin(); it != file.unique_end(); ++it) {
            std::cout << "    '" <<  simplify(it->name()) << "'" << std::endl;
        }
    }

    return EXIT_SUCCESS;
}
