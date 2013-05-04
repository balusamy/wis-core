#include <iostream>
#include <boost/filesystem.hpp>
#include <boost/program_options.hpp>
#include <boost/interprocess/managed_mapped_file.hpp>
#include <boost/format.hpp>
#include <boost/units/detail/utility.hpp>
#include <boost/unordered_map.hpp>

#include "trie_layout.hpp"

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

namespace shared {

std::ostream& operator<<(std::ostream& s, shared::external_ref const& ref)
{
    s << ref.part_number << ":" << ref.offset;
    return s;
}

}

void print_trie(const void* base, shared::trie_node* node, size_t level, size_t maxlevel)
{
    if (level > maxlevel)
        return;
    std::string indent(level, ' ');
    size_t offset = (const char*)node - (const char*)base;
    std::cout << indent << "Node address=" << offset << " level=" << level << "\n";
    for (int i = 0; i < node->children.size(); ++i) {
        auto const& ptr = node->children[i].ptr;
        std::cout << indent << " Key '" << node->children[i].label << "'";
        if (ptr.which() == 0) {
            std::cout << ":" << std::endl;
            shared::trie_node* child = boost::get<ipc::offset_ptr<shared::trie_node>>(ptr).get();
            if (child) {
                print_trie(base, child, level + 1, maxlevel);
            }
        } else {
            std::cout << " = " << ptr << std::endl;
        }
    }
}

struct trie_stats
{
    trie_stats()
        : node_count(0)
        , max_level(0)
        , external_refs_count(0)
        , total_label_size(0)
        , leaves(0)
    {}

    size_t node_count;
    size_t max_level;
    size_t external_refs_count;
    size_t total_label_size;
    size_t leaves;
    boost::unordered_map<size_t, size_t> refs_by_part;
};

void collect_trie_stats(shared::trie_node* node, int level, trie_stats& stats) {
    ++stats.node_count;
    stats.max_level = std::max<size_t>(stats.max_level, level);
    for (int i = 0; i < node->children.size(); ++i) {
        auto const& ptr = node->children[i].ptr;
        stats.total_label_size += node->children[i].label.size();
        if (ptr.which() == 0) {
            shared::trie_node* child = boost::get<ipc::offset_ptr<shared::trie_node>>(ptr).get();
            if (child) {
                collect_trie_stats(child, level + 1, stats);
            } else {
                ++stats.leaves;
            }
        } else {
            auto ref = boost::get<shared::external_ref const&>(ptr);
            ++stats.external_refs_count;
            ++stats.refs_by_part[ref.part_number];
        }
    }

}

int main(int argc, const char** argv)
{
    fs::path input_file;

    po::options_description desc("Options");
    desc.add_options()
        ("help", "produce this help message")
        ("input-file", po::value<fs::path>(&input_file)->required(), "file to examine")
        ("verbose,v", "be very noisy")
        ("root,r", po::value<size_t>(), "show specific root")
        ("level,l", po::value<size_t>()->default_value(SIZE_MAX), "only show levels below it")
        ("stats,s", "collect node stats")
        ("test", "do not use")
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

    if (vm.count("root")) {
        size_t offset = vm["root"].as<size_t>();
        shared::trie_node* node = reinterpret_cast<shared::trie_node*>((char*)file.get_address() + offset);
        if (!file.belongs_to_segment(node)) {
            std::cerr << "Node @" << offset << " not found in part " << input_file << std::endl;
            return EXIT_FAILURE;
        }
        std::cout << "Reporting info for " << input_file << " @" << offset << ":" << std::endl;
        if (vm.count("stats")) {
            trie_stats stats;
            collect_trie_stats(node, 1, stats);
            std::cout << "    Subtree size   " << stats.node_count << std::endl;
            std::cout << "    Subtree depth  " << stats.max_level << std::endl;
            std::cout << "    Leaves count   " << stats.leaves << std::endl;
            std::cout << "    External refs  " << stats.external_refs_count << std::endl;
            std::cout << "    Total key data " << stats.total_label_size << std::endl;
            std::cout << "    Refs by part   ";
            for (std::pair<const size_t, size_t> const& p : stats.refs_by_part) {
                std::cout << p.first << "=" << p.second << " ";
            }
            std::cout << std::endl;
        } else {
            std::cout << "    Segment start  " << boost::format("%p") % file.get_address() << std::endl;
            std::cout << "    Segment end    " << boost::format("%p") % (void*)((char*)file.get_address() + file.get_size()) << std::endl;
            print_trie(file.get_address(), node, 1, vm["level"].as<size_t>());
        }
        return EXIT_SUCCESS;
    }

    std::cout << "Reporting info for " << input_file << ":" << std::endl;
    std::cout << "    File size      " << report_size(fs::file_size(input_file)) << std::endl;
    std::cout << "    Segment size   " << report_size(file.get_size()) << std::endl;
    std::cout << "    Free memory    " << report_size(file.get_free_memory()) << std::endl;
    //std::cout << "    Consistent     " << (file.check_sanity() ? "true" : "false") << std::endl;
    std::cout << "    Named objects  " << file.get_num_named_objects() << std::endl;
    std::cout << "    Unique objects " << file.get_num_unique_objects() << std::endl;

    shared::part_root* part_root = file.find<shared::part_root>(ipc::unique_instance).first;
    if (part_root) {
        std::cout << "Part-specific info:" << std::endl;
        std::cout << "    Nodes count    " << part_root->nodes_count << std::endl;
    }

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
