#pragma once

#include <boost/utility/string_ref.hpp>
#include <boost/operators.hpp>
#include <boost/variant.hpp>
#include <boost/container/vector.hpp>
#include <boost/container/string.hpp>
#include <boost/interprocess/offset_ptr.hpp>
#include <boost/interprocess/managed_mapped_file.hpp>
#include <boost/interprocess/allocators/allocator.hpp>

namespace shared {

using boost::string_ref;

namespace cont = ::boost::container;
namespace ipc = ::boost::interprocess;

typedef ipc::managed_mapped_file::segment_manager segment_manager;
typedef ipc::managed_mapped_file::handle_t handle;
typedef cont::basic_string<char, std::char_traits<char>,
    ipc::allocator<char, segment_manager>> string;

template <typename T>
ipc::allocator<T, segment_manager> make_allocator(segment_manager* mgr)
{
    return ipc::allocator<T, segment_manager>(mgr);
}

struct external_ref
    : public boost::equality_comparable<external_ref>
{
    external_ref(uint32_t part_number, uint32_t offset)
        : part_number(part_number), offset(offset)
    {}

    bool operator == (external_ref const& other) const
    { 
        return part_number == other.part_number &&
            offset == other.offset;
    }

    uint32_t part_number;
    uint32_t offset;
};

struct trie_node
{
    trie_node(segment_manager* mgr)
        : children(make_allocator<child>(mgr))
    {}

    struct child
        : public boost::totally_ordered<child>
    {
        typedef boost::variant<ipc::offset_ptr<trie_node>,
            external_ref> ptr_t;

        child(string_ref const& label, segment_manager* mgr)
            : label(label.begin(), label.end(), make_allocator<char>(mgr)) {}

        child(string_ref const& label, ptr_t const& ptr, segment_manager* mgr)
            : label(label.begin(), label.end(), make_allocator<char>(mgr)), ptr(ptr) {}

        string label;
        ptr_t ptr;

        bool operator < (child const& other) const
        { return label < other.label; }

        bool operator == (child const& other) const
        { return label == other.label; }
    };
    cont::vector<child, ipc::allocator<child, 
        ipc::managed_mapped_file::segment_manager>> children;
};

struct part_root
{
    part_root()
    {}

    uint32_t nodes_count;
};

}
