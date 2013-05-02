#include "trie.hpp"

#include <memory>

#include <boost/operators.hpp>
#include <boost/variant.hpp>
#include <boost/optional.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/range/algorithm.hpp>
#include <boost/container/vector.hpp>
#include <boost/container/string.hpp>
#include <boost/interprocess/offset_ptr.hpp>
#include <boost/interprocess/managed_mapped_file.hpp>
#include <boost/unordered_map.hpp>
#include <boost/format.hpp>

namespace fs = ::boost::filesystem;
namespace cont = ::boost::container;
namespace ipc = ::boost::interprocess;

using boost::string_ref;

template<typename Range1T, typename Range2T, typename PredicateT>
    inline boost::iterator_range<typename boost::range_const_iterator<Range1T>::type> common_prefix(
    const Range1T& Input,
    const Range2T& Test,
    PredicateT Comp)
{
    typedef typename
        ::boost::range_const_iterator<Range1T>::type Iterator1T;
    typedef typename
        ::boost::range_const_iterator<Range2T>::type Iterator2T;

    ::boost::iterator_range<Iterator1T> lit_input(::boost::as_literal(Input));
    ::boost::iterator_range<Iterator2T> lit_test(::boost::as_literal(Test));

    Iterator1T InputEnd=::boost::end(lit_input);
    Iterator2T TestEnd=::boost::end(lit_test);

    Iterator1T it=::boost::begin(lit_input);
    Iterator2T pit=::boost::begin(lit_test);
    for(;
        it!=InputEnd && pit!=TestEnd;
        ++it,++pit)
    {
        if(!(Comp(*it,*pit)))
            break;
    }
    return ::boost::iterator_range<Iterator1T>(::boost::begin(lit_input), it);
}

template<typename Range1T, typename Range2T>
    inline boost::iterator_range<typename boost::range_const_iterator<Range1T>::type> common_prefix(
    const Range1T& Input,
    const Range2T& Test)
{
    return common_prefix(Input, Test, boost::is_equal());
}

template<typename Range1T, typename Range2T>
    inline size_t common_prefix_length(
    const Range1T& Input,
    const Range2T& Test)
{
    return boost::size(common_prefix(Input, Test));
}

namespace shared {

struct external_ref
{
    external_ref(size_t part_number, string_ref const& prefix)
        : part_number(part_number), prefix(prefix.begin(), prefix.end())
    {}

    size_t part_number;
    cont::string prefix;
};

struct trie_node
{
    struct child
        : public boost::totally_ordered<child>
    {
        typedef boost::variant<ipc::offset_ptr<trie_node>,
            ipc::offset_ptr<external_ref>> ptr_t;

        child(string_ref const& label)
            : label(label.begin(), label.end()) {}

        child(string_ref const& label, ptr_t const& ptr)
            : label(label.begin(), label.end()), ptr(ptr) {}

        cont::string label;
        ptr_t ptr;

        bool operator < (child const& other) const
        { return label < other.label; }

        bool operator == (child const& other) const
        { return label == other.label; }
    };
    cont::vector<child> children;
};

struct trie_root
{
    trie_root(string_ref const& prefix)
        : prefix(prefix.begin(), prefix.end()) {}

    trie_node root_node;
    cont::string prefix;
};

}

struct trie_part;

struct grow_policy
{
    virtual size_t initial_size() const = 0;
    virtual boost::optional<size_t> grow(size_t current_size) const = 0;
};

struct trie_part
{
    trie_part(fs::path const& part, grow_policy* allocator)
        : part_(part), allocator_(allocator)
    {
        reopen();
    }

    ~trie_part()
    {
        close();
    }

    void reopen()
    {
        close();

        file_ = boost::in_place(ipc::open_or_create, part_.string().c_str(), allocator_->initial_size());
    }

    void close()
    {
        if (is_dirty_)
            persist();
        else if (file_)
            file_ = boost::none;
    }

    void persist()
    {
        /*if (!fs::exists(part_))
            fs::create_directories(part_.parent_path());
        std::string pattern = part_.filename().string() + ".%%%%-%%%%-%%%%-%%%%";
        auto temp_part = fs::unique_path(pattern);
        io::file_descriptor_sink temp_file(temp_part.string(), std::ios::out | std::ios::binary);
        capnp::writeMessageToFd(temp_file.handle(), *message_);
        temp_file.close();
        if (file_.is_open())
            file_.close();
        fs::rename(temp_part, part_);*/
        is_dirty_ = false;
    }

    shared::trie_root* get_root(string_ref const& prefix)
    {
        return file_->find_or_construct<shared::trie_root>(prefix.data())(prefix);
    }

private:
    fs::path part_;
    grow_policy* allocator_;

    bool is_dirty_;
    bool needs_grow_;

    boost::optional<ipc::managed_mapped_file> file_;
};

struct trie_node_ref
{
    typedef shared::trie_node::child::ptr_t ptr_t;

    trie_node_ref(trie_part* part, shared::trie_node* node)
        : part_(part), node_(node)
    {}

    trie_node_ref(trie_part* part, shared::trie_node* node, ptr_t const& ptr)
        : part_(part), node_(node), ptr_(ptr)
    {}

    trie_part* part() const
    { return part_; }

    shared::trie_node* node() const
    { return node_; }

    ptr_t ptr() const
    { return ptr_; }

    trie_node_ref with_ptr(ptr_t const& ptr) const
    {
        trie_node_ref result(*this);
        result.ptr_ = ptr;
        return result;
    }

private:
    trie_part* part_;
    shared::trie_node* node_;
    ptr_t ptr_;
};

template <>
struct pimpl<trie>::implementation
{
    trie_node_ref allocate_node(trie_part* source)
    {
        //TODO: implement
        return trie_node_ref(nullptr, nullptr);
    }

    trie_node_ref resolve_node(shared::trie_node::child::ptr_t const& p, trie_part* source)
    {
        struct resolve_visitor
            : public boost::static_visitor<trie_node_ref>
        {
            resolve_visitor(implementation& impl, trie_part* source) 
                : impl_(impl), source_(source) {}

            trie_node_ref operator()(ipc::offset_ptr<shared::trie_node> const& p) const
            {
                return trie_node_ref(source_, p.get());
            }

            trie_node_ref operator()(ipc::offset_ptr<shared::external_ref> const& ref) const
            {
                return impl_.resolve_external_ref(*ref);
            }

        private:
            trie_part* source_;
            implementation& impl_;
        };
        return boost::apply_visitor(resolve_visitor(*this, source), p).with_ptr(p);
    }

    trie_node_ref resolve_external_ref(shared::external_ref const& ref)
    {
        // TODO: implement
        return trie_node_ref(nullptr, nullptr);
    }

    void do_insert(trie_node_ref const& ref, string_ref const& s)
    {
        assert(ref.node() != nullptr);
        assert(ref.part() != nullptr);
        // Find the child with the longest matching prefix
        size_t maxlen = 0;
        shared::trie_node::child* match;
        for (shared::trie_node::child& child : ref.node()->children) {
            size_t len = common_prefix_length(child.label, s);
            if (len > maxlen) {
                maxlen = len;
                match = &child;
            }
        }

        if (maxlen == 0) {
            auto children = ref.node()->children;
            auto it = std::upper_bound(children.begin(), children.end(), s,
                    [](string_ref const& a, shared::trie_node::child const& b) {
                        return boost::range::lexicographical_compare(a, b.label);
                    });

            children.insert(it, shared::trie_node::child(s));
        }

        string_ref rest = s.substr(maxlen);
        if (rest.empty()) {
            // String already in trie, do nothing
            return;
        }
        // If the string to be inserted has label as its prefix
        // insert it into the corresponding subtree
        if (maxlen == match->label.size()) {
            if (match->ptr == shared::trie_node::child::ptr_t())
                throw std::logic_error(std::string("String '") + s.data() + "' has a proper prefix in the trie");
            else {
                do_insert(resolve_node(match->ptr, ref.part()), rest);
            }
        }
        // Otherwise, split the node
        string_ref matchRest = string_ref(match->label.begin(), match->label.size()).substr(maxlen);
        trie_node_ref new_ref = allocate_node(ref.part());
        assert(!(new_ref.ptr() == trie_node_ref::ptr_t()));
        new_ref.node()->children.push_back(shared::trie_node::child(matchRest, match->ptr));
        new_ref.node()->children.push_back(shared::trie_node::child(rest));
        boost::sort(new_ref.node()->children);
        match->label.erase(maxlen);
        match->ptr = new_ref.ptr();
    }

    void load_part(size_t idx)
    {
        if (parts.count(idx) != 0)
            return;
        std::string name = str(boost::format("%04u") % idx);
        std::unique_ptr<trie_part> part(new trie_part(part_dir / name, nullptr));
        parts[idx] = std::move(part);
    }

    implementation(fs::path const& part_dir)
        : part_dir(part_dir)
        , root(nullptr, nullptr)
    {
        // Will create it if missing
        load_part(0);

        root = resolve_external_ref(shared::external_ref(0, ""));
    }

    fs::path part_dir;
    trie_node_ref root;
    boost::unordered_map<size_t, std::unique_ptr<trie_part>> parts;
};

trie::trie(fs::path const& path, bool read_only)
    : base(path)
{
}

void trie::insert(boost::string_ref const& data)
{
    (*this)->do_insert((*this)->root, data);
}
