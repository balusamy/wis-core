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
#include <boost/interprocess/smart_ptr/unique_ptr.hpp>
#include <boost/interprocess/smart_ptr/deleter.hpp>
#include <boost/interprocess/managed_mapped_file.hpp>
#include <boost/interprocess/allocators/node_allocator.hpp>
#include <boost/interprocess/allocators/cached_node_allocator.hpp>
#include <boost/unordered_map.hpp>
#include <boost/format.hpp>

namespace fs = ::boost::filesystem;
namespace cont = ::boost::container;
namespace ipc = ::boost::interprocess;

using boost::string_ref;

template <typename RangeT>
string_ref as_ref(RangeT const& range)
{
    return string_ref(::boost::begin(range), ::boost::size(range));
}

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
    external_ref(string_ref const& prefix)
        : prefix(prefix.begin(), prefix.end())
    {}

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
    virtual ~grow_policy() {}

    virtual size_t initial_size() const = 0;
    virtual bool should_grow(size_t current_size, size_t free) const = 0;
    virtual boost::optional<size_t> grow(size_t current_size) const = 0;
};

struct exponential_grow_policy
    : grow_policy
{
    exponential_grow_policy(size_t initial_size, double free_factor, double grow_factor)
        : initial_size_(initial_size), free_factor_(free_factor), grow_factor_(grow_factor)
    {}

    size_t initial_size() const
    { return initial_size_; }

    bool should_grow(size_t current_size, size_t free) const
    {
        return 1. * free / current_size < free_factor_;
    }

    boost::optional<size_t> grow(size_t current_size) const
    {
        return static_cast<size_t>(grow_factor_ * current_size);
    }

    double grow_factor() const
    {
        return grow_factor_;
    }

private:
    size_t initial_size_;
    double free_factor_;
    double grow_factor_;
};

struct limited_grow_policy
    : exponential_grow_policy
{
    limited_grow_policy(size_t initial_size, double free_factor, double grow_factor, size_t limit)
        : exponential_grow_policy(initial_size, free_factor, grow_factor), limit_(limit)
    {}

    boost::optional<size_t> grow(size_t current_size) const 
    {
        size_t new_size = static_cast<size_t>(grow_factor() * current_size);
        if (new_size > limit_)
            return boost::none;
        return new_size;
    }

private:
    size_t limit_;
};

template<class T, class Allocator>
class stateful_deleter
{
public:
   typedef typename boost::intrusive::
      pointer_traits<typename Allocator::void_pointer>::template
         rebind_pointer<T>::type                pointer;

private:
   typedef typename boost::intrusive::
      pointer_traits<pointer>::template
         rebind_pointer<Allocator>::type    allocator_pointer;

   allocator_pointer allocator_;

public:
    stateful_deleter(allocator_pointer allocator)
        : allocator_(allocator)
    {}

    void operator()(const pointer &p)
    {
        allocator_->deallocate_one(ipc::ipcdetail::to_raw_pointer(p));
    }
};

struct trie_part
{
    trie_part(fs::path const& part, grow_policy* policy)
        : part_(part), policy_(policy)
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

        file_ = boost::in_place(ipc::open_or_create, part_.string().c_str(), policy_->initial_size());
        allocator_ = boost::in_place(file_->get_segment_manager());
        deleter_ = boost::in_place(&*allocator_);
        ref_allocator_ = boost::in_place(file_->get_segment_manager());
        ref_deleter_ = boost::in_place(&*ref_allocator_);
    }

    void close()
    {
        deleter_ = boost::none;
        allocator_ = boost::none;
        ref_deleter_ = boost::none;
        ref_allocator_ = boost::none;
        file_ = boost::none;
    }

    shared::trie_root* create_root(string_ref const& prefix)
    {
        return file_->find_or_construct<shared::trie_root>(prefix.data(), std::nothrow)(prefix);
    }

    shared::trie_root* get_root(string_ref const& prefix)
    {
        return file_->find<shared::trie_root>(prefix.data()).first;
    }

    typedef ipc::cached_node_allocator<shared::trie_node,
        ipc::managed_mapped_file::segment_manager> node_allocator_t;
    typedef stateful_deleter<shared::trie_node,
        node_allocator_t> node_deleter_t;
    typedef ipc::unique_ptr<shared::trie_node, node_deleter_t> node_ptr_t;

    typedef ipc::node_allocator<shared::external_ref,
        ipc::managed_mapped_file::segment_manager> ref_allocator_t;
    typedef stateful_deleter<shared::external_ref,
        ref_allocator_t> ref_deleter_t;
    typedef ipc::unique_ptr<shared::external_ref, ref_deleter_t> ref_ptr_t;

    node_ptr_t create_node()
    {
        if (!can_allocate_more())
            return node_ptr_t(nullptr, *deleter_);
        try {
            return node_ptr_t(
                    new (&*allocator_->allocate_one()) shared::trie_node(),
                    *deleter_);
        } catch (ipc::bad_alloc const&) {
            return node_ptr_t(nullptr, *deleter_);
        }
    }

    ref_ptr_t create_external_ref(string_ref const& prefix)
    {
        if (!can_allocate_more())
            return ref_ptr_t(nullptr, *ref_deleter_);
        try {
            return ref_ptr_t(
                    new (&*ref_allocator_->allocate_one()) shared::external_ref(prefix),
                    *ref_deleter_);
        } catch (ipc::bad_alloc const&) {
            return ref_ptr_t(nullptr, *ref_deleter_);
        }
    }

private:
    bool can_allocate_more()
    {
        // TODO: support growing
        if (policy_->should_grow(file_->get_size(), file_->get_free_memory())) {
            auto new_size = policy_->grow(file_->get_size());
            if (new_size)
                throw std::logic_error("Part growing is not supported");
            // New nodes should be placed elsewhere
            return false;
        }
        return true;
    }

    fs::path part_;
    grow_policy* policy_;

    bool needs_grow_;

    boost::optional<node_allocator_t> allocator_;
    boost::optional<node_deleter_t> deleter_;

    boost::optional<ref_allocator_t> ref_allocator_;
    boost::optional<ref_deleter_t> ref_deleter_;

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
    trie_node_ref create_node(trie_part* source, string_ref const& prefix)
    {
        trie_part::node_ptr_t node = source->create_node();
        while (!node) {
            trie_part::ref_ptr_t ref = source->create_external_ref(prefix);
            if (!ref) {
                // Houston, we have a problem
                // Should not happen, really, but hard to ensure
                // Recovery is not really possible
                throw std::logic_error(str(
                            boost::format("Cannot allocate an external ref for prefix %s") % prefix));
            }
            trie_part* from = load_part(this->current_part, true);
            shared::trie_root* root = from->create_root(prefix);
            if (!root) {
                ++this->current_part;
                continue;
            }
            ref->part_number = this->current_part;
            return trie_node_ref(from, &root->root_node, ref.release());
        }
        auto ptr = node.get();
        return trie_node_ref(source, ptr.get(), ptr);
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
        trie_part* part = load_part(ref.part_number);
        shared::trie_root* root = part->get_root(as_ref(ref.prefix));
        if (!root)
            throw std::logic_error(str(boost::format("No root '%s' found in part %d")
                        % ref.prefix % ref.part_number));
        return trie_node_ref(part, &root->root_node);
    }

    void do_insert(trie_node_ref const& ref, string_ref const& full_str, size_t start_pos)
    {
        string_ref s = full_str.substr(start_pos);
        string_ref prefix = full_str.substr(0, start_pos);
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
                do_insert(resolve_node(match->ptr, ref.part()), full_str, start_pos + maxlen);
            }
        }
        // Otherwise, split the node
        string_ref matchRest = string_ref(match->label.begin(), match->label.size()).substr(maxlen);
        trie_node_ref new_ref = create_node(ref.part(), prefix);
        assert(!(new_ref.ptr() == trie_node_ref::ptr_t()));
        new_ref.node()->children.push_back(shared::trie_node::child(matchRest, match->ptr));
        new_ref.node()->children.push_back(shared::trie_node::child(rest));
        boost::sort(new_ref.node()->children);
        match->label.erase(maxlen);
        match->ptr = new_ref.ptr();
    }

    trie_part* load_part(size_t idx, bool create_if_missing = false)
    {
        if (parts.count(idx) == 0) {
            std::string name = str(boost::format("%04u") % idx);
            if (!create_if_missing && !fs::exists(part_dir / name))
                throw std::logic_error("Part " + name + " not found in " + part_dir.string());
            std::unique_ptr<trie_part> part(new trie_part(part_dir / name, part_grow_policy.get()));
            parts[idx] = std::move(part);
        }
        return parts[idx].get();
    }

    implementation(fs::path const& part_dir)
        : part_dir(part_dir)
        , part_grow_policy(new limited_grow_policy(1U << 28, 0.8, 2., 1U << 28)) // 256 MB starting size, 256 MB limit
    {
        fs::create_directories(part_dir);
        // Will create it if missing
        load_part(0, true)->create_root("");
    }

    fs::path part_dir;
    std::unique_ptr<grow_policy> part_grow_policy;
    size_t current_part;
    boost::unordered_map<size_t, std::unique_ptr<trie_part>> parts;
};

trie::trie(fs::path const& path, bool read_only)
    : base(path)
{
}

void trie::insert(boost::string_ref const& data)
{
    implementation& impl = **this;
    auto root = impl.resolve_external_ref(shared::external_ref(0, ""));
    impl.do_insert(root, data, 1);
}
