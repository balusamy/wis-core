#include "trie.hpp"

#include <memory>

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
#include <boost/interprocess/allocators/allocator.hpp>
#include <boost/interprocess/allocators/node_allocator.hpp>
#include <boost/interprocess/allocators/cached_node_allocator.hpp>
#include <boost/unordered_map.hpp>
#include <boost/format.hpp>

#include "trie_layout.hpp"

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

template <>
string_ref as_ref<shared::string>(shared::string const& s)
{
    return string_ref(s.begin().get(), s.size());
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


        bool should_init = !fs::exists(part_);

        file_ = boost::in_place(ipc::open_or_create, part_.string().c_str(), policy_->initial_size());
        allocator_ = boost::in_place(file_->get_segment_manager());
        deleter_ = boost::in_place(&*allocator_);
        root_ = file_->find_or_construct<shared::part_root>(ipc::unique_instance)();

        if (should_init) {
            // Horrible hack to keep allocator alive indefinitely
            allocator_->get_node_pool()->inc_ref_count();
        }
    }

    void close()
    {
        deleter_ = boost::none;
        allocator_ = boost::none;
        file_ = boost::none;
    }

    shared::segment_manager* segment_manager() const
    {
        return file_->get_segment_manager();
    }

    shared::trie_root* create_root()
    {
        if (!can_allocate_more()) {
            std::cout << "Part " << part_ << ": !can_allocate_more for root" << std::endl;
            return nullptr;
        }
        uint32_t root_id = root_->next_root_id++;
        char buf[9]; // Should be safe for uint32_t
        ::snprintf(buf, sizeof(buf), "%X", root_id);
        return file_->find_or_construct<shared::trie_root>(buf, std::nothrow)(root_id, segment_manager());
    }

    shared::trie_root* get_root(uint32_t root_id)
    {
        char buf[9]; // Should be safe for uint32_t
        ::snprintf(buf, sizeof(buf), "%X", root_id);
        return file_->find<shared::trie_root>(buf).first;
    }

    typedef ipc::cached_node_allocator<shared::trie_node,
        ipc::managed_mapped_file::segment_manager> node_allocator_t;
    typedef stateful_deleter<shared::trie_node,
        node_allocator_t> node_deleter_t;
    typedef ipc::unique_ptr<shared::trie_node, node_deleter_t> node_ptr_t;

    node_ptr_t create_node()
    {
        if (!can_allocate_more()) {
            return node_ptr_t(nullptr, *deleter_);
        }
        try {
            return node_ptr_t(
                    new (&*allocator_->allocate_one()) 
                        shared::trie_node(allocator_->get_segment_manager()),
                    *deleter_);
        } catch (ipc::bad_alloc const&) {
            std::cout << "Part " << part_ << ": ipc::bad_alloc" << std::endl;
            return node_ptr_t(nullptr, *deleter_);
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

    shared::part_root* root_;

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
    trie_node_ref create_node(trie_part* source)
    {
        trie_part::node_ptr_t node = source->create_node();
        if (!node) {
            shared::trie_root* root = nullptr;
            trie_part* from = nullptr;
            while (!root) {
                from = load_part(this->current_part, true);
                root = from->create_root();
                if (!root) {
                    ++this->current_part;
                    std::cout << "Switching current part to " << this->current_part << std::endl;
                }
            }
            shared::external_ref ref(this->current_part, root->root_id);
            return trie_node_ref(from, &root->root_node, ref);
        }
        auto ptr = node.get();
        return trie_node_ref(source, ptr.get(), node.release());
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

            trie_node_ref operator()(shared::external_ref const& ref) const
            {
                return impl_.resolve_external_ref(ref);
            }

        private:
            trie_part* source_;
            implementation& impl_;
        };
        return boost::apply_visitor(resolve_visitor(*this, source), p).with_ptr(p);
    }

    trie_node_ref resolve_external_ref(shared::external_ref const& ref)
    {
        return resolve_external_ref(ref.part_number, ref.root_id);
    }

    trie_node_ref resolve_external_ref(size_t part_number, uint32_t root_id)
    {
        trie_part* part = load_part(part_number);
        shared::trie_root* root = part->get_root(root_id);
        if (!root)
            throw std::logic_error(str(boost::format("No root '%X' found in part %d")
                        % root_id % part_number));
        return trie_node_ref(part, &root->root_node);
    }

    std::pair<shared::trie_node::child*, size_t> find_longest_match(shared::trie_node* node, string_ref const& s)
    {
        size_t maxlen = 0;
        shared::trie_node::child* match = nullptr;
#if 1
        for (shared::trie_node::child& child : node->children) {
            size_t len = common_prefix_length(child.label, s);
            if (len > maxlen) {
                maxlen = len;
                match = &child;
                assert(match->label.size() >= maxlen);
            }
        }
#else
        auto it = std::upper_bound(children.begin(), children.end(), s,
                [](string_ref const& a, shared::trie_node::child const& b) {
                    return boost::range::lexicographical_compare(a, b.label);
                });
#endif
        return std::make_pair(match, maxlen);
    }

    void do_insert(trie_node_ref const& ref, string_ref const& full_str, size_t start_pos)
    {
        string_ref s = full_str.substr(start_pos);
        string_ref prefix = full_str.substr(0, start_pos);
        assert(ref.node() != nullptr);
        assert(ref.part() != nullptr);
        // Find the child with the longest matching prefix
        size_t maxlen;
        shared::trie_node::child* match;
        std::tie(match, maxlen) = find_longest_match(ref.node(), s);
        assert(!match || match->label.size() >= maxlen);

        if (maxlen == 0) {
            auto& children = ref.node()->children;
            auto it = std::upper_bound(children.begin(), children.end(), s,
                    [](string_ref const& a, shared::trie_node::child const& b) {
                        return boost::range::lexicographical_compare(a, b.label);
                    });

            children.insert(it, shared::trie_node::child(s, ref.part()->segment_manager()));
            return;
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
            return;
        }
        // Otherwise, split the node
        string_ref matchRest = as_ref(match->label).substr(maxlen);
        trie_node_ref new_ref = create_node(ref.part());
        assert(!(new_ref.ptr() == trie_node_ref::ptr_t()));
        // FIXME: this is broken, because match->ptr may be external to new_ref.node()
        new_ref.node()->children.push_back(shared::trie_node::child(matchRest, match->ptr, new_ref.part()->segment_manager()));
        new_ref.node()->children.push_back(shared::trie_node::child(rest, new_ref.part()->segment_manager()));
        boost::sort(new_ref.node()->children);
        match->label.erase(maxlen);
        match->ptr = new_ref.ptr();
    }

    void do_search_exact(trie_node_ref const& ref, string_ref const& full_str, size_t start_pos, trie::results_t& results)
    {
        string_ref s = full_str.substr(start_pos);
        string_ref prefix = full_str.substr(0, start_pos);
        if (s.empty()) {
            // EOS hack :(
            results.push_back(std::string(full_str.data(), full_str.size() - 1));
            return;
        }

        auto const& children = ref.node()->children;
        if (children.empty()) {
            return;
        }

        // Find the child with the longest matching prefix
        size_t maxlen;
        shared::trie_node::child* match;
        std::tie(match, maxlen) = find_longest_match(ref.node(), s);

        if (maxlen == 0 || maxlen < match->label.size()) {
            return;
        }

        string_ref rest = s.substr(maxlen);

        if (rest.empty()) {
            // EOS hack :(
            results.push_back(std::string(full_str.data(), full_str.size() - 1));
            return;
        } else {
            auto child_ref = resolve_node(match->ptr, ref.part());
            if (child_ref.node() != nullptr) {
                do_search_exact(child_ref, full_str, start_pos + maxlen, results);
            }
        }
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
        , part_grow_policy(new limited_grow_policy(1U << 28, 0.9, 2., 1U << 28)) // 256 MB starting size, 256 MB limit
    {
        fs::create_directories(part_dir);
        // TODO: implement real initialization step
        auto part = load_part(0, true);
        if (part->get_root(0) == nullptr) {
            part->create_root();
        }
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
    auto root = impl.resolve_external_ref(0, 0);
    impl.do_insert(root, data, 0);
}

void trie::search_exact(boost::string_ref const& data, results_t& results)
{
    implementation& impl = **this;
    auto root = impl.resolve_external_ref(0, 0);
    impl.do_search_exact(root, data, 0, results);
}
