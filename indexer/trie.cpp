#include "trie.hpp"

#include <memory>
#include <iostream>

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
#include <boost/interprocess/allocators/cached_adaptive_pool.hpp>
#include <boost/interprocess/allocators/adaptive_pool.hpp>
#include <boost/unordered_map.hpp>
#include <boost/format.hpp>
#include <boost/filesystem/fstream.hpp>

#include "trie_layout.hpp"
#include "fuzzy_processor.hpp"

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
        p->~T();
        allocator_->deallocate_one(ipc::ipcdetail::to_raw_pointer(p));
    }
};

struct trie_part
{
    trie_part(fs::path const& part, size_t part_number, grow_policy* policy)
        : part_(part), part_number_(part_number), policy_(policy)
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

    size_t number() const
    {
        return part_number_;
    }

    typedef ipc::cached_adaptive_pool<shared::trie_node,
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
            ++root_->nodes_count;
            node_ptr_t result(
                    new (&*allocator_->allocate_one())
                        shared::trie_node(allocator_->get_segment_manager()),
                    *deleter_);
            // HACK: preallocate some space in all nodes to reduce reallocations
            result->children.reserve(10);
            return result;
        } catch (ipc::bad_alloc const&) {
            --root_->nodes_count;
            std::cout << "Part " << part_ << ": ipc::bad_alloc" << std::endl;
            return node_ptr_t(nullptr, *deleter_);
        }
    }

    void delete_node(shared::trie_node* node)
    {
        --root_->nodes_count;
        node->~trie_node();
        allocator_->deallocate_one(node);
    }

    shared::trie_node* get_node(uint32_t offset)
    {
        return static_cast<shared::trie_node*>(file_->get_address_from_handle(offset));
    }

    template <typename T>
    uint32_t stable_offset(T* p)
    {
        assert(p != nullptr);
        uint32_t result = file_->get_handle_from_address(p);
        assert(file_->get_address_from_handle(result) == p);
        assert(result < file_->get_size());
        return result;
    }

    template <typename T>
    uint32_t stable_offset(ipc::offset_ptr<T> const& p)
    {
        return stable_offset(p.get());
    }

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

private:
    fs::path part_;
    size_t part_number_;
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
            trie_part* from = nullptr;
            while (!node) {
                from = load_part(this->current_part, true);
                node = from->create_node();
                if (!node) {
                    ++this->current_part;
                    std::cout << "Switching current part to " << this->current_part << std::endl;
                }
            }
            shared::external_ref ref(this->current_part, from->stable_offset(node.get()));
            return trie_node_ref(from, node.release().get(), ref);
        }
        auto ptr = node.get();
        return trie_node_ref(source, ptr.get(), node.release());
    }

    trie_node_ref::ptr_t normalize_ptr(trie_node_ref::ptr_t const& p, trie_part* source, trie_part* dest)
    {
        if (source == dest)
            return p;
        struct normalize_visitor
            : public boost::static_visitor<trie_node_ref::ptr_t>
        {
            normalize_visitor(implementation& impl, trie_part* source, trie_part* dest)
                : impl_(impl), source_(source), dest_(dest) {}

            trie_node_ref::ptr_t operator()(ipc::offset_ptr<shared::trie_node> const& p) const
            {
                if (!p)
                    return p;
                return shared::external_ref(source_->number(), source_->stable_offset(p));
            }

            trie_node_ref::ptr_t operator()(shared::external_ref const& ref) const
            {
                if (ref.part_number != dest_->number())
                    return ref;
                return ipc::offset_ptr<shared::trie_node>(dest_->get_node(ref.offset));
            }

        private:
            implementation& impl_;
            trie_part* source_;
            trie_part* dest_;
        };
        return boost::apply_visitor(normalize_visitor(*this, source, dest), p);
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
        return resolve_external_ref(ref.part_number, ref.offset);
    }

    trie_node_ref resolve_external_ref(size_t part_number, uint32_t offset)
    {
        trie_part* part = load_part(part_number);
        shared::trie_node* node = part->get_node(offset);
        if (!node)
            throw std::logic_error(str(boost::format("No node @%X found in part %d")
                        % offset % part_number));
        return trie_node_ref(part, node);
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

    boost::optional<trie_node_ref> do_insert(trie_node_ref const& ref, string_ref const& full_str, size_t start_pos)
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
            if (!ref.part()->can_allocate_more()) {
                trie_node_ref new_ref = create_node(ref.part());
                assert(!(new_ref.ptr() == trie_node_ref::ptr_t()));
                assert(new_ref.part() != ref.part());

                auto& children = new_ref.node()->children;
                // Copy everything explicitly, with right allocators
                for (shared::trie_node::child const& child : ref.node()->children) {
                    auto ptr = normalize_ptr(child.ptr, ref.part(), new_ref.part());
                    children.push_back(shared::trie_node::child(as_ref(child.label), ptr, 
                                new_ref.part()->segment_manager()));
                }
                // TODO: simplify/optimize
                auto it = std::upper_bound(children.begin(), children.end(), s,
                        [](string_ref const& a, shared::trie_node::child const& b) {
                            return boost::range::lexicographical_compare(a, b.label);
                        });
                children.insert(it, shared::trie_node::child(s, new_ref.part()->segment_manager()));

                ref.part()->delete_node(ref.node());
                return new_ref;
            } else {
                auto& children = ref.node()->children;
                auto it = std::upper_bound(children.begin(), children.end(), s,
                        [](string_ref const& a, shared::trie_node::child const& b) {
                            return boost::range::lexicographical_compare(a, b.label);
                        });

                children.insert(it, shared::trie_node::child(s, ref.part()->segment_manager()));
                return boost::none;
            }
        }

        string_ref rest = s.substr(maxlen);
        if (rest.empty()) {
            // String already in trie, do nothing
            return boost::none;
        }
        // If the string to be inserted has match->label as its prefix
        // insert it into the corresponding subtree
        if (maxlen == match->label.size()) {
            if (match->ptr == shared::trie_node::child::ptr_t())
                throw std::logic_error(std::string("String '") + s.data() + "' has a proper prefix in the trie");
            else {
                auto new_ref = do_insert(resolve_node(match->ptr, ref.part()), full_str, start_pos + maxlen);
                if (new_ref) {
                    match->ptr = new_ref->ptr();
                }
            }
            return boost::none;
        }
        // Otherwise, split the node
        string_ref matchRest = as_ref(match->label).substr(maxlen);
        trie_node_ref new_ref = create_node(ref.part());
        assert(!(new_ref.ptr() == trie_node_ref::ptr_t()));

        auto match_ptr = normalize_ptr(match->ptr, ref.part(), new_ref.part());
        new_ref.node()->children.push_back(shared::trie_node::child(matchRest, match_ptr, new_ref.part()->segment_manager()));
        new_ref.node()->children.push_back(shared::trie_node::child(rest, new_ref.part()->segment_manager()));
        boost::sort(new_ref.node()->children);

        match->label.erase(maxlen);
        match->ptr = new_ref.ptr();
        return boost::none;
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

    void do_search(trie_node_ref const& ref, std::string& scrap, fuzzy_processor const& proc, 
            fuzzy_processor::context const& ctx, bool exact_dist, trie::results_t& results,
            size_t skip_prefix = 0)
    {
        for (shared::trie_node::child const& child : ref.node()->children) {
            fuzzy_processor::context new_ctx(ctx);
            scrap.append(child.label.begin(), child.label.end());

            bool is_leaf = child.ptr == trie_node_ref::ptr_t();

            size_t dist;
            string_ref s(scrap);
            s.remove_prefix(skip_prefix);
            if (proc.check(s, is_leaf, &dist, &new_ctx)) {
                if (!is_leaf) {
                    auto child_ref = resolve_node(child.ptr, ref.part());
                    do_search(child_ref, scrap, proc, new_ctx, exact_dist, results,
                            skip_prefix);
                } else if (!exact_dist || dist == proc.max_corrections()) {
                    append_result(results, scrap);
                }
            }

            scrap.resize(scrap.size() - child.label.size());
        }
    }

    void do_search_semiexact(trie_node_ref const& ref, std::string& scrap,
            string_ref const& str, size_t switch_len,
            fuzzy_processor const& proc, fuzzy_processor::context const& ctx, bool exact_dist,
            trie::results_t& results)
    {
        for (shared::trie_node::child const& child : ref.node()->children) {
            scrap.append(child.label.begin(), child.label.end());
            size_t step = child.label.size();

            bool is_leaf = child.ptr == trie_node_ref::ptr_t();

            size_t prefix = switch_len + step - scrap.size();
            if (scrap.size() < switch_len) {
                if (!is_leaf && str.substr(0, step) == as_ref(child.label)) {
                    auto child_ref = resolve_node(child.ptr, ref.part());
                    do_search_semiexact(child_ref, scrap, str.substr(step), switch_len,
                            proc, ctx, exact_dist, results);
                }
            } else if (str.substr(0, prefix) == as_ref(child.label).substr(0, prefix)) {
                fuzzy_processor::context new_ctx(ctx);
                if (scrap.size() > switch_len) {
                    size_t dist;
                    if (proc.check(as_ref(child.label).substr(prefix), is_leaf,
                                &dist, &new_ctx)) {
                        if (!is_leaf) {
                            auto child_ref = resolve_node(child.ptr, ref.part());
                            do_search(child_ref, scrap, proc, new_ctx, exact_dist, results,
                                    switch_len);
                        } else if (!exact_dist || dist == proc.max_corrections()) {
                            append_result(results, scrap);
                        }
                    }
                } else if (!is_leaf) {
                    auto child_ref = resolve_node(child.ptr, ref.part());
                    do_search(child_ref, scrap, proc, new_ctx, exact_dist, results,
                            switch_len);
                }
            }

            scrap.resize(scrap.size() - child.label.size());
        }
    }

    void do_search2(trie_node_ref const& ref, std::string& scrap, size_t switch_len,
            fuzzy_processor const& proc1, fuzzy_processor::context const& ctx1,
            bool exact_dist1, fuzzy_processor const& proc2,
            fuzzy_processor::context const& ctx2, bool exact_dist2,
            trie::results_t& results)
    {
        for (shared::trie_node::child const& child : ref.node()->children) {
            fuzzy_processor::context new_ctx1(ctx1);
            size_t start_pos = scrap.size();
            scrap.append(child.label.begin(), child.label.end());

            bool is_leaf = child.ptr == trie_node_ref::ptr_t();

            const size_t gap = proc1.max_corrections();
            for (; start_pos < scrap.size(); ++start_pos) {
                if (start_pos + 1 > switch_len + gap) {
                    break;
                }
                proc1.feed(scrap, new_ctx1);
                bool final_state = false;
                if (!proc1.query(new_ctx1, final_state)) {
                    break;
                }
                if (start_pos + 1 < switch_len - gap) {
                    continue;
                }
                if (final_state) {
                    fuzzy_processor::context new_ctx2(ctx2);
                    bool ok2 = true, final2 = false;
                    size_t dist2;
                    for (size_t k = start_pos + 1; k < scrap.size(); ++k) {
                        proc2.feed(scrap[k], new_ctx2);
                        if (!proc2.query(new_ctx2, final2, &dist2)) {
                            ok2 = false;
                            break;
                        }
                    }
                    if (start_pos + 1 == scrap.size() || ok2) {
                        if (!is_leaf) {
                            auto child_ref = resolve_node(child.ptr, ref.part());
                            do_search(child_ref, scrap, proc2, new_ctx2,
                                    exact_dist2, results);
                        } else if (final2 && 
                                (!exact_dist2 || dist2 == proc2.max_corrections())) {
                            append_result(results, scrap);
                        }
                    }
                }
            }

            if (!is_leaf && start_pos == scrap.size()) {
                auto child_ref = resolve_node(child.ptr, ref.part());
                do_search2(child_ref, scrap, switch_len,
                        proc1, new_ctx1, exact_dist1, proc2, ctx2, exact_dist2, results);
            }

            scrap.resize(scrap.size() - child.label.size());
        }
    }

    trie_part* load_part(size_t idx, bool create_if_missing = false)
    {
        if (parts.count(idx) == 0) {
            std::string name = str(boost::format("%04u") % idx);
            if (!create_if_missing && !fs::exists(part_dir / name))
                throw std::logic_error("Part " + name + " not found in " + part_dir.string());
            std::unique_ptr<trie_part> part(new trie_part(part_dir / name, idx, part_grow_policy.get()));
            parts[idx] = std::move(part);
        }
        return parts[idx].get();
    }

    shared::external_ref load_ref(fs::path const& path)
    {
        fs::ifstream file(path);
        shared::external_ref result(0, 0);
        char delimiter;
        file >> result.part_number >> delimiter >> result.offset;
        if (!file.good())
            throw std::logic_error("Cannot read ref " + path.string());
        return result;
    }

    void save_ref(fs::path const& path, shared::external_ref const& ref)
    {
        fs::ofstream file(path);
        file << ref.part_number << ":" << ref.offset << std::endl;
        file.close();
        if (!file.good())
            throw std::logic_error("Cannot write ref " + path.string());
    }

    void append_result(trie::results_t& results, boost::string_ref const& s)
    {
        // EOS hack :(
        results.push_back(std::string(s.data(), s.size() - 1));
    }

    std::string append_eos(boost::string_ref const& s)
    {
        std::string str;
        str.reserve(s.size() + EOS.size());
        str.append(s.begin(), s.end());
        str += EOS;
        return str;
    }

    implementation(fs::path const& part_dir)
        : part_dir(part_dir)
        , part_grow_policy(new limited_grow_policy(1U << 28, 0.04, 2., 1U << 28)) // 256 MB starting size, 256 MB limit
        , current_part(0)
        , head(0, 0)
    {
        fs::create_directories(part_dir);
        // TODO: implement real initialization step
        fs::path head_path = part_dir / "HEAD";
        if (!fs::exists(head_path)) {
            auto part = load_part(0, true);
            auto node = part->create_node();
            head = shared::external_ref(0, part->stable_offset(node.release()));
            save_ref(head_path, head);
        } else {
            head = load_ref(head_path);
        }
    }

    static const std::string EOS;

    fs::path part_dir;
    std::unique_ptr<grow_policy> part_grow_policy;
    size_t current_part;
    shared::external_ref head;
    boost::unordered_map<size_t, std::unique_ptr<trie_part>> parts;
};

// 0xFF is chosen because will never be in a valid UTF-8 string
const std::string pimpl<trie>::implementation::EOS = "\xFF";

trie::trie(fs::path const& path, bool read_only)
    : base(path)
{
}

void trie::insert(boost::string_ref const& data)
{
    implementation& impl = **this;
    auto root = impl.resolve_external_ref(impl.head);
    auto new_head = impl.do_insert(root, data, 0);
    if (new_head) {
        auto part = new_head->part();
        impl.head = shared::external_ref(part->number(),
                part->stable_offset(new_head->node()));
        std::cout << "Moving HEAD to " << impl.head.part_number << ":" 
            << impl.head.offset << std::endl;
        impl.save_ref(impl.part_dir / "HEAD", impl.head);
    }
}

void trie::search_exact(boost::string_ref const& data, results_t& results)
{
    implementation& impl = **this;
    auto root = impl.resolve_external_ref(impl.head);
    impl.do_search_exact(root, data, 0, results);
}

void trie::search(boost::string_ref const& data, size_t k, bool has_transp, results_t& results)
{
    implementation& impl = **this;
    if (k == 0) {
        search_exact(data, results);
        return;
    }

    std::string pattern = impl.append_eos(data);
    fuzzy_processor proc(pattern, k, has_transp);
    fuzzy_processor::context ctx(proc);

    auto root = impl.resolve_external_ref(impl.head);
    std::string scrap;
    impl.do_search(root, scrap, proc, ctx, false, results);
}


void trie::search_split(boost::string_ref const& data, size_t switch_len,
        size_t k1, bool exact_dist1, size_t k2, bool exact_dist2,
        bool has_transp, results_t& results)
{
    implementation& impl = **this;
    if (k1 == 0 && k2 == 0) {
        search_exact(data, results);
        return;
    }
    if (switch_len == 0) {
        search(data, k1 + k2, has_transp, results);
        return;
    }

    std::string pattern = impl.append_eos(data);
    string_ref s1 = string_ref(pattern).substr(0, switch_len);
    string_ref s2 = string_ref(pattern).substr(switch_len);

    fuzzy_processor proc1(s1, k1, has_transp);
    fuzzy_processor proc2(s2, k2, has_transp);
    fuzzy_processor::context ctx1(proc1);
    fuzzy_processor::context ctx2(proc2);

    auto root = impl.resolve_external_ref(impl.head);
    std::string scrap;
    if (k1 != 0) {
        // It is possible that s1 matches an empty string.
        if (s1.size() == k1 || (!exact_dist1 && s1.size() < k1)) {
            impl.do_search(root, scrap, proc2, ctx2, exact_dist2, results);
        }
        impl.do_search2(root, scrap, switch_len, proc1, ctx1, exact_dist1,
                proc2, ctx2, exact_dist2, results);
    } else {
        impl.do_search_semiexact(root, scrap, pattern, switch_len,
                proc2, ctx2, exact_dist2, results);
    }
}
