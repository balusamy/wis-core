#include "index.hpp"

#include <memory>
#include <boost/format.hpp>

#include "trie.hpp"
#include "exceptions.hpp"

namespace fs = boost::filesystem;

static const std::string EOS = "\xFF";

template <>
struct pimpl<indexer::index>::implementation
{
    implementation(fs::path const& path)
        : forward(path / "fwd"), reverse(path / "rev")
    {}

    trie forward;
    trie reverse;
};

namespace indexer {

index::index(fs::path const& path)
    : base(path)
{
}

void index::insert(boost::string_ref const& data)
{
    implementation& impl = **this;
    std::string s(data);
    s += EOS;
    impl.forward.insert(s);
    std::reverse(s.begin(), --s.end());
    impl.reverse.insert(s);
}

void index::search(boost::string_ref const& data, size_t k, results_t& results)
{
    implementation& impl = **this;
    if (k != 0) {
        BOOST_THROW_EXCEPTION(common_exception()
                << errinfo_rpc_code(::rpc_error::OPERATION_NOT_SUPPORTED)
                << errinfo_message(str(boost::format(
                            "Index search with k=%s is not implemented") % k)));
    } else {
        std::string word(data.begin(), data.end());
        word += EOS;
        impl.forward.search_exact(word, results);
    }
}

}
