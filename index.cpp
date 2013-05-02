#include "index.hpp"

#include <memory>

#include "trie.hpp"

namespace fs = boost::filesystem;

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
    impl.forward.insert(s);
    std::reverse(s.begin(), s.end());
    impl.reverse.insert(s);
}

}
