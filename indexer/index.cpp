#include "index.hpp"

#include <memory>
#include <boost/format.hpp>
#include <boost/range/algorithm.hpp>
#include <boost/range/algorithm_ext.hpp>

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

void index::search(boost::string_ref const& data, size_t k, bool has_transp, results_t& results)
{
    implementation& impl = **this;
    if (k != 0) {
        size_t switch_len = data.size() / 2;
        size_t switch_len_1 = data.size() - switch_len;

        if (switch_len == 0) {
            impl.forward.search(data, k, has_transp, results);
            return;
        }

        results_t rev_results;
        std::string copy(data);

        if (has_transp) {
            std::swap(copy[switch_len - 1], copy[switch_len]);

            if (k == 1) {
                impl.forward.search_exact(copy, results);
            } else {
                size_t k1 = 0, k2 = k - 1;
                for (; k2 >= k1; ++k1, --k2) {
                    impl.forward.search_split(copy, switch_len, k1, true, k2, false,
                            has_transp, results);
                }
                boost::reverse(copy);
                for (; k2-- > 0; ++k1) {
                    impl.reverse.search_split(copy, switch_len_1, k2, false, k1, true,
                            has_transp, rev_results);
                }
                boost::reverse(copy);
            }

            std::swap(copy[switch_len - 1], copy[switch_len]);
        }

        size_t k1 = 0, k2 = k;
        for (; k2 >= k1; ++k1, --k2) {
            impl.forward.search_split(copy, switch_len, k1, true, k2, false,
                    has_transp, results);
        }
        boost::reverse(copy);
        for (; k2-- > 0; ++k1) {
            impl.reverse.search_split(copy, switch_len_1, k2, false, k1, true,
                    has_transp, rev_results);
        }
        boost::reverse(copy);

        for (auto& s : rev_results) {
            boost::reverse(s);
            results.push_back(s);
        }

        boost::erase(results, boost::unique<boost::return_found_end>(boost::sort(results)));
    } else {
        // TODO: handle EOS in the trie?
        std::string word(data.begin(), data.end());
        word += EOS;
        impl.forward.search_exact(word, results);
    }
}

}
