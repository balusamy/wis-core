#pragma once

#include <boost/dynamic_bitset.hpp>
#include <boost/array.hpp>
#include <boost/utility/string_ref.hpp>
#include <vector>

// Fast Damerau–Levenshtein (restricted) distance check
// algorithm based on work of Leonid Boitsov

struct fuzzy_processor {
    // TODO: use a custom pool allocator here
    typedef boost::dynamic_bitset<size_t> pattern_mask_t;
    typedef std::vector<pattern_mask_t> row_t;

    struct context {
        friend class fuzzy_processor;
        context(fuzzy_processor const& processor);
        context(context&&) = default;
        context(context const&) = default;
        ~context() = default;

    private:
        row_t R, Rp;
        size_t position;
        size_t cnt, cntp;
        pattern_mask_t const* SMapP;
    };

    fuzzy_processor(boost::string_ref const& pattern, size_t k, bool has_transpositions);

    size_t max_corrections() const
    { return k; }

    size_t pattern_size() const
    { return m; }

    bool has_transpositions() const
    { return has_transp; }

    bool check(boost::string_ref const& t, bool final, size_t* dist = nullptr, 
            context* ctx = nullptr) const;
    void feed(char c, context& ctx) const;

    void feed(boost::string_ref const& t, context& ctx) const {
        feed(t[ctx.position], ctx);
    }

    bool query(context& ctx, bool& is_final, size_t* dist = nullptr) const;

private:
    friend class context;

    bool do_feed(row_t& R1, size_t& cnt1, char c, context& ctx) const;

    row_t Ri;

    std::vector<pattern_mask_t> S;

    boost::array<pattern_mask_t::size_type, 
        1 << std::numeric_limits<unsigned char>::digits> Map;

    size_t k;
    size_t m;

    bool has_transp;
};

