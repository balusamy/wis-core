#include "fuzzy_processor.hpp"

#include <boost/optional.hpp>
#include <limits>

fuzzy_processor::fuzzy_processor(boost::string_ref const& pattern, size_t k,
        bool has_transpositions)
    : Map({}), k(k), m(pattern.size()), has_transp(has_transpositions)
{
    assert(!pattern.empty());
    S.assign(m + 1, pattern_mask_t(m));
    for (size_t i = 0, cnt = 0; i < m; ++i)
    {
        size_t& idx = Map[static_cast<unsigned char>(pattern[i])];
        if (!idx) {
            idx = ++cnt;
        }
        S[idx][i] = 1;
    }

    Ri.assign(k + 1, pattern_mask_t(m));

    for (size_t i = 0; i <= k; ++i)
        for (size_t j = 0; j < i; ++j)
            Ri[i][j] = 1;
}

fuzzy_processor::context::context(fuzzy_processor const& processor)
{
    cnt = cntp = position = 0;
    R = processor.Ri;
    SMapP = &processor.S[0];
}

bool fuzzy_processor::check(boost::string_ref const& t, bool final, size_t* dist, 
        context* ctx) const
{
    row_t R1;
    R1.assign(k + 1, pattern_mask_t(m));
    size_t cnt1;

    boost::optional<context> new_ctx;
    if (ctx == nullptr)
        new_ctx = boost::in_place(*this);
    context& ctx_ref = ctx == nullptr ? *new_ctx : *ctx;

    size_t& j = ctx_ref.position;
    for (; j < t.size(); ++j) {
        do_feed(R1, cnt1, t[j], ctx_ref);
    }

    if (final) {
        if (!ctx_ref.R[k].test(m - 1))
            return false;
        if (dist) {
            size_t x;
            for (x = k; ctx_ref.R[x].test(m - 1) && x --> 0;);
            *dist = x + 1;
        }
        return true;
    } else {
        if (has_transp)
            return ctx_ref.cnt <= k || ctx_ref.cntp < k;
        else
            return ctx_ref.cnt <= k;
    }
}

void fuzzy_processor::feed(char c, context& ctx) const
{
    row_t R1;
    R1.assign(k + 1, pattern_mask_t(m));
    size_t cnt1;

    do_feed(R1, cnt1, c, ctx);
    ++ctx.position;
}

bool fuzzy_processor::query(context& ctx, bool& is_final, size_t* dist) const
{
    if (ctx.R[k].test(m - 1)) {
        if (dist) {
            size_t x;
            for (x = k; ctx.R[x].test(m - 1) && x --> 0;);
            *dist = x + 1;
        }
        is_final = true;
    }
    if (has_transp)
        return ctx.cnt <= k || ctx.cntp < k;
    else
        return ctx.cnt <= k;
}

bool fuzzy_processor::do_feed(row_t& R1, size_t& cnt1, char c, context& ctx) const
{
    cnt1 = ctx.cnt;
    pattern_mask_t const& SMap = this->S[Map[static_cast<unsigned char>(c)]];
    const size_t j = ctx.position;

    // Allocation here
    pattern_mask_t temp(m);

    for (size_t d = ctx.cnt; d <= k; ++d) {
        // R1[d] = (((R[d] << 1) | (j <= d)) & SMap);
        temp = ctx.R[d];
        temp <<= 1;
        if (j <= d)
            temp.set(0);
        temp &= SMap;
        R1[d] = temp;
        // end
        if (d > ctx.cnt) {
            // R1[d] |= ((R[d - 1] | R1[d - 1]) << 1) | R[d - 1] | (j <= d - 1);
            temp = ctx.R[d - 1];
            temp |= R1[d - 1];
            temp <<= 1;
            temp |= ctx.R[d - 1];
            if (j <= d - 1)
                temp.set(0);
            R1[d] |= temp;
            // end
        }
    }
    if (has_transp && j > 0) {
        size_t d0 = std::max<size_t>(ctx.cntp + 1, 1);
        for (size_t d = d0; d <= k; ++d) {
            // R1[d] |= (RP[d - 1] << 2 | (Uint(j <= d) << 1)) & SMapP & (SMap << 1);
            temp = ctx.Rp[d - 1];
            temp <<= 2;
            if (j <= d && m > 1)
                temp.set(1);
            temp &= *ctx.SMapP;
            // Alloc here :(
            temp &= SMap << 1;
            R1[d] |= temp;
            // end
        }
    }
    for (size_t d = ctx.cnt; d <= k; ++d) {
        if (d == cnt1 && j >= d && R1[d].none()) {
            cnt1 = d + 1;
        }
    }

    if (has_transp) {
        ctx.Rp = ctx.R;
    }
    ctx.R = R1;
    ctx.SMapP = &SMap;

    ctx.cntp = ctx.cnt;
    ctx.cnt = cnt1;
}
