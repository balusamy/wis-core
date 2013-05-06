from itertools import izip_longest

from nlp import itokenise


# Directly from the docs
def grouper(n, iterable, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    # grouper(3, 'ABCDEFG', 'x') --> ABC DEF Gxx
    args = [iter(iterable)] * n
    return izip_longest(fillvalue=fillvalue, *args)

def merge_sorted(lists):
    if not lists: return []
    return sorted(set.union(*map(set,lists)))

def tokens(string, ilist=None):
    if ilist is None: string, ilist = itokenise(string)
    return [string[f:t] for f, t in ilist]
