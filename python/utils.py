from itertools import izip_longest


# Directly from the docs
def grouper(n, iterable, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    # grouper(3, 'ABCDEFG', 'x') --> ABC DEF Gxx
    args = [iter(iterable)] * n
    return izip_longest(fillvalue=fillvalue, *args)

def merge_sorted(lists):
    if not lists: return []
    return sorted(set.union(*map(set,lists)))
