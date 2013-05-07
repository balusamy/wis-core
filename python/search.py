#!/usr/bin/env python2

from __future__ import print_function
from __future__ import division

import argparse
from collections import defaultdict, Counter
import cPickle
from math import log
from pymongo import MongoClient
import rpcz
import time
import sys

import index_server_rpcz as index_rpcz
import index_server_pb2 as index_pb
from nlp import normalise_drop, normalise_gently
from utils import merge_sorted, tokens


class IndexServer(object):
    def __init__(self, server_endpoint, store_name):
        self.app = rpcz.Application()
        self.iserver = index_rpcz.IndexQueryService_Stub(
                    self.app.create_rpc_channel(server_endpoint))
        store = index_pb.UseStore()
        store.location = store_name
        self.iserver.useStore(store, deadline_ms=5)

    def query(self, query_word, max_mistakes=0, timeout=2, keys_only=False):
        query = index_pb.WordQuery()
        query.options.Clear()
        query.options.keysOnly = keys_only
        query.word = query_word
        query.maxCorrections = max_mistakes
        return self.iserver.wordQuery(query, deadline_ms=timeout)


class Searcher(object):
    def correct_token(self, token):
        index = self.index
        if len(token) >= 5:
            try:
                if index.query(token, max_mistakes=0, keys_only=True).exact_total == 0:
                    r = index.query(token, max_mistakes=1, keys_only=True)
                    if r.exact_total == 0:
                        r = index.query(token, max_mistakes=2, keys_only=True)
                    if 0 < r.exact_total <= 10:
                        new = map(lambda rec: rec.key, r.values)
                        self.corrected.append((token, new))
                        return new
            except rpcz.RpcDeadlineExceeded:
                self.correct_deadline = True
        return [token]


    def __init__(self, query, server='tcp://localhost:5555', store_path='enwiki'):
        k1 = 1.6
        b = 0.75

        self.timings = defaultdict(lambda: 0)
        self.corrected = []

        with open('mongo.cred', 'rt') as f:
            MONGO_HOST = f.readline().strip()
            MONGO_DB   = f.readline().strip()
            MONGO_USER = f.readline().strip()
            MONGO_PASS = f.readline().strip()
        MONGO_ADDRESS = 'mongodb://{user}:{password}@{host}/{db}'.format(user=MONGO_USER, password=MONGO_PASS, host=MONGO_HOST, db=MONGO_DB)
        self.mongo = MongoClient(MONGO_ADDRESS)
        self.db = self.mongo[MONGO_DB]

        index = self.index = IndexServer(server, store_path)


        query_tokens = map(self.correct_token, normalise_gently(tokens(query)))

        querysets = set([frozenset(normalise_drop(ts)) for ts in query_tokens])
        querysets = filter(lambda s: s, querysets)
        if not querysets: raise NotEnoughEntropy()

        kw_docsets = defaultdict(lambda: frozenset())
        doc_poslists = defaultdict(lambda: defaultdict(lambda: []))
        self.freq = freq = defaultdict(lambda: Counter())
        docs = None

        for queryset in querysets:
            matched_docs = set()

            for kw in queryset:
                self._TIME()
                res = index.query(kw, max_mistakes=0, timeout=5)
                if res.exact_total == 0:
                    try:
                        res = index.query(kw, max_mistakes=1, timeout=2)
                    except rpcz.RpcDeadlineExceeded:
                        self.extraquery_deadline = True
                self._TIME('index')

                for record in res.values:
                    key = record.key
                    if key in kw_docsets:
                        matched_docs |= kw_docsets[key]
                        continue
                    data = record.value.parts

                    docpostings = map(cPickle.loads, data)

                    key_set = set()
                    for (sha1, positions) in docpostings:
                        key_set.add(sha1)
                        matched_docs.add(sha1)
                        doc_poslists[sha1][key].append(positions)
                        freq[key][sha1] += len(positions)
                    kw_docsets[key] = frozenset(key_set)
            if docs is None:
                docs = matched_docs
            else:
                docs &= matched_docs
            if not docs:
                break
            self._TIME('proc')


        self._TIME()
        doc_count = Counter()
        doc_count.update({kw: len(freq[kw]) for kw in freq})

        N = self.N = self.db.articles.count()
        idf = {kw: max(0.4, log((N - doc_count[kw] + 0.5) / (doc_count[kw] + 0.5))) for kw in freq}

        self.poslists = {sha1: merge_sorted([l for klists in doc_poslists[sha1].values() for l in klists]) for sha1 in docs}
        self._TIME('proc')

        # Here comes BM25 to save the world!
        scores = []
        avg_size = self.db.service.find_one({'_id': 'avg_len'})['val']
        doc_headers = self.db.articles.find({'_id': {'$in': list(docs)}, 'size': {'$gt': 0}}, {'size':1, 'title':1})
        self._TIME('mongo')
        query_tokens = set([t for qs in query_tokens for t in qs])
        for d in doc_headers:
            score = 0

            sha1 = d['_id']
            size = d['size']
            title = d['title']

            for kw in freq:
                m = (freq[kw][sha1] / size  * (k1 + 1)) / (freq[kw][sha1] / size + k1 * (1 - b + b * size / avg_size))
                score += idf[kw] * m

            # Prioritise title matches (our own heuristic)
            keywords_bag = Counter(normalise_gently(query_tokens))
            title_tokens = normalise_gently(tokens(title))
            title_bag = Counter(title_tokens)
            both = keywords_bag & title_bag
            both_c = sum(both.values())
            ratio = both_c / (len(query_tokens) + len(title_tokens) - both_c)
            score += 10 * ratio

            tokens_title = normalise_drop(title_tokens)
            title_set = set(tokens_title)
            both = set(freq.keys()) & title_set
            ratio = len(both) / len(freq)
            score += 10 * ratio

            scores.append((sha1, score))
            self._TIME('ranking')

        self._TIME()
        self.scores = sorted(scores, key=lambda p: p[1], reverse=True)
        self._TIME('ranking')
        self.results = map(lambda p: p[0], self.scores)

    def show_document(self, sha1, hili=lambda w: '{' + w + '}'):
        NUM_BEFORE = 10
        NUM_AFTER = 15

        self._TIME()

        positions = self.poslists[sha1]
        doc = self.db.articles.find_one({'_id': sha1}, {'_id':0, 'title':1,
            'text':1, 'tokens':1, 'tokens_title':1})

        title_tokens = doc['tokens_title']

        events = []
        title_events = [(0, -1)]
        for pos in (p for p in positions if p >= 0):
            events.extend([(pos - NUM_BEFORE, -1), (pos, 0), (pos + NUM_AFTER + 1, 1)])
        events.sort()
        for pos in (-1 - p for p in positions if p < 0):
            title_events.append((pos, 0))
        title_events.append((len(title_tokens), 1))

        class Part(object):
            def __init__(self, start, tokens, text):
                self.start = start
                self.hili = set()
                self.end = None
                self.tokens = tokens
                self.text = text

            def str(self):
                result = []
                prev_to = None
                for i in xrange(self.start, self.end):
                    if i < 0 or i >= len(self.tokens):
                        continue
                    from_, to = self.tokens[i]
                    if prev_to:
                        result.append(self.text[prev_to:from_])
                    s = self.text[from_:to]
                    if i in self.hili:
                        result.append(hili(s))
                    else:
                        result.append(s)
                    prev_to = to
                return u''.join(result)

        def flatten_text(events, tokens, text):
            parts = []
            depth = 0
            for (p, e) in events:
                if e == -1:
                    if depth == 0:
                        parts.append(Part(p, tokens, text))
                    depth += 1
                elif e == 0:
                    parts[-1].hili.add(p)
                else:
                    depth -= 1
                    if depth == 0:
                        parts[-1].end = p
            return (p.str() for p in parts)

        title = next(flatten_text(title_events, title_tokens, doc['title']))
        text_parts = flatten_text(events, doc['tokens'], doc['text'])

        self._TIME('render')

        return {'title': doc['title'],
                'display_title': title,
                'parts': text_parts}


    def _TIME(self, label=None):
        if label:
            self.timings[label] += time.time() - self._timer
        self._timer = time.time()

class NotEnoughEntropy(ValueError):
    pass


def show_results(searcher, n=10, skip=0):
    for doc in searcher.results[skip:skip+n]:
        yield searcher.show_document(doc)

def show_titles(searcher, n=10, skip=0):
    for res in show_results(searcher, n, skip):
        yield res['title']


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Make an index query.')
    parser.add_argument('query', type=str,
                        help='the query word')
    parser.add_argument('--mistakes', '-k', type=int, default=0,
                        help='the number of allowed mistakes')
    parser.add_argument('--server', '-s', type=str, default="tcp://localhost:5555",
                        help='the index server to connect to')
    parser.add_argument('--index', '-i', type=str, default="idontcare",
                        help='the remote index store to open')
    parser.add_argument('--timeout', '-t', type=float, default=5.0,
                        help='the request timeout')
    parser.add_argument('--raw', action='store_true', default=False,
                        help='Bypass the Searcher')
    parser.add_argument('--keys-only', action='store_true', default=False,
                        help='Show only the index keys found, not postings')
    args = parser.parse_args()

    if not args.raw:
        s = Searcher(args.query, server=args.server, store_path=args.index,
                     max_mistakes=args.mistakes)
        for doc in show_results(s):
            text = '\n'.join((u"... {0} ...".format(p) for p in doc['parts']))
            print(u"Title: {0}\n{1}\n\n".format(doc['title'], text))
        sys.exit(0)

    index = IndexServer(args.server, args.index)
    result = index.query(unicode(args.query, "UTF-8"), args.mistakes, args.timeout,
            args.keys_only)
    if result.HasField('exact_total'):
        print("Total results: {0}".format(result.exact_total))
    for i, record in enumerate(result.values):
        if args.keys_only:
            print("Result #{0}: {1}".format(i, record.key.encode("UTF-8")))
        else:
            print("Result #{0}: {1} = {2}".format(i, record.key.encode("UTF-8"), record.value))
    print("Done")
