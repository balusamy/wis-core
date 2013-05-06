#!/usr/bin/env python2

from __future__ import print_function

import argparse
from collections import defaultdict, Counter
import cPickle
from math import log
from pymongo import MongoClient
import rpcz
import time

import index_server_rpcz as index_rpcz
import index_server_pb2 as index_pb
from nlp import normalise_drop
from utils import merge_sorted


class IndexServer(object):
    def __init__(self, server_endpoint, store_name):
        self.app = rpcz.Application()
        self.iserver = index_rpcz.IndexQueryService_Stub(
                    self.app.create_rpc_channel(server_endpoint))
        store = index_pb.UseStore()
        store.location = store_name
        self.iserver.useStore(store, deadline_ms=5)

    def query(self, query_word, max_mistakes=0, timeout=5):
        query = index_pb.WordQuery()
        query.options.Clear()
        query.word = query_word
        query.maxCorrections = max_mistakes
        return self.iserver.wordQuery(query, deadline_ms=timeout)


class Searcher(object):
    def __init__(self, keywords):
        k1 = 1.6
        b = 0.75

        self.timings = defaultdict(lambda: 0)

        with open('mongo.cred', 'rt') as f:
            MONGO_HOST = f.readline().strip()
            MONGO_DB   = f.readline().strip()
            MONGO_USER = f.readline().strip()
            MONGO_PASS = f.readline().strip()
        MONGO_ADDRESS = 'mongodb://{user}:{password}@{host}/{db}'.format(user=MONGO_USER, password=MONGO_PASS, host=MONGO_HOST, db=MONGO_DB)
        self.mongo = MongoClient(MONGO_ADDRESS)
        self.db = self.mongo[MONGO_DB]


        keywords = set(normalise_drop(keywords))
        index = IndexServer('tcp://localhost:5555', 'enwiki')

        matched_docsets = []
        doc_poslists = defaultdict(lambda: [])
        freq = defaultdict(lambda: Counter())

        for kw in keywords:
            self._TIME()
            res = index.query(kw, max_mistakes=0).values
            self._TIME('index')

            if not res:
                data = []
            else:
                data = res[0].value.parts

            docpostings = map(cPickle.loads, data)

            matched_docs = set()
            for (sha1, positions) in docpostings:
                matched_docs.add(sha1)
                doc_poslists[sha1].append(positions)
                freq[kw][sha1] += len(positions)
            matched_docsets.append(matched_docs)
            self._TIME('proc')


        self._TIME()
        doc_count = {kw: len(freq[kw]) for kw in freq}

        N = self.N = self.db.articles.count()
        idf = {kw: log((N - doc_count[kw] + 0.5) / (doc_count[kw] + 0.5)) for kw in keywords}

        docs = set.intersection(*matched_docsets) if matched_docsets else set()
        self.poslists = {sha1: merge_sorted(doc_poslists[sha1]) for sha1 in docs}
        self._TIME('proc')

        # Here comes BM52 to save the world!
        scores = []
        avg_size = self.db.service.find_one({'_id': 'avg_len'})['val']
        doc_sizes = self.db.articles.find({'_id': {'$in': list(docs)}, 'size': {'$gt': 0}}, {'size':1})
        self._TIME('mongo')
        for d in doc_sizes:
            score = 0

            sha1 = d['_id']
            size = d['size']

            for kw in keywords:
                m = (freq[kw][sha1] * (k1 + 1)) / (freq[kw][sha1] + k1 * (1 - b + b * size / avg_size))
                score += idf[kw] * m
            scores.append((sha1, score))
            self._TIME('ranking')

        self._TIME()
        self.scores = sorted(scores, key=lambda p: p[1], reverse=True)
        self._TIME('ranking')
        for sha1, s in self.scores:
            doc = self.db.articles.find_one({'_id': sha1}, {'_id':0, 'title':1})
            t = doc['title']
            print((t, s, sha1))


    def show_documents(self, n=10, hili=lambda w:w):
        if not self.scores: return None

        NUM_BEFORE = 5
        NUM_AFTER = 5

        self._TIME()

        result = []
        for sha1, score in self.scores[:n]:
            positions = self.poslists[sha1]
            doc = self.db.articles.find_one({'_id': sha1}, {'_id':0, 'title':1, 'url':1, 'text':1})
            doc = self.fetched[sha1]

            tokens = doc['text']
            parts = []
            for pos in positions:
                ws = tokens[pos-NUM_BEFORE : pos] + [hili(tokens[pos])] + tokens[pos+1 : pos+NUM_AFTER]
                parts.append(' '.join(ws))

            result.append({'title': doc['title'], 'url': '#', 'parts': parts})
        self._TIME('render')

        return result

    def _TIME(self, label=None):
        if label:
            self.timings[label] += time.time() - self._timer
        self._timer = time.time()



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
    args = parser.parse_args()

    index = IndexServer(args.server, args.index)
    result = index.query(unicode(args.query, "UTF-8"), args.mistakes, args.timeout)
    if result.HasField('exact_total'):
        print("Total results: {0}".format(result.exact_total))
    for i, record in enumerate(result.values):
        print("Result #{0}: {1} = {2}".format(i, record.key.encode("UTF-8"), record.value))
    print("Done")
