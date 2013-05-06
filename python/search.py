#!/usr/bin/env python2

from __future__ import print_function

import argparse
from collections import namedtuple, OrderedDict
import cPickle
from nlp import stem
from pymongo import MongoClient
import rpcz
from time import time

import index_server_rpcz as index_rpcz
import index_server_pb2 as index_pb


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


DocPostings = namedtuple('DocPostings', ['postings', 'num_keywords',
                                         'num_positions',
                                         'query_time', 'proc_time'])

def search(keyword):
    keyword = stem(keyword)
    index = IndexServer('tcp://localhost:5550', 'enwiki')

    t1 = time()

    res = index.query(keyword).values

    t2 = time()

    if not res: return None
    count = len(res)
    res = res[0]
    postings = map(cPickle.loads, res.value.parts)

    ps = OrderedDict()
    for sha1, i in postings:
        if sha1 in ps: ps[sha1].append(i)
        else: ps[sha1] = [i]

    t3 = time()

    return DocPostings(ps, count, len(postings), t2-t1, t3-t2)


RenderedDocs = namedtuple('RenderedDocs', ['docs', 'render_time'])

def show_documents(docs, hili=lambda w:w):
    if not docs: return None

    NUM_BEFORE = 5
    NUM_AFTER = 5

    t1 = time()

    with open('mongo.cred', 'rt') as f:
        MONGO_HOST = f.readline().strip()
        MONGO_DB   = f.readline().strip()
        MONGO_USER = f.readline().strip()
        MONGO_PASS = f.readline().strip()
    MONGO_ADDRESS = 'mongodb://{user}:{password}@{host}/{db}'.format(user=MONGO_USER, password=MONGO_PASS, host=MONGO_HOST, db=MONGO_DB)
    mongo = MongoClient(MONGO_ADDRESS)
    db = mongo[MONGO_DB]
    articles = db.articles

    missing_doc = {'title': '???', 'url': '#', 'parts': []}

    result = []
    for sha1, positions in docs.items():
        doc = articles.find_one({'_id': sha1})

        if not doc:
          result.append(missing_doc)
          continue

        tokens = doc['text']
        parts = []
        for pos in positions:
            ws = tokens[pos-NUM_BEFORE : pos] + [hili(tokens[pos])] + tokens[pos+1 : pos+NUM_AFTER]
            parts.append(' '.join(ws))

        result.append({'title': doc['title'], 'url': '#', 'parts': parts})

    t2 = time()

    return RenderedDocs(result, t2-t1)


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
