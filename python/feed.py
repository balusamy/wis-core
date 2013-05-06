#!/usr/bin/env python2

from __future__ import print_function

import argparse
from bz2 import BZ2File
from collections import Counter, OrderedDict
import cPickle
from pymongo import MongoClient
import rpcz
from time import time

from extract import unwiki
import index_server_pb2 as index_pb
import index_server_rpcz as index_rpcz
from nlp import tokenise, normalise
import parse_wiki
from utils import grouper


parser = argparse.ArgumentParser(description='Populate index databases.')
parser.add_argument('dumpfile', help='dump-file path')
parser.add_argument('-m', '--mongocred', default='mongo.cred', help='path to MongoDB credentials', metavar='FILE')
parser.add_argument('-r', '--round', type=int, default=50, help='number of articles to process during one round', metavar='NUMBER')
parser.add_argument('--disable-index', action='store_true', default=False, help='no not build index on the index server')
parser.add_argument('--disable-mongo', action='store_true', default=False, help='no not store documents in MongoDB')
parser.add_argument('--skip', type=int, default=0, help='skip this number of articles')
args = parser.parse_args()


##
# Initialising index-store
if not args.disable_index:
    ISERVER_ADDRESS = 'tcp://localhost:5555'
    STORE_NAME = 'idontcare'

    app = rpcz.Application()
    iserver = index_rpcz.IndexBuilderService_Stub(
                app.create_rpc_channel(ISERVER_ADDRESS))
    store = index_pb.StoreParameters()
    store.location = STORE_NAME
    store.overwrite = True
    iserver.createStore(store, deadline_ms=1)


##
# Initialising MongoDB
if not args.disable_mongo:
    with open(args.mongocred, 'rt') as f:
        MONGO_HOST = f.readline().strip()
        MONGO_DB   = f.readline().strip()
        MONGO_USER = f.readline().strip()
        MONGO_PASS = f.readline().strip()
    MONGO_ADDRESS = 'mongodb://{user}:{password}@{host}/{db}'.format(user=MONGO_USER, password=MONGO_PASS, host=MONGO_HOST, db=MONGO_DB)

    mongo = MongoClient(MONGO_ADDRESS)
    db = mongo[MONGO_DB]
    articles = db.articles

    articles.drop()
    db.service.remove({'_id': 'avg_len'})


def update_avg_len():
    pipeline = [
        {'$project': {'_id': 0, 'text': 1}},
        {'$unwind': '$text'},
        {'$group': {'_id': None, 'total': {'$sum': 1}}},
    ]

    avg_len = articles.aggregate(pipeline)['result'][0]['total'] / articles.count()
    db.service.save({'_id': 'avg_len', 'val': avg_len})
    return avg_len


try:
    with BZ2File(args.dumpfile, 'r') as f:
        parser = parse_wiki.articles(f)

        skip = args.skip
        for i in range(skip):
            parser.next()

        time_preproc = 0
        time_iserv = 0
        time_mongo = 0
        last_time = time()
        articles_count = 0
        this_round_count = 0

        for docgroup in grouper(args.round, parser):

            t1 = time()

            postings = OrderedDict()
            bdata = index_pb.BuilderData()
            docs = []

            for doc in docgroup:
                if not doc: break

                (title, ns, sha1, text) = doc

                if ns != '0': continue
                if not text: continue # wtf
                if text.startswith('#REDIRECT'): continue


                text = unwiki(text)
                all_tokens = tokenise(text)
                tokens = normalise(all_tokens)

                if not tokens: continue

                article_tokens = Counter()

                for i, w in tokens:
                    article_tokens[w] += 1
                    if w in postings: postings[w].append((sha1, i))
                    else: postings[w] = [(sha1, i)]

                docs.append({
                    '_id': sha1,
                    'title': title,
                    'text': all_tokens,
                    'maxf': article_tokens.most_common(1)[0][1],
                })

            if not docs: continue

            for w, ps in postings.items():
                record = bdata.records.add()
                record.key = w
                record.value.parts.extend(map(cPickle.dumps, ps))

            t2 = time()

            # Index
            if not args.disable_index:
                iserver.feedData(bdata, deadline_ms=10)

            t3 = time()

            # MongoDB
            if not args.disable_mongo:
                articles.insert(docs)

            t4 = time()

            this_round_count += len(docs)

            time_preproc += t2-t1
            time_iserv += t3-t2
            time_mongo += t4-t3


            ##
            # Reporting stats

            new_total = articles_count + this_round_count
            print('preproc: {preproc:.6f}  iserv: {iserv:.6f}  mongo: {mongo:.6f}  // +{new} articles (= {total} total)'.format(
                preproc = time_preproc / this_round_count,
                iserv = time_iserv / this_round_count,
                mongo = time_mongo / this_round_count,
                new = this_round_count,
                total = new_total,
            ))
            print('{speed:.2f} articles/s'.format(speed=this_round_count/(time()-last_time)))

            articles_count = new_total
            this_round_count = 0
            time_preproc = time_iserv = time_mongo = 0
            last_time = time()


        print('Recalculating service vars now...')
        t1 = time()
        avg_len = update_avg_len()
        t2 = time()
        print('Done in {0:.1f} seconds. Avg document size = {1}.'.format(t2-t1, avg_len))
finally:
    if not args.disable_mongo:
        mongo.close()
    if not args.disable_index:
        iserver.closeStore(index_pb.Void())
