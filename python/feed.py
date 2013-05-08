#!/usr/bin/env python2

from __future__ import print_function

import argparse
from bz2 import BZ2File
from collections import Counter, defaultdict
import cPickle
from pymongo import MongoClient
import rpcz
from time import time

from extract import unwiki
import index_server_pb2 as index_pb
import index_server_rpcz as index_rpcz
from nlp import itokenise, normalise
import parse_wiki
import utils
from utils import grouper, negate_tokens


parser = argparse.ArgumentParser(description='Populate index databases.')
parser.add_argument('dumpfile', help='dump-file path')
parser.add_argument('-m', '--mongocred', default='mongo.cred', help='path to MongoDB credentials', metavar='FILE')
parser.add_argument('-r', '--round', type=int, default=50, help='number of articles to process during one round', metavar='NUMBER')
parser.add_argument('--disable-index', action='store_true', default=False, help='no not build index on the index server')
parser.add_argument('--disable-mongo', action='store_true', default=False, help='no not store documents in MongoDB')
parser.add_argument('--skip', type=int, default=0, help='skip this number of articles')
parser.add_argument('--empty', action='store_true', default=False, help='empty database')
args = parser.parse_args()


##
# Initialising index-store
if not args.disable_index:
    ISERVER_ADDRESS = 'tcp://localhost:5555'
    STORE_NAME = 'enwiki'

    app = rpcz.Application()
    iserver = index_rpcz.IndexBuilderService_Stub(
                app.create_rpc_channel(ISERVER_ADDRESS))
    store = index_pb.StoreParameters()
    store.location = STORE_NAME
    if args.empty:
        store.overwrite = True
        iserver.createStore(store, deadline_ms=1)
    else:
        store.overwrite = False
        iserver.openStore(store, deadline_ms=1)


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
    if args.empty:
        articles.drop()
        articles.ensure_index([('_id', 1), ('size', 1)]) # covering index to speed up size queries

    db.service.remove({'_id': 'avg_len'})


def update_avg_len():
    pipeline = [
        {'$project': {'_id': 0, 'size': 1}},
        {'$group': {'_id': None, 'total': {'$sum': '$size'}}},
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
        processed_articles = skip

        for docgroup in grouper(args.round, parser):

            t1 = time()

            postings = defaultdict(lambda: [])
            bdata = index_pb.BuilderData()
            docs = []

            for doc in docgroup:
                if not doc: break

                (title, ns, sha1, text) = doc

                if ns != '0': continue
                if not text: continue # wtf
                if text[:9].lower() == ('#redirect'): continue


                text = unwiki(text)
                itokens = list(itokenise(text))
                itokens_title = list(itokenise(title))

                tokens = normalise(utils.tokens(text, itokens))
                tokens_title = negate_tokens(normalise(utils.tokens(title, itokens_title)))
                tokens_all = tokens_title + tokens
                if not tokens_all: continue

                article_tokens = Counter()

                thisdoc_postings = defaultdict(lambda: [])
                for i, w in tokens_all:
                    article_tokens[w] += 1
                    thisdoc_postings[w].append(i)
                for w, l in thisdoc_postings.iteritems():
                    postings[w].append((sha1, l))

                docs.append({
                    '_id': sha1,
                    'title': title,
                    'tokens_title': itokens_title,
                    'text': text,
                    'tokens': itokens,
                    'size': len(itokens),
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

            processed_articles += args.round

            new_total = articles_count + this_round_count
            print('preproc: {preproc:.6f}  iserv: {iserv:.6f}  mongo: {mongo:.6f}  // +{new} articles (= {total} total @ {processed})'.format(
                preproc = time_preproc / this_round_count,
                iserv = time_iserv / this_round_count,
                mongo = time_mongo / this_round_count,
                new = this_round_count,
                total = new_total,
                processed = processed_articles,
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
