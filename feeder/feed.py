#!/usr/bin/env python2

from __future__ import print_function

import sys
from pymongo import MongoClient
from bz2 import BZ2File
import rpcz
from time import time

import index_server_rpcz as index_rpcz
import index_server_pb2 as index_pb
import parse_wiki
from extract import unwiki


DUMP_PATH = '/home/kirrun/Downloads/enwiki-20130403-pages-meta-current1.xml-p000000010p000010000.bz2'
STAT_REPORT = 10


##
# Initialising index-store

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

MONGO_ADDRESS = 'mongodb://localhost:27017'
DB_NAME = 'wiki'

mongo = MongoClient(MONGO_ADDRESS)
db = mongo[DB_NAME]
articles = db.articles
articles.drop()
articles.ensure_index([('sha1', 1)])


try:
    with BZ2File(DUMP_PATH, 'r') as f:
        articles_count = 0

        time_preproc = 0
        time_iserv = 0
        time_mongo = 0

        for (title, sha1, text) in parse_wiki.articles(f):

            if text.startswith('#REDIRECT'): continue

            t1 = time()

            text = unwiki(text)
            text = text.split()

            bdata = index_pb.BuilderData()
            for i, w in enumerate(text):
                record = bdata.records.add()

                record.key = w
                record.value = str((sha1, i))

            t2 = time()

            # Index
            iserver.feedData(bdata, deadline_ms=10)

            t3 = time()

            # MongoDB
            doc = {
                'sha1': sha1,
                'title': title,
                'text': text,
            }
            articles.insert(doc)

            t4 = time()

            articles_count += 1

            time_preproc += t2-t1
            time_iserv += t3-t2
            time_mongo += t4-t3

            if articles_count % STAT_REPORT == 0:
                print('preproc: {preproc:.6f}  iserv: {iserv:.6f}  mongo: {mongo:.6f}  // {count} articles'.format(
                    count = articles_count,
                    preproc = time_preproc / STAT_REPORT,
                    iserv = time_iserv / STAT_REPORT,
                    mongo = time_mongo / STAT_REPORT,
                ))
                sys.stdout.flush()
                time_preproc = time_iserv = time_mongo = 0
except Exception as e:
    print(e)
finally:
    mongo.close()
    iserver.closeStore(index_pb.Void())
