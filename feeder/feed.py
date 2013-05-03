#!/usr/bin/env python2

from pymongo import MongoClient
from bz2 import BZ2File
import rpcz

import index_server_rpcz as index_rpcz
import index_server_pb2 as index_pb
import parse_wiki


DUMP_PATH = '/home/kirrun/Downloads/enwiki-20130403-pages-meta-current1.xml-p000000010p000010000.bz2'


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
        for (title, sha1, text) in parse_wiki.articles(f):

            if text.startswith('#REDIRECT'): continue

            print(title)

            # Index
            bdata = index_pb.BuilderData()
            for i in range(1000):
                record = bdata.records.add()

                record.key = u'{0}{1}'.format(title, i)
                record.value = 'helloworld'
            iserver.feedData(bdata, deadline_ms=10)

            # MongoDB
            doc = {
                'sha1': sha1,
                'title': title,
                'text': text,
            }
            articles.insert(doc)
finally:
    mongo.close()
    iserver.closeStore(index_pb.Void())
