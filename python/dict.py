#!/usr/bin/env python2

from __future__ import print_function

import argparse
from bz2 import BZ2File
import rpcz
from time import time

from extract import unwiki
import index_server_pb2 as index_pb
import index_server_rpcz as index_rpcz
from nlp import normalise_gently, has_char
import parse_wiki
import utils
from utils import grouper


parser = argparse.ArgumentParser(description='Populate index databases.')
parser.add_argument('dumpfile', help='dump-file path')
parser.add_argument('-r', '--round', type=int, default=50, help='number of articles to process during one round', metavar='NUMBER')
parser.add_argument('--skip', type=int, default=0, help='skip this number of articles')
args = parser.parse_args()


##
# Initialising index-store
ISERVER_ADDRESS = 'tcp://localhost:5555'
STORE_NAME = 'enwiki'

app = rpcz.Application()
iserver = index_rpcz.IndexBuilderService_Stub(
            app.create_rpc_channel(ISERVER_ADDRESS))
store = index_pb.StoreParameters()
store.location = STORE_NAME
store.overwrite = False
iserver.openStore(store, deadline_ms=1)


def good(t):
    return len(t) > 3 and has_char(t)


try:
    with BZ2File(args.dumpfile, 'r') as f:
        parser = parse_wiki.articles(f)

        skip = args.skip
        for i in range(skip):
            parser.next()

        time_preproc = 0
        time_iserv = 0
        last_time = time()
        articles_count = 0
        this_round_count = 0
        processed_articles = skip

        for docgroup in grouper(args.round, parser):

            t1 = time()

            bdata = index_pb.BuilderData()
            round_tokens = set()
            processed = 0

            for doc in docgroup:
                if not doc: break

                (title, ns, sha1, text) = doc

                if ns != '0': continue
                if not text: continue # wtf
                if text[:9].lower() == ('#redirect'): continue

                processed += 1


                text = unwiki(text)

                tokens = normalise_gently(filter(good, utils.tokens(text)))
                tokens_title = normalise_gently(filter(good, utils.tokens(title)))
                round_tokens |= set(tokens_title) | set(tokens)


            for w in round_tokens:
                record = bdata.records.add()
                record.key = w
                record.value.parts.append('')
                del record.value.parts[:]

            t2 = time()

            # Index
            iserver.feedData(bdata, deadline_ms=10)

            t3 = time()

            this_round_count += processed

            time_preproc += t2-t1
            time_iserv += t3-t2


            ##
            # Reporting stats

            processed_articles += args.round

            new_total = articles_count + this_round_count
            print('preproc: {preproc:.6f}  iserv: {iserv:.6f}  // +{new} articles (= {total} total @ {processed})'.format(
                preproc = time_preproc / this_round_count,
                iserv = time_iserv / this_round_count,
                new = this_round_count,
                total = new_total,
                processed = processed_articles,
            ))
            print('{speed:.2f} articles/s'.format(speed=this_round_count/(time()-last_time)))

            articles_count = new_total
            this_round_count = 0
            time_preproc = time_iserv = time_mongo = 0
            last_time = time()
finally:
    iserver.closeStore(index_pb.Void())
