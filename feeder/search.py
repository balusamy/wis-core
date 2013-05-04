#!/usr/bin/env python2
import argparse
import rpcz

import index_server_rpcz as index_rpcz
import index_server_pb2 as index_pb

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

class index_server(object):
    def __init__(self, server_endpoint, store_name):
        self.app = rpcz.Application()
        self.iserver = index_rpcz.IndexQueryService_Stub(
                    self.app.create_rpc_channel(server_endpoint))
        store = index_pb.UseStore()
        store.location = store_name
        self.iserver.useStore(store, deadline_ms=1)

    def query(self, query_word, max_mistakes, timeout):
        query = index_pb.WordQuery()
        query.options.Clear()
        query.word = query_word
        query.maxCorrections = max_mistakes
        return self.iserver.wordQuery(query, deadline_ms=timeout)
        

if __name__ == '__main__':
    args = parser.parse_args()
    index = index_server(args.server, args.index)
    result = index.query(unicode(args.query, "UTF-8"), args.mistakes, args.timeout)
    if result.HasField('exact_total'):
        print "Total results: {0}".format(result.exact_total)
    for i, record in enumerate(result.values):
        print "Result #{0}: {1} = {2}".format(i, record.key.encode("UTF-8"), record.value) 
    print "Done"
