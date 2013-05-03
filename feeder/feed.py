#!/usr/bin/env python2

import rpcz

import index_server_rpcz as index_rpcz
import index_server_pb2 as index_pb
import time


app = rpcz.Application()
stub = index_rpcz.IndexBuilderService_Stub(
        app.create_rpc_channel('tcp://localhost:5555'))

store = index_pb.StoreParameters()
store.location = 'idontcare'
store.overwrite = True
stub.createStore(store)
#stub.openStore(store)

prefix = '_'*300

total = 0

try:
    for j in range(1000):
        print('j = {0}'.format(j))
        k = j * 1000
        data = index_pb.BuilderData()
        for i in range(1000):
            record = data.records.add()

            record.key = prefix + 'ninebytes{0}'.format(k+i)
            record.value = 'helloworld'

        a = time.time()
        stub.feedData(data, deadline_ms=10)
        b = time.time()
        d = b-a
        total += d
        print('{0} ; {1}'.format(d, total))
finally:
    stub.closeStore(index_pb.Void())
