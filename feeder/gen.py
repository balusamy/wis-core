#!/usr/bin/env python2

from rpcz import compiler


PROTO = '../index_server.proto'

compiler.generate_proto(PROTO, '.')
compiler.generate_proto(
        PROTO, '.',
        with_plugin='python_rpcz', suffix='_rpcz.py')
