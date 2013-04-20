find_program(PROTOC_GEN_CPP_RPCZ NAMES protoc-gen-cpp_rpcz)
find_program(PROTOC_GEN_PYTHON_RPCZ NAMES protoc-gen-python_rpcz)

function(PROTOBUF_GENERATE_RPCZ_ONLY SRCS HDRS)
  PROTOBUF_GENERATE_MULTI(PLUGIN "cpp_rpcz" PROTOS ${ARGN}
                          OUTPUT_STRUCT "_SRCS:.rpcz.cc;_HDRS:.rpcz.h"
                          FLAGS "--plugin=protoc-gen-cpp_rpcz=${PROTOC_GEN_CPP_RPCZ}")
  set(${SRCS} ${_SRCS} PARENT_SCOPE)
  set(${HDRS} ${_HDRS} PARENT_SCOPE)
endfunction()

function(PROTOBUF_GENERATE_PYTHON_RPCZ_ONLY SRCS)
  PROTOBUF_GENERATE_MULTI(PLUGIN "python_rpcz" PROTOS ${ARGN}
                          OUTPUT_STRUCT "_SRCS:_rpcz.py"
                          FLAGS "--plugin=protoc-gen-python_rpcz=${PROTOC_GEN_PYTHON_RPCZ}")
  set(${SRCS} ${_SRCS} PARENT_SCOPE)
endfunction()

function(PROTOBUF_GENERATE_RPCZ SRCS HDRS)
    PROTOBUF_GENERATE_CPP(_SRCS_PB2 _HDRS_PB2 ${ARGN})
    PROTOBUF_GENERATE_RPCZ_ONLY(_SRCS_RPCZ _HDRS_RPCZ ${ARGN})
    set(${SRCS} ${_SRCS_PB2} ${_SRCS_RPCZ} PARENT_SCOPE)
    set(${HDRS} ${_HDRS_PB2} ${_HDRS_RPCZ} PARENT_SCOPE)
endfunction()

function(PROTOBUF_GENERATE_PYTHON_RPCZ SRCS)
    PROTOBUF_GENERATE_PYTHON(_SRCS_PB2 ${ARGN})
    PROTOBUF_GENERATE_PYTHON_RPCZ_ONLY(_SRCS_RPCZ ${ARGN})
    set(${SRCS} ${_SRCS_PB2} ${_SRCS_RPCZ} PARENT_SCOPE)
endfunction()
