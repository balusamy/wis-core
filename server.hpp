#pragma once

#include <rpcz/rpcz.hpp>
#include "index_server.pb.h"
#include "index_server.rpcz.h"

namespace indexserver {

class IndexBuilder : public IndexBuilderService
{
  virtual void createStore(const CreateStore& request, rpcz::reply<Void> reply);
  virtual void buildIndex(const Void& request, rpcz::reply<Void> reply);
};

}

