#include "stagedb.hpp"

#include <memory>
#include <leveldb/db.h>
#include <leveldb/write_batch.h>

namespace fs = boost::filesystem;

template <>
struct pimpl<stage_db>::implementation
{
    std::unique_ptr<leveldb::DB> db;
};

stage_db::stage_db(fs::path const& path, bool read_only)
{
    implementation& impl = **this;

    std::string db_path = path.string();
    leveldb::DB* raw_db;
    
    leveldb::Options options;
    options.create_if_missing = true;

    leveldb::Status status = leveldb::DB::Open(options, db_path, &raw_db);
    if (!status.ok())
        // TODO
        assert(false);
    impl.db.reset(raw_db);
}

void stage_db::append(indexserver::BuilderData const& data)
{
    implementation& impl = **this;

    leveldb::WriteBatch batch;
    for (indexserver::IndexRecord const& r : data.records())
    {
        batch.Put(r.key(), r.value());
    }
    impl.db->Write(leveldb::WriteOptions(), &batch);
}
