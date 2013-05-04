#include "stagedb.hpp"

#include <memory>
#include <leveldb/db.h>
#include <leveldb/write_batch.h>

#include "exceptions.hpp"

namespace fs = boost::filesystem;

template <>
struct pimpl<stage_db>::implementation
{
    leveldb::WriteBatch batch;
    std::unique_ptr<leveldb::DB> db;
};

using boost::string_ref;

leveldb::Slice as_slice(string_ref const& s)
{
    return leveldb::Slice(s.data(), s.size());
}

stage_db::stage_db(fs::path const& path, bool read_only)
{
    implementation& impl = **this;

    std::string db_path = path.string();
    leveldb::DB* raw_db;
    
    leveldb::Options options;
    options.create_if_missing = true;

    leveldb::Status status = leveldb::DB::Open(options, db_path, &raw_db);
    if (!status.ok())
        BOOST_THROW_EXCEPTION(common_exception()
                << errinfo_rpc_code(::rpc_error::INVALID_STORE)
                << errinfo_message("Cannot open store db"));
    impl.db.reset(raw_db);
}

std::string stage_db::get(string_ref const& key)
{
    implementation& impl = **this;

    std::string data;
    leveldb::Status s = impl.db->Get(leveldb::ReadOptions(), as_slice(key), &data);
    if (!s.ok()) {
        BOOST_THROW_EXCEPTION(common_exception()
                << errinfo_rpc_code(::rpc_error::INVALID_STORE)
                << errinfo_message(std::string("Error while reading db, key = ") + key.begin()));
    }
    return data;
}

void stage_db::append(string_ref const& key, string_ref const& value, bool transacted)
{
    implementation& impl = **this;

    std::string data;
    leveldb::Status s = impl.db->Get(leveldb::ReadOptions(), as_slice(key), &data);
    if (s.ok()) {
        data.append(value.begin(), value.end());
        if (transacted) {
            impl.batch.Put(as_slice(key), data);
            return;
        }
        s = impl.db->Put(leveldb::WriteOptions(), as_slice(key), data);
    } else {
        if (transacted) {
            impl.batch.Put(as_slice(key), as_slice(value));
            return;
        }
        s = impl.db->Put(leveldb::WriteOptions(), as_slice(key), as_slice(value));
    }
    if (!s.ok()) {
        BOOST_THROW_EXCEPTION(common_exception()
                << errinfo_rpc_code(::rpc_error::INVALID_STORE)
                << errinfo_message(std::string("Error while writing to db, key = ") + key.begin()));
    }
}

void stage_db::rollback()
{
    implementation& impl = **this;
    impl.batch = leveldb::WriteBatch();
}

void stage_db::commit()
{
    implementation& impl = **this;
    leveldb::Status s = impl.db->Write(leveldb::WriteOptions(), &impl.batch);
    if (!s.ok()) {
        BOOST_THROW_EXCEPTION(common_exception()
                << errinfo_rpc_code(::rpc_error::INVALID_STORE)
                << errinfo_message("Error while committing to db"));
    }
    impl.batch = leveldb::WriteBatch();
}
