#include "value_db.hpp"

#include <memory>
#include <sstream>
#include <mongo/client/dbclient.h>
#include <unordered_map>

#include "exceptions.hpp"

template <>
struct pimpl<indexer::value_db>::implementation
{
    mutable mongo::DBClientConnection db;
    std::string ns;
};

template <>
struct pimpl<indexer::value_db::transaction>::implementation
{
    typedef std::unordered_map<std::string, 
            std::unique_ptr<mongo::BSONObjBuilder>> objects_t;
    objects_t objects;
    mongo::DBClientConnection* connection;
    std::string* ns;
};

using boost::string_ref;

namespace indexer {

mongo::StringData as_str(string_ref const& x)
{
    return mongo::StringData(x.data(), x.size());
}

value_db::value_db(string_ref const& server, string_ref const& ns)
{
    implementation& impl = **this;
    impl.db.connect(server.data());
    impl.ns.assign(ns.begin(), ns.end());
    impl.db.ensureIndex(impl.ns, BSON( "key" << 1 ));
}

std::string value_db::get(string_ref const& key) const
{
    implementation const& impl = **this;
    auto cursor = impl.db.query(impl.ns, QUERY("key" << as_str(key)));
    std::ostringstream oss;
    while (cursor->more()) {
        auto const& obj = cursor->next();
        for (auto const& part : obj["values"].Array()) {
            oss << part.String();
        }
    }
    return oss.str();
}

std::unique_ptr<value_db::transaction> value_db::start_tx()
{
    implementation& impl = **this;
    std::unique_ptr<transaction> result(new transaction());
    (*result)->connection = &impl.db;
    (*result)->ns = &impl.ns;
    return result;
}

value_db::transaction::transaction()
{
}

value_db::transaction::~transaction()
{
}

void value_db::transaction::append(string_ref const& key, string_ref const& value)
{
    implementation& impl = **this;
    std::string key_s(key.begin(), key.end());
    auto it = impl.objects.find(key_s);
    if (it == impl.objects.end()) {
        typedef implementation::objects_t::mapped_type bptr_t;
        it = impl.objects.emplace(key_s, bptr_t(new mongo::BSONObjBuilder())).first;
        *it->second << mongo::GENOID << "key" << as_str(key);
        mongo::BSONArrayBuilder values(it->second->subarrayStart("values"));
        values << as_str(value);
    } else {
        mongo::BSONArrayBuilder(it->second->bb()) << as_str(value);
    }
}

void value_db::transaction::commit()
{
    implementation& impl = **this;
    for (auto const& p : impl.objects) {
        mongo::BSONObj obj = p.second->obj();
        impl.connection->insert(*impl.ns, obj);
    }
}

void value_db::transaction::rollback()
{
    implementation& impl = **this;
    impl.objects.clear();
}

}
