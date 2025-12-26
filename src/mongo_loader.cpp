#include "mongo_loader.h"

#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/basic/kvp.hpp>
#include <bsoncxx/oid.hpp>
#include <mongocxx/uri.hpp>
#include <mongocxx/options/find.hpp>

using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_document;

static const bsoncxx::stdx::string_view FIELD_TEXT = "clean_text";

MongoLoader::MongoLoader(const MongoConfig& cfg)
    : client_(mongocxx::uri{cfg.uri})
{
    auto db = client_[cfg.dbname];
    coll_ = db[cfg.collname];
}

mongocxx::collection& MongoLoader::collection() {
    return coll_;
}

std::string MongoLoader::fetch_snippet_by_oid_hex(const std::string& oid_hex, size_t max_chars) {
    try {
        bsoncxx::oid oid{oid_hex};
        auto filter = make_document(kvp("_id", oid));

        mongocxx::options::find opts;
        opts.projection(make_document(kvp(FIELD_TEXT, 1)));

        auto maybe = coll_.find_one(filter.view(), opts);
        if (!maybe) return "";

        auto v = (*maybe)[FIELD_TEXT];
        if (!v || v.type() != bsoncxx::type::k_string) return "";

        auto sv = v.get_string().value;
        std::string txt(sv.data(), sv.size());
        if (txt.size() > max_chars) txt.resize(max_chars), txt += "...";
        return txt;
    } catch (...) {
        return "";
    }
}
