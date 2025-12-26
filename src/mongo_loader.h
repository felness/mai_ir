#pragma once
#include <string>
#include <mongocxx/client.hpp>
#include <mongocxx/collection.hpp>

struct MongoConfig {
    std::string uri = "mongodb://localhost:27017";
    std::string dbname = "lab_corpus";
    std::string collname = "documents_clean";
};

struct DocMeta {
    std::string mongo_id;
    std::string title;
    std::string source;
    std::string url;
};

class MongoLoader {
public:
    explicit MongoLoader(const MongoConfig& cfg);

    mongocxx::collection& collection();
    std::string fetch_snippet_by_oid_hex(const std::string& oid_hex, size_t max_chars = 200);

private:
    mongocxx::client client_;
    mongocxx::collection coll_;
};
