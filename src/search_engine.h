#pragma once
#include <string>
#include <vector>
#include <cstdint>

#include "mongo_loader.h"
#include "tokenizer.h"
#include "boolean_index.h"

struct SearchEngineConfig {
    TokenizerConfig tokenizer;
};

class SearchEngine {
public:
    SearchEngine(MongoLoader& loader, SearchEngineConfig cfg);

    void build_index(int64_t limit = 0);

    std::vector<uint32_t> search_and(const std::string& query) const;

    const std::vector<DocMeta>& meta() const { return meta_; }
    size_t docs_indexed() const { return meta_.size(); }
    size_t terms_count() const { return index_.terms_count(); }

private:
    MongoLoader& loader_;
    SearchEngineConfig cfg_;

    BooleanIndex index_;
    std::vector<DocMeta> meta_; 

    static bool extract_oid_hex(const bsoncxx::document::view& doc, std::string& out_hex);
};
