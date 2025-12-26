#include "search_engine.h"

#include <iostream>
#include <string>
#include <vector>
#include <algorithm>

#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/basic/kvp.hpp>
#include <mongocxx/options/find.hpp>

#include "tokenizer.h"
#include "stemmer.h"
#include "boolean_index.h"

using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_document;

static const bsoncxx::stdx::string_view FIELD_TEXT  = "clean_text";
static const bsoncxx::stdx::string_view FIELD_TITLE = "title";
static const bsoncxx::stdx::string_view FIELD_SRC   = "source";
static const bsoncxx::stdx::string_view FIELD_URL   = "url";

static std::string get_str_or_empty(const bsoncxx::document::view& doc,
                                    bsoncxx::stdx::string_view field) {
    auto v = doc[field];
    if (v && v.type() == bsoncxx::type::k_string) {
        auto sv = v.get_string().value;
        return std::string(sv.data(), sv.size());
    }
    return "";
}

bool SearchEngine::extract_oid_hex(const bsoncxx::document::view& doc, std::string& out_hex) {
    auto idv = doc["_id"];
    if (!idv) return false;

    if (idv.type() == bsoncxx::type::k_oid) {
        out_hex = idv.get_oid().value.to_string();
        return true;
    }
    if (idv.type() == bsoncxx::type::k_string) {
        auto sv = idv.get_string().value;
        out_hex.assign(sv.data(), sv.size());
        return true;
    }
    return false;
}

static std::vector<std::string> tokenize_to_vector(const std::string& text,
                                                   const TokenizerConfig& cfg) {
    std::vector<std::string> out;
    out.reserve(64);

    auto cb = [](const char* tok, void* user) {
        auto* v = static_cast<std::vector<std::string>*>(user);
        v->emplace_back(tok);
    };

    tokenize_stream(text, cfg, cb, &out);
    return out;
}

SearchEngine::SearchEngine(MongoLoader& loader, SearchEngineConfig cfg)
    : loader_(loader), cfg_(std::move(cfg)) {}

void SearchEngine::build_index(int64_t limit) {
    auto& coll = loader_.collection();

    mongocxx::options::find opts;
    if (limit > 0) opts.limit(limit);

    opts.projection(make_document(
        kvp("_id", 1),
        kvp(FIELD_TEXT, 1),
        kvp(FIELD_TITLE, 1),
        kvp(FIELD_SRC, 1),
        kvp(FIELD_URL, 1)
    ));

    auto filter = make_document(
        kvp(FIELD_TEXT, make_document(kvp("$exists", true), kvp("$ne", "")))
    );

    std::cout << "Building index...\n";

    meta_.clear();
    uint32_t doc_id = 0;

    for (auto&& doc : coll.find(filter.view(), opts)) {
        auto ct = doc[FIELD_TEXT];
        if (!ct || ct.type() != bsoncxx::type::k_string) continue;

        std::string oid_hex;
        if (!extract_oid_hex(doc, oid_hex)) continue;

        auto sv = ct.get_string().value;
        std::string text(sv.data(), sv.size());

        DocMeta m;
        m.mongo_id = oid_hex;
        m.title = get_str_or_empty(doc, FIELD_TITLE);
        m.source = get_str_or_empty(doc, FIELD_SRC);
        m.url = get_str_or_empty(doc, FIELD_URL);
        meta_.push_back(std::move(m));

        auto toks = tokenize_to_vector(text, cfg_.tokenizer);

        for (const auto& t : toks) {
            if (!t.empty()) {
                index_.add_term(t, doc_id);

                std::string st = stem_ru(t);
                if (!st.empty() && st != t) {
                    index_.add_term(st, doc_id);
                }
            }
        }

        doc_id++;
        if (doc_id % 500 == 0) {
            std::cout << "Indexed docs: " << doc_id << "\r" << std::flush;
        }
    }

    std::cout << "\nIndex built. Docs: " << meta_.size()
              << ", terms: " << index_.terms_count() << "\n";
}

std::vector<uint32_t> SearchEngine::search_and(const std::string& query) const {
    auto qtokens = tokenize_to_vector(query, cfg_.tokenizer);
    if (qtokens.empty()) return {};

    bool first = true;
    std::vector<uint32_t> running;

    for (const auto& qt : qtokens) {
        if (qt.empty()) continue;

        const auto* p_exact = index_.get_postings(qt);

        std::string st = stem_ru(qt);
        const auto* p_stem = (st != qt) ? index_.get_postings(st) : nullptr;

        std::vector<uint32_t> term_docs = postings_union(p_exact, p_stem);

        if (first) {
            running = std::move(term_docs);
            first = false;
        } else {
            running = postings_intersect(running, term_docs);
        }

        if (running.empty()) break;
    }

    return running;
}
