#include <iostream>
#include <string>

#include <mongocxx/instance.hpp>

#include "mongo_loader.h"
#include "search_engine.h"

static void print_results(
    MongoLoader& loader,
    const std::vector<uint32_t>& doc_ids,
    const std::vector<DocMeta>& meta
) {
    if (doc_ids.empty()) {
        std::cout << "Ничего не найдено.\n";
        return;
    }

    std::cout << "\nFOUND: " << doc_ids.size() << " documents\n";
    std::cout << "----------------------------------------\n";

    size_t show = std::min<size_t>(10, doc_ids.size());
    for (size_t i = 0; i < show; ++i) {
        uint32_t did = doc_ids[i];
        if (did >= meta.size()) continue;

        const auto& m = meta[did];

        std::cout << "[" << (i + 1) << "] doc_id=" << did << "\n";
        std::cout << "Mongo _id: " << (m.mongo_id.empty() ? "[нет]" : m.mongo_id) << "\n";
        std::cout << "Title: " << (m.title.empty() ? "[без названия]" : m.title) << "\n";
        std::cout << "Source: " << (m.source.empty() ? "[неизвестно]" : m.source) << "\n";
        std::cout << "URL: " << (m.url.empty() ? "[url отсутствует]" : m.url) << "\n";

        if (!m.mongo_id.empty()) {
            std::string sn = loader.fetch_snippet_by_oid_hex(m.mongo_id, 200);
            if (!sn.empty()) std::cout << "Snippet: " << sn << "\n";
        }

        std::cout << "----------------------------------------\n";
    }

    if (doc_ids.size() > show) {
        std::cout << "... and " << (doc_ids.size() - show) << " more\n";
    }
}

int main(int argc, char** argv) {
    MongoConfig mcfg;
    if (argc >= 2) mcfg.uri = argv[1];
    if (argc >= 3) mcfg.dbname = argv[2];
    if (argc >= 4) mcfg.collname = argv[3];
    int64_t limit = (argc >= 5) ? std::stoll(argv[4]) : 0;

    mongocxx::instance inst{}; 

    try {
        MongoLoader loader(mcfg);

        SearchEngineConfig scfg;
        SearchEngine engine(loader, scfg);
        engine.build_index(limit);

        std::cout << "Ready. docs=" << engine.docs_indexed()
                  << " terms=" << engine.terms_count() << "\n";

        while (true) {
            std::cout << "\nQuery (empty to exit): ";
            std::string q;
            std::getline(std::cin, q);
            if (q.empty()) break;

            auto res = engine.search_and(q);
            print_results(loader, res, engine.meta());
        }

        return 0;
    } catch (const std::exception& e) {
        std::cerr << "ERROR: " << e.what() << "\n";
        return 2;
    }
}
