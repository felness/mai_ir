#include "boolean_index.h"

void BooleanIndex::add_term(const std::string& term, uint32_t doc_id) {
    auto& v = term_to_docs_[term];
    if (v.empty() || v.back() != doc_id) v.push_back(doc_id);
}

const std::vector<uint32_t>* BooleanIndex::get_postings(const std::string& term) const {
    auto it = term_to_docs_.find(term);
    if (it == term_to_docs_.end()) return nullptr;
    return &it->second;
}

std::vector<uint32_t> postings_union(const std::vector<uint32_t>* a, const std::vector<uint32_t>* b) {
    std::vector<uint32_t> out;
    if (!a && !b) return out;
    if (a && !b) return *a;
    if (!a && b) return *b;

    const auto& A = *a;
    const auto& B = *b;

    out.reserve(A.size() + B.size());
    size_t i = 0, j = 0;

    while (i < A.size() && j < B.size()) {
        uint32_t x = A[i], y = B[j];
        if (x == y) { out.push_back(x); i++; j++; }
        else if (x < y) { out.push_back(x); i++; }
        else { out.push_back(y); j++; }
    }
    while (i < A.size()) out.push_back(A[i++]);
    while (j < B.size()) out.push_back(B[j++]);

    return out;
}

std::vector<uint32_t> postings_intersect(const std::vector<uint32_t>& a, const std::vector<uint32_t>& b) {
    std::vector<uint32_t> out;
    out.reserve(a.size() < b.size() ? a.size() : b.size());

    size_t i = 0, j = 0;
    while (i < a.size() && j < b.size()) {
        uint32_t x = a[i], y = b[j];
        if (x == y) { out.push_back(x); i++; j++; }
        else if (x < y) i++;
        else j++;
    }
    return out;
}
