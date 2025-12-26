#pragma once
#include <string>
#include <vector>
#include <unordered_map>
#include <cstdint>

class BooleanIndex {
public:
    void add_term(const std::string& term, uint32_t doc_id);

    const std::vector<uint32_t>* get_postings(const std::string& term) const;

    size_t terms_count() const { return term_to_docs_.size(); }

private:
    std::unordered_map<std::string, std::vector<uint32_t>> term_to_docs_;
};

std::vector<uint32_t> postings_union(
    const std::vector<uint32_t>* a,
    const std::vector<uint32_t>* b
);

std::vector<uint32_t> postings_intersect(
    const std::vector<uint32_t>& a,
    const std::vector<uint32_t>& b
);
