#pragma once
#include <cstddef>
#include <cstdint>
#include <string>

struct TokenizerConfig {
    bool keep_hyphen_inside = true;
    bool drop_short_tokens = true;
    size_t min_token_bytes = 2;
    bool ascii_to_lower = true;
};

using token_cb = void (*)(const char* token, void* user);

void tokenize_stream(
    const std::string& text,
    const TokenizerConfig& cfg,
    token_cb cb,
    void* user
);
