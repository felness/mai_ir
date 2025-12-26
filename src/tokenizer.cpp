#include "tokenizer.h"
#include <cctype>

static inline bool is_ascii_alnum(unsigned char c) {
    return (c >= 'a' && c <= 'z') ||
           (c >= 'A' && c <= 'Z') ||
           (c >= '0' && c <= '9');
}

static inline unsigned char ascii_lower(unsigned char c) {
    if (c >= 'A' && c <= 'Z') return (unsigned char)(c - 'A' + 'a');
    return c;
}

static inline bool is_utf8_cyrillic_2bytes(unsigned char b1, unsigned char b2) {
    if (b1 == 0xD0) {
        return (b2 == 0x81) || (b2 >= 0x90 && b2 <= 0xBF);
    }
    if (b1 == 0xD1) {
        return (b2 == 0x91) || (b2 >= 0x80 && b2 <= 0x8F);
    }
    return false;
}

static inline size_t utf8_char_len(unsigned char c) {
    if (c < 0x80) return 1;
    if ((c >> 5) == 0b110) return 2;
    if ((c >> 4) == 0b1110) return 3;
    if ((c >> 3) == 0b11110) return 4;
    return 1;
}

static inline bool is_word_char_utf8(const std::string& s, size_t i) {
    unsigned char c = (unsigned char)s[i];
    if (c < 0x80) return is_ascii_alnum(c);
    if (i + 1 < s.size()) {
        unsigned char c2 = (unsigned char)s[i + 1];
        if (is_utf8_cyrillic_2bytes(c, c2)) return true;
    }
    return false;
}

void tokenize_stream(
    const std::string& text,
    const TokenizerConfig& cfg,
    token_cb cb,
    void* user
) {
    const size_t n = text.size();
    size_t i = 0;

    auto commit = [&](size_t start, size_t end) {
        if (end <= start) return;
        size_t len = end - start;
        if (cfg.drop_short_tokens && len < cfg.min_token_bytes) return;

        std::string token;
        token.reserve(len);

        for (size_t k = start; k < end; ) {
            unsigned char c = (unsigned char)text[k];
            if (c < 0x80) {
                token.push_back((char)(cfg.ascii_to_lower ? ascii_lower(c) : c));
                k += 1;
            } else {
                size_t clen = utf8_char_len(c);
                for (size_t t = 0; t < clen && (k + t) < end; ++t) token.push_back(text[k + t]);
                k += clen;
            }
        }

        cb(token.c_str(), user);
    };

    while (i < n) {
        if (!is_word_char_utf8(text, i)) {
            i += utf8_char_len((unsigned char)text[i]);
            continue;
        }

        size_t start = i;
        size_t end = i;

        while (i < n) {
            if (cfg.keep_hyphen_inside && text[i] == '-') {
                bool left_ok = (i > start);
                bool right_ok = (i + 1 < n) && is_word_char_utf8(text, i + 1);
                if (left_ok && right_ok) {
                    i += 1;
                    end = i;
                    continue;
                } else {
                    break;
                }
            }

            if (is_word_char_utf8(text, i)) {
                unsigned char c = (unsigned char)text[i];
                i += utf8_char_len(c);
                end = i;
            } else {
                break;
            }
        }

        commit(start, end);
    }
}
