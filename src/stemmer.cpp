#include "stemmer.h"
#include <algorithm>
#include <vector>

static bool ends_with(const std::string& s, const std::string& suf) {
    return s.size() >= suf.size() && s.compare(s.size() - suf.size(), suf.size(), suf) == 0;
}
static bool too_short_for_stem(const std::string& t) {
    return t.size() < 8;
}
static void normalize_yo(std::string& s) {
    auto repl = [](std::string& str, const std::string& from, const std::string& to){
        size_t pos = 0;
        while ((pos = str.find(from, pos)) != std::string::npos) {
            str.replace(pos, from.size(), to);
            pos += to.size();
        }
    };
    repl(s, "\xD0\x81", "\xD0\x95"); 
    repl(s, "\xD1\x91", "\xD0\xB5"); 
}
static void strip_soft_sign(std::string& s) {
    const std::string soft = "\xD1\x8C"; // ь
    if (ends_with(s, soft)) s.erase(s.size() - soft.size());
}

std::string stem_ru(std::string token) {
    normalize_yo(token);
    if (too_short_for_stem(token)) return token;

    static std::vector<std::string> suf = []{
        std::vector<std::string> v = {
            "аться","яться","ешься","етесь","ится","ются","ется","утся",
            "ностями","ностях","ностью","ность",
            "остью","ости","ость",
            "ыми","ими","ого","ему","ому","ые","ие","ая","яя","ой","ый","ий","ую","юю",
            "ым","им","ом","ем","ых","их",
            "ами","ями","иями","ием","иям","ям","ам","ов","ев","ёв","ей","ью","ия","ья","ие","ье",
            "ешь","ете","ите","или","ала","ыла","ило","ать","ять","ить","ет","ют","ут",
            "а","я","ы","и","у","ю","о","е"
        };
        std::sort(v.begin(), v.end(), [](auto& a, auto& b){ return a.size() > b.size(); });
        v.erase(std::unique(v.begin(), v.end()), v.end());
        return v;
    }();

    for (const auto& s : suf) {
        if (ends_with(token, s) && token.size() > s.size() + 4) {
            token.erase(token.size() - s.size());
            break;
        }
    }
    strip_soft_sign(token);
    return token;
}
