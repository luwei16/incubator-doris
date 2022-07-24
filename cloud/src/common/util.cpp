
// clang-format off
#include "util.h"
// FIXME: we should not rely other modules that may rely on this common module
#include "meta-service/keys.h"
#include "meta-service/codec.h"

#include <iomanip>
#include <sstream>
#include <unordered_map>
#include <variant>
// clang-format on

namespace selectdb {

/**
 * This is a naïve implementation of hex, DONOT use it on retical path.
 */
std::string hex(std::string_view str) {
    std::stringstream ss;
    for (auto& i : str) {
        ss << std::hex << std::setw(2) << std::setfill('0') << ((int16_t)i & 0xff);
    }
    return ss.str();
}

/**
 * This is a naïve implementation of unhex.
 */
std::string unhex(std::string_view hex_str) {
    // clang-format off
    const static std::unordered_map<char, char> table = {
            {'0', 0},  {'1', 1},  {'2', 2},  {'3', 3},  {'4', 4}, 
            {'5', 5},  {'6', 6},  {'7', 7},  {'8', 8},  {'9', 9}, 
            {'a', 10}, {'b', 11}, {'c', 12}, {'d', 13}, {'e', 14}, {'f', 15},
            {'A', 10}, {'B', 11}, {'C', 12}, {'D', 13}, {'E', 14}, {'F', 15}};
    // clang-format on
    size_t len = hex_str.length();
    len &= ~0x01UL;
    std::string buf(len >> 1, '\0');
    for (size_t i = 0; i < len; ++i) {
        const auto it = table.find(hex_str[i]);
        if (it == table.end()) break;
        buf[i >> 1] |= i & 0x1 ? (it->second & 0x0f) : (it->second & 0x0f) << 4;
    }
    return buf;
}

static std::string explain_fields(std::string_view text, const std::vector<std::string>& fields,
                                  const std::vector<int>& pos) {
    if (fields.size() != pos.size() || fields.size() == 0 || pos.size() == 0) {
        return std::string(text.data(), text.size());
    }
    size_t last_hyphen_pos = pos.back() + 1;
    std::stringstream ss;
    std::string blank_line;
    for (size_t i = 0; i <= last_hyphen_pos; ++i) {
        ss << ' ';
    }
    blank_line = ss.str();
    ss.str("");

    // Each line with hyphens
    for (size_t i = 0; i < fields.size(); ++i) {
        std::string line = blank_line;
        line[pos[i]] = '/';
        for (size_t j = 0; j < i; ++j) {
            line[pos[j]] = '|';
        }
        for (size_t j = pos[i] + 1; j <= last_hyphen_pos; ++j) {
            line[j] = '-';
        }
        ss << line << " " << i << ". " << fields[i] << std::endl;
    }

    // Mark position indicator
    std::string line = blank_line;
    for (size_t i = 0; i < fields.size(); ++i) {
        line[pos[i]] = '|';
    }
    ss << line << std::endl;
    for (size_t i = 0; i < fields.size(); ++i) {
        line[pos[i]] = 'v';
    }
    ss << line << std::endl;

    // Original text to explain
    ss << text << std::endl;

    return ss.str();
}

std::string prettify_key(std::string_view key_hex) {
    // Decoded result containner
    //                                    val                  tag  pos
    //                     .---------------^----------------.  .^.  .^.
    std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>> fields;
    std::string unhex_key = unhex(key_hex);
    int key_space = unhex_key[0];
    std::string_view key_copy = unhex_key;
    key_copy.remove_prefix(1); // Remove the first key space byte
    int ret = decode_key(&key_copy, &fields);
    if (ret != 0) return "";

    std::vector<std::string> fields_str;
    std::vector<int> fields_pos;
    fields_str.reserve(fields.size() + 1);
    fields_pos.reserve(fields.size() + 1);
    // Key space byte
    fields_str.push_back("key space: " + std::to_string(key_space));
    fields_pos.push_back(0);

    for (auto& i : fields) {
        fields_str.emplace_back(std::get<1>(i) == EncodingTag::BYTES_TAG
                                        ? std::get<std::string>(std::get<0>(i))
                                        : std::to_string(std::get<int64_t>(std::get<0>(i))));
        fields_pos.push_back((std::get<2>(i) + 1) * 2);
    }

    return explain_fields(key_hex, fields_str, fields_pos);
}

} // namespace selectdb
// vim: et tw=100 ts=4 sw=4 cc=80:
