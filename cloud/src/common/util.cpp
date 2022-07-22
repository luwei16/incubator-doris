
// clang-format off
#include "util.h"

#include <iomanip>
#include <sstream>
// clang-format on

std::string hex(std::string_view str) {
    std::stringstream ss;
    for (auto& i : str) {
        ss << std::hex << std::setw(2) << std::setfill('0') << ((int16_t)i & 0xff);
    }
    return ss.str();
}
// vim: et tw=100 ts=4 sw=4 cc=80:
