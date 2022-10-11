#include "olap/rowset/segment_v2/inverted_index_desc.h"
#include "gutil/strings/strip.h"

namespace doris::segment_v2 {
const std::string segment_suffix = ".dat";
const std::string index_suffix = ".idx";
const std::string index_name_separator = "_";

std::string InvertedIndexDescriptor::get_temporary_index_path(const std::string& segment_path,
                                                              uint32_t uuid) {
    return StripSuffixString(segment_path, segment_suffix) + index_name_separator + std::to_string(uuid);
}

std::string InvertedIndexDescriptor::get_index_file_name(const std::string& segment_path,
                                                         uint32_t uuid) {
    return StripSuffixString(segment_path, segment_suffix)
           + index_name_separator + std::to_string(uuid) + index_suffix;
}
} // namespace doris::segment_v2