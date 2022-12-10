#pragma once

#include <vector>

#include "cloud/io/file_system.h"

namespace doris {

namespace segment_v2 {
void compact_column(int32_t index_id, int src_segment_num, int dest_segment_num,
                    std::vector<std::string> src_index_files,
                    std::vector<std::string> dest_index_files, const io::FileSystemSPtr& fs,
                    std::string index_writer_path, std::string tablet_path,
                    std::vector<std::vector<std::pair<uint32_t, uint32_t>>> trans_vec,
                    std::vector<uint32_t> dest_segment_num_rows);
} // namespace segment_v2
} // namespace doris
