#include "inverted_index_compaction.h"

#include <CLucene.h>

#include "inverted_index_compound_directory.h"
#include "inverted_index_compound_reader.h"

namespace doris {
namespace segment_v2 {
void compact_column(int32_t column_id, int src_segment_num, int dest_segment_num,
                    std::vector<std::string> src_index_files,
                    std::vector<std::string> dest_index_files, io::FileSystem* fs,
                    std::string index_writer_path, std::string tablet_path,
                    std::vector<std::vector<std::pair<uint32_t, uint32_t>>> trans_vec,
                    std::vector<uint32_t> dest_segment_num_rows) {
    lucene::store::Directory* dir =
            DorisCompoundDirectory::getDirectory(fs, index_writer_path.c_str(), false);
    lucene::index::IndexWriter* index_writer = _CLNEW lucene::index::IndexWriter(
            dir, nullptr, true /* create */, true /* closeDirOnShutdown */);
    // NOTE: need to ref_cnt-- for dir,
    // when index_writer is destroyed, if closeDir is set, dir will be close
    // _CLDECDELETE(dir) will try to ref_cnt--, when it decreases to 1, dir will be destroyed.
    _CLLDECDELETE(dir)

    // get compound directory src_index_dirs
    std::vector<lucene::store::Directory*> src_index_dirs(src_segment_num);
    for (int i = 0; i < src_segment_num; ++i) {
        // format: rowsetId_segmentId_columnId.idx
        std::string src_idx_full_name =
                src_index_files[i] + "_" + std::to_string(column_id) + ".idx";
        DorisCompoundReader* reader = new DorisCompoundReader(
                DorisCompoundDirectory::getDirectory(fs, tablet_path.c_str(), true),
                src_idx_full_name.c_str());
        src_index_dirs[i] = reader;
    }

    // get dest idx file paths
    std::vector<lucene::store::Directory*> dest_index_dirs(dest_segment_num);
    for (int i = 0; i < dest_segment_num; ++i) {
        // format: rowsetId_segmentId_columnId
        auto path = tablet_path + "/" + dest_index_files[i] + "_" + std::to_string(column_id);
        dest_index_dirs[i] = DorisCompoundDirectory::getDirectory(fs, path.c_str(), true);
    }

    index_writer->indexCompaction(src_index_dirs, dest_index_dirs, trans_vec,
                                  dest_segment_num_rows);

    index_writer->close();
    _CLDELETE(index_writer);
    index_writer = nullptr;
    for (auto d : src_index_dirs) {
        if (d != nullptr) {
            d->close();
            _CLDELETE(d);
        }
    }
    for (auto d : dest_index_dirs) {
        if (d != nullptr) {
            d->close();
            _CLDELETE(d);
        }
    }

    // delete temporary index_writer_path
    fs->delete_directory(index_writer_path.c_str());
}
} // namespace segment_v2
} // namespace doris