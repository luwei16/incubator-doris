#include "io/cloud/tmp_file_mgr.h"

#include <rapidjson/document.h>

namespace doris::io {

static std::string TMP_FILE_DIR_PATH = "path";
static std::string MAX_CACHE_BYTES = "max_cache_bytes";
static std::string MAX_UPLOAD_BYTES = "max_upload_bytes";

Status TmpFileMgr::create_tmp_file_mgrs() {
    if (config::tmp_file_dirs.empty()) {
        LOG(ERROR) << "The config tmp_file_dirs is empty";
        return Status::OLAPInternalError(OLAP_ERR_INPUT_PARAMETER_ERROR);
    }
    using namespace rapidjson;
    Document document;
    document.Parse(config::tmp_file_dirs.c_str());
    if (!document.IsArray()) {
        LOG(ERROR) << "The config tmp_file_dirs need to be array";
        return Status::OLAPInternalError(OLAP_ERR_INPUT_PARAMETER_ERROR);
    }
    std::vector<TmpFileDirConfig> configs;
    for (auto& config : document.GetArray()) {
        TmpFileDirConfig tmp_file_mgr_config;
        auto map = config.GetObject();
        if (!map.HasMember(TMP_FILE_DIR_PATH.c_str())) {
            LOG(ERROR) << "The config doesn't have member 'path' ";
            return Status::OLAPInternalError(OLAP_ERR_INPUT_PARAMETER_ERROR);
        }
        tmp_file_mgr_config.path = map.FindMember(TMP_FILE_DIR_PATH.c_str())->value.GetString();
        tmp_file_mgr_config.max_cache_bytes =
                map.HasMember(MAX_CACHE_BYTES.c_str())
                        ? map.FindMember(MAX_CACHE_BYTES.c_str())->value.GetInt64()
                        : 0;
        tmp_file_mgr_config.max_upload_bytes =
                map.HasMember(MAX_UPLOAD_BYTES.c_str())
                        ? map.FindMember(MAX_UPLOAD_BYTES.c_str())->value.GetInt64()
                        : 0;
        if (tmp_file_mgr_config.max_cache_bytes < 0 || tmp_file_mgr_config.max_upload_bytes < 0) {
            LOG(WARNING) << "max_cache_bytes or max_upload_bytes size should not less than or "
                            "equal to zero";
            return Status::OLAPInternalError(OLAP_ERR_INPUT_PARAMETER_ERROR);
        }
        configs.push_back(tmp_file_mgr_config);
    }
    static TmpFileMgr factory {configs};
    _s_instance = &factory;
    return Status::OK();
}

FileReaderSPtr TmpFileMgr::lookup_tmp_file(const Path& path) {
    auto& tmp_file_dir = _tmp_file_dirs[std::hash<std::string>()(path.filename().native()) %
                                        _tmp_file_dirs_size];
    if (tmp_file_dir.max_cache_bytes == 0) {
        return nullptr;
    }
    {
        std::lock_guard lock(tmp_file_dir.mtx);
        if (tmp_file_dir.file_set.count(path) == 0) {
            return nullptr;
        }
    }
    FileReaderSPtr file_reader;
    auto st = global_local_filesystem()->open_file(path, &file_reader);
    if (!st.ok()) {
        LOG(WARNING) << "could not open tmp file. err=" << st;
        return nullptr;
    }
    return file_reader;
}

bool TmpFileMgr::insert_tmp_file(const Path& path, size_t file_size) {
    auto& tmp_file_dir = _tmp_file_dirs[std::hash<std::string>()(path.filename().native()) %
                                        _tmp_file_dirs_size];
    if (tmp_file_dir.max_cache_bytes == 0) {
        return false;
    }
    auto local_fs = global_local_filesystem();
    std::vector<Path> remove_paths;
    {
        std::lock_guard lock(tmp_file_dir.mtx);
        tmp_file_dir.cur_cache_bytes += file_size;
        while (tmp_file_dir.cur_cache_bytes > tmp_file_dir.max_cache_bytes) {
            auto& [remove_path, size] = tmp_file_dir.file_list.back();
            tmp_file_dir.file_set.erase(remove_path);
            tmp_file_dir.cur_cache_bytes -= size;
            remove_paths.push_back(std::move(remove_path));
            tmp_file_dir.file_list.pop_back();
        }
        tmp_file_dir.file_list.push_front(std::make_pair(path, file_size));
        tmp_file_dir.file_set.insert(path);
    }
    for (auto& remove_path : remove_paths) {
        auto st = local_fs->delete_file(remove_path);
        if (!st.ok()) {
            LOG(WARNING) << "could not remove tmp file. err=" << st;
        }
    }
    return true;
}

} // namespace doris::io
