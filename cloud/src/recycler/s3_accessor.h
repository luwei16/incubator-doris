#pragma once

#include <memory>
#include <string>
#include <vector>

namespace Aws::S3 {
class S3Client;
} // namespace Aws::S3

namespace selectdb {

struct S3Conf {
    std::string ak;
    std::string sk;
    std::string endpoint;
    std::string region;
    std::string bucket;
    std::string prefix;
};

class S3Accessor {
public:
    explicit S3Accessor(S3Conf conf);
    ~S3Accessor();

    const std::string& path() const { return path_; }

    // returns 0 for success otherwise error
    int init();

    // returns 0 for success otherwise error
    int delete_objects_by_prefix(const std::string& relative_path);

    // returns 0 for success otherwise error
    int delete_objects(const std::vector<std::string>& relative_paths);

    // returns 0 for success otherwise error
    int delete_object(const std::string& relative_path);

    // for test
    // returns 0 for success otherwise error
    int put_object(const std::string& relative_path, const std::string& content);

    // returns 0 for success otherwise error
    int list(const std::string& relative_path, std::vector<std::string>* keys);

private:
    std::string get_key(const std::string& relative_path) const;

private:
    std::shared_ptr<Aws::S3::S3Client> s3_client_;
    S3Conf conf_;
    std::string path_;
};

} // namespace selectdb
