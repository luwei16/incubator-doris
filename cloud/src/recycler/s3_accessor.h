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
};

class S3Accessor {
public:
    explicit S3Accessor(S3Conf conf);
    ~S3Accessor();

    // returns 0 for success otherwise error
    int init();

    // returns 0 for success otherwise error
    int delete_objects_by_prefix(const std::string& bucket, const std::string& prefix);

    // returns 0 for success otherwise error
    int delete_objects(const std::string& bucket, const std::vector<std::string>& keys);

    // returns 0 for success otherwise error
    int delete_object(const std::string& bucket, const std::string& key);

    // for test
    // returns 0 for success otherwise error
    int put_object(const std::string& bucket, const std::string& key, const std::string& content);

    // returns 0 for success otherwise error
    int list(const std::string& bucket, const std::string& prefix, std::vector<std::string>* keys);

private:
    std::shared_ptr<Aws::S3::S3Client> s3_client_;
    S3Conf conf_;
};

} // namespace selectdb
