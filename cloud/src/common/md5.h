#pragma once

#include <openssl/md5.h>
#include <stddef.h>

#include <string>
namespace selectdb {

class Md5Digest {
public:
    Md5Digest() { MD5_Init(&_md5_ctx); }

    void update(const void* data, size_t length) { MD5_Update(&_md5_ctx, data, length); }

    void digest() {
        unsigned char buf[MD5_DIGEST_LENGTH];
        MD5_Final(buf, &_md5_ctx);

        char hex_buf[2 * MD5_DIGEST_LENGTH];

        static char dig_vec_lower[] = "0123456789abcdef";
        char* to = hex_buf;
        for (int i = 0; i < MD5_DIGEST_LENGTH; ++i) {
            *to++ = dig_vec_lower[buf[i] >> 4];
            *to++ = dig_vec_lower[buf[i] & 0x0F];
        }
        _hex.assign(hex_buf, 2 * MD5_DIGEST_LENGTH);
    }

    const std::string& hex() const { return _hex; }

private:
    MD5_CTX _md5_ctx;
    std::string _hex;
};
} // namespace selectdb
