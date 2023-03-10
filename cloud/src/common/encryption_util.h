#pragma once

#include <gen_cpp/selectdb_cloud.pb.h>
#include <stdint.h>
#include <cstdint>
#include <string>
#include "common/config.h"
#include "common/util.h"
#include "meta-service/keys.h"
#include "meta-service/txn_kv.h"


namespace selectdb {

using AkSkPair = std::pair<std::string, std::string>;
/**
 * @brief Encrypt ak/sk pair
 *
 * @param plain_ak_sk plain ak/sk pair
 * @param encryption_method encryption_method
 * @param encryption_key encryptionn_key
 * @param cipher_ak_sk output cipher ak/sk pair in base64 format
 * @return 0 for success, negative for error
 */
int encrypt_ak_sk(const AkSkPair& plain_ak_sk, const std::string& encryption_method, const std::string& encryption_key, AkSkPair* cipher_ak_sk);

/**
 * @brief Decrypt ak/sk pair
 * 
 * @param cipher_ak_sk cipher ak/sk pair in base64 format
 * @param encryption_method encryption_method
 * @param encryption_key encryption_key
 * @param plain_ak_sk output plain ak/sk pair
 * @return 0 for success, negative for error
 */
int decrypt_ak_sk(const AkSkPair& cipher_ak_sk, const std::string& encryption_method, const std::string& encryption_key, AkSkPair* plain_ak_sk);

extern std::map<int64_t, std::string> global_encryption_key_info_map;


// Todo: Should we need to refresh it
int init_global_encryption_key_info_map(std::shared_ptr<TxnKv> txn_kv);

/**
 * @brief Get the encryption key for ak sk by key_id
 * 
 * @param version_id 
 * @param encryption_key output encryption_key 
 * @return 0 for success, negative for error 
 */
inline static int get_encryption_key_for_ak_sk(int64_t key_id, std::string* encryption_key) {
    if (global_encryption_key_info_map.count(key_id)) {
        *encryption_key = global_encryption_key_info_map.at(key_id);
        return 0;
    }
    return -1;
}

/**
 * @brief Get the newest encryption key for ak sk
 * 
 * @param key_id
 * @param encryption_key 
 * @return 0 for success, negative for error  
 */
inline static int get_newest_encryption_key_for_ak_sk(int64_t* key_id, std::string* encryption_key) {
    if (global_encryption_key_info_map.empty()) {
        return -1;
    }
    auto it = global_encryption_key_info_map.crbegin();
    *key_id = it->first;
    *encryption_key = it->second;
    return 0;
}

inline static std::string get_encryption_method_for_ak_sk() {
    return config::encryption_method;
}

size_t base64_decode(const char* data, size_t length, char* decoded_data);

} // namespace selectdb
