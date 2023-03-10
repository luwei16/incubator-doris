#include "common/config.h"
#include "common/encryption_util.h"
#include <gtest/gtest.h>

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

TEST(EncryptionTest, EncryptTest) {
    std::string mock_ak = "AKIDOsfsagadsgdfaadgdsgdf";
    std::string mock_sk = "Hx60p12123af234541nsVsffdfsdfghsdfhsdf34t";
    selectdb::config::encryption_method = "AES_256_ECB";
    // "selectdbselectdbselectdbselectdb" -> "c2VsZWN0ZGJzZWxlY3RkYnNlbGVjdGRic2VsZWN0ZGI="
    selectdb::config::encryption_key = "c2VsZWN0ZGJzZWxlY3RkYnNlbGVjdGRic2VsZWN0ZGI=";
    { 
        std::string decoded_text(selectdb::config::encryption_key.length(), '0');
        int decoded_text_len = selectdb::base64_decode(selectdb::config::encryption_key.c_str(), selectdb::config::encryption_key.length(), decoded_text.data()); 
        ASSERT_TRUE(decoded_text_len > 0);
        decoded_text.assign(decoded_text.data(), decoded_text_len);
        std::cout << "decoded_string: " << decoded_text << std::endl;
        ASSERT_TRUE(decoded_text == "selectdbselectdbselectdbselectdb");
        int ret;
        selectdb::AkSkPair cipher_ak_sk_pair;
        ret = selectdb::encrypt_ak_sk({mock_ak, mock_sk}, selectdb::config::encryption_method, decoded_text, &cipher_ak_sk_pair);
        ASSERT_TRUE(ret == 0);
        std::cout << "cipher ak: " << cipher_ak_sk_pair.first << std::endl;
        std::cout << "cipher sk: " << cipher_ak_sk_pair.second << std::endl;
        selectdb::AkSkPair plain_ak_sk_pair;
        ret = selectdb::decrypt_ak_sk(cipher_ak_sk_pair, selectdb::config::encryption_method, decoded_text, &plain_ak_sk_pair);
        ASSERT_TRUE(ret == 0);
        std::cout << "plain ak: " << plain_ak_sk_pair.first << std::endl;
        std::cout << "plain sk: " << plain_ak_sk_pair.second << std::endl;
        ASSERT_TRUE(mock_ak == plain_ak_sk_pair.first);
        ASSERT_TRUE(mock_sk == plain_ak_sk_pair.second);
    }

     
}