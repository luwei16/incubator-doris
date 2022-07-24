
// clang-format off
#include "meta-service/codec.h"
#include "common/util.h"

#include <gtest/gtest.h>

#include <cstring>
#include <random>
// clang-format on

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

TEST(CodecTest, StringCodecTest) {
    std::mt19937 gen(std::random_device("/dev/urandom")());
    const int max_len = (2 << 16) + 10086;
    std::uniform_int_distribution<int> rd_len(0, max_len);
    std::uniform_int_distribution<char> rd_char(std::numeric_limits<char>::min(),
                                                std::numeric_limits<char>::max());

    int ret = -1;

    // Correctness test
    {
        int case_count = 50;
        std::string str1;
        std::string str2;
        str1.reserve(max_len);
        str2.reserve(max_len);
        std::string b1;
        std::string b2;
        std::string d1;
        std::string d2;
        b1.reserve(1 + max_len * 2 + 2);
        b2.reserve(1 + max_len * 2 + 2);
        d1.reserve(max_len);
        d2.reserve(max_len);
        while (case_count--) {
            str1.clear();
            str2.clear();
            b1.clear();
            b2.clear();
            d1.clear();
            d2.clear();
            int len1 = rd_len(gen);
            int len2 = rd_len(gen);
            int zero_count1 = 0;
            int zero_count2 = 0;
            while (len1--) {
                str1.push_back(rd_char(gen));
                str1.back() == 0x00 ? ++zero_count1 : zero_count1 += 0;
            }
            while (len2--) {
                str2.push_back(rd_char(gen));
                str2.back() == 0x00 ? ++zero_count2 : zero_count2 += 0;
            }
            selectdb::encode_bytes(str1, &b1);
            selectdb::encode_bytes(str2, &b2);
            int sequence = std::memcmp(&str1[0], &str2[0],
                                       str1.size() > str2.size() ? str2.size() : str1.size());
            int sequence_decoded =
                    std::memcmp(&b1[0], &b2[0], b1.size() > b2.size() ? b2.size() : b1.size());
            ASSERT_TRUE((sequence * sequence_decoded > 0) ||
                        (sequence == 0 && sequence_decoded == 0));
            ASSERT_TRUE(b1[0] == selectdb::EncodingTag::BYTES_TAG);
            ASSERT_TRUE(b2[0] == selectdb::EncodingTag::BYTES_TAG);
            // Check encoded value size, marker + zero_escape + terminator
            ASSERT_TRUE(b1.size() == (str1.size() + 1 + zero_count1 + 2));
            ASSERT_TRUE(b2.size() == (str2.size() + 1 + zero_count2 + 2));

            // Decoding test
            b1 += "selectdb is good";
            b2 += "selectdb will be better";
            std::string_view b1_sv(b1);
            ret = selectdb::decode_bytes(&b1_sv, &d1);
            ASSERT_TRUE(ret == 0);
            ASSERT_TRUE(d1 == str1);
            std::string_view b2_sv(b2);
            ret = selectdb::decode_bytes(&b2_sv, &d2);
            ASSERT_TRUE(ret == 0);
            ASSERT_TRUE(d2 == str2);
            ASSERT_TRUE(b1_sv == "selectdb is good");
            ASSERT_TRUE(b2_sv == "selectdb will be better");
        }
    }

    // Boundary tests
    {
        std::vector<std::string> strs;
        std::vector<std::string> expected;

        int zeroes = 1 * 1024 * 1024;
        strs.emplace_back(zeroes, static_cast<char>(0x00));
        expected.push_back("");
        expected.back().push_back(selectdb::EncodingTag::BYTES_TAG);
        while (zeroes--) {
            expected.back().push_back(selectdb::EncodingTag::BYTE_ESCAPE);
            expected.back().push_back(selectdb::EncodingTag::ESCAPED_00);
        }
        expected.back().push_back(selectdb::EncodingTag::BYTE_ESCAPE);
        expected.back().push_back(selectdb::EncodingTag::BYTES_ENDING);

        ASSERT_TRUE(strs.size() == expected.size());
        for (int i = 0; i < strs.size(); ++i) {
            std::string b1;
            std::string d1;
            std::string_view sv(strs[i]);
            selectdb::encode_bytes(sv, &b1);
            ASSERT_TRUE(b1.size() == expected[i].size());
            for (int j = 0; j < b1.size(); ++j) {
                ASSERT_TRUE(expected[i][j] == b1[j]);
            }
            std::string_view b1_sv(b1);
            ret = selectdb::decode_bytes(&b1_sv, &d1);
            ASSERT_EQ(ret, 0);
            ASSERT_EQ(b1_sv.size(), 0);
            ASSERT_EQ(d1.size(), strs[i].size());
            ASSERT_TRUE(d1 == strs[i]);
        }
    }

    // Other tests
    {
        std::string str1 = "This is";
        std::string str2 = "tHIS IS";
        // Append something strange
        str1.push_back(static_cast<char>(0x00));
        str1 += "an string";
        str1.push_back(static_cast<char>(0xff));
        str2.push_back(static_cast<char>(0x00));
        str2 += "AN STRING";
        str2.push_back(static_cast<unsigned char>(0xff));

        // Output byte array
        std::string b1;
        std::string b2;

        selectdb::encode_bytes(str1, &b1);
        selectdb::encode_bytes(str2, &b2);
        ASSERT_TRUE(std::memcmp(&b1[0], &b2[0], b1.size() > b2.size() ? b2.size() : b1.size()) < 0);

        std::string str11;
        std::string str22;
        std::string_view b1_sv(b1);
        std::string_view b2_sv(b2);
        selectdb::decode_bytes(&b1_sv, &str11);
        selectdb::decode_bytes(&b2_sv, &str22);
        ASSERT_TRUE(str1 == str11);
        ASSERT_TRUE(str2 == str22);
    }
}

TEST(CodecTest, Int64CodecTest) {
    using namespace selectdb;
    int ret = 0;

    // Basic test
    {
        std::string out1;
        selectdb::encode_int64(10086, &out1);
        ASSERT_EQ(out1[0], selectdb::EncodingTag::POSITIVE_FIXED_INT_TAG);
        std::cout << hex(out1) << std::endl;
        int64_t val1 = 10010;
        std::string_view in(out1);
        ret = selectdb::decode_int64(&in, &val1);
        ASSERT_EQ(ret, 0);
        ASSERT_EQ(val1, 10086);

        std::string out2;
        selectdb::encode_int64(-1001011, &out2);
        ASSERT_EQ(out2[0], selectdb::EncodingTag::NEGATIVE_FIXED_INT_TAG);
        std::cout << hex(out2) << std::endl;
        int64_t val2 = 10086;
        in = out2;
        ret = selectdb::decode_int64(&in, &val2);
        ASSERT_EQ(ret, 0);
        ASSERT_EQ(val2, -1001011);

        // Compare lexical order
        ASSERT_LT(out2, out1);
    }
}

// vim: et tw=100 ts=4 sw=4 cc=80:
