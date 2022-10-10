// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "olap/inverted_index_parser.h"
#include "util/string_util.h"

namespace doris {

std::string inverted_index_parser_type_to_string(InvertedIndexParserType parser_type) {
    switch (parser_type)
    {
    case PARSER_NOT_SET:
        return "not_set";
    case PARSER_NONE:
        return "none";
    case PARSER_STANDARD:
        return "standard";
    case PARSER_ENGLISH:
        return "english";
    case PARSER_CHINESE:
        return "chinese";
    default:
        return "unknown";
    }

    return "unknown";
}

InvertedIndexParserType get_inverted_index_parser_type_from_string(const std::string& parser_str) {
    auto parser_str_lower = to_lower(parser_str);
    if (parser_str_lower == "not_set") {
        return PARSER_NOT_SET;
    } else if (parser_str_lower == "none") {
        return PARSER_NONE;
    } else if (parser_str_lower == "standard") {
        return PARSER_STANDARD;
    } else if (parser_str_lower == "english") {
        return PARSER_ENGLISH;
    } else if (parser_str_lower == "chinese") {
        return PARSER_CHINESE;
    }

    return PARSER_UNKNOWN;
}

} // namespace doris
