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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Columns/IColumn.cpp
// and modified by Doris

#include "vec/columns/column.h"

#include <sstream>

#include "vec/columns/column_const.h"
#include "vec/columns/column_nullable.h"
#include "vec/core/field.h"

namespace doris::vectorized {

std::string IColumn::dump_structure() const {
    std::stringstream res;
    res << get_family_name() << "(size = " << size();

    ColumnCallback callback = [&](ColumnPtr& subcolumn) {
        res << ", " << subcolumn->dump_structure();
    };

    const_cast<IColumn*>(this)->for_each_subcolumn(callback);

    res << ")";
    return res.str();
}

void IColumn::insert_from(const IColumn& src, size_t n) {
    insert(src[n]);
}

bool is_column_nullable(const IColumn& column) {
    return check_column<ColumnNullable>(column);
}

bool is_column_const(const IColumn& column) {
    return check_column<ColumnConst>(column);
}

ColumnPtr IColumn::create_with_offsets(const Offsets& offsets, const Field& default_field,
                                       size_t total_rows, size_t shift) const {
    if (offsets.size() + shift != size()) {
        LOG(FATAL) << fmt::format(
                "Incompatible sizes of offsets ({}), shift ({}) and size of column {}",
                offsets.size(), shift, size());
    }
    auto res = clone_empty();
    res->reserve(total_rows);
    ssize_t current_offset = -1;
    for (size_t i = 0; i < offsets.size(); ++i) {
        ssize_t offsets_diff = static_cast<ssize_t>(offsets[i]) - current_offset;
        current_offset = offsets[i];
        if (offsets_diff > 1) {
            res->insert_many(default_field, offsets_diff - 1);
        }
        res->insert_from(*this, i + shift);
    }
    ssize_t offsets_diff = static_cast<ssize_t>(total_rows) - current_offset;
    if (offsets_diff > 1) {
        res->insert_many(default_field, offsets_diff - 1);
    }
    return res;
}

} // namespace doris::vectorized
