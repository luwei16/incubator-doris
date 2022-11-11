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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/DataTypes/DataTypeObject.cpp
// and modified by Doris

#include <util/string_util.h>
#include <vec/data_types/data_type_object.h>
#include <vec/io/io_helper.h>

#include <vec/data_types/data_type_factory.hpp>

namespace doris::vectorized {

DataTypeObject::DataTypeObject(const String& schema_format_, bool is_nullable_)
        : schema_format(to_lower(schema_format_)), is_nullable(is_nullable_) {}
bool DataTypeObject::equals(const IDataType& rhs) const {
    if (const auto* object = typeid_cast<const DataTypeObject*>(&rhs)) {
        return schema_format == object->schema_format && is_nullable == object->is_nullable;
    }
    return false;
}

int64_t DataTypeObject::get_uncompressed_serialized_bytes(const IColumn& column, int be_exec_version) const {
    LOG(FATAL) << "Method get_uncompressed_serialized_bytes() is not implemented for data type " << get_name();
}

char* DataTypeObject::serialize(const IColumn& column, char* buf, int be_exec_version) const {
    LOG(FATAL) << "Method serialize() is not implemented for data type " << get_name(); 
}

const char* DataTypeObject::deserialize(const char* buf, IColumn* column, int be_exec_version) const {
    LOG(FATAL) << "Method deserialize() is not implemented for data type " << get_name();  
}

} // namespace doris::vectorized
