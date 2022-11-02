//  Licensed to the Apache Software Foundation (ASF) under one
//  or more contributor license agreements.  See the NOTICE file
//  distributed with this work for additional information
//  regarding copyright ownership.  The ASF licenses this file
//  to you under the Apache License, Version 2.0 (the
//  "License"); you may not use this file except in compliance
//  with the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an
//  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
//  KIND, either express or implied.  See the License for the
//  specific language governing permissions and limitations
//  under the License.

#include "range_predicate.h"

#include <memory>
#include <sstream>

#include "exec/olap_utils.h"
#include "olap/schema.h"
#include "runtime/string_value.hpp"

namespace doris {

template <class T>
RangePredicate<T>::RangePredicate(uint32_t column_id)
        : ColumnPredicate(column_id){
    _predicate_params = std::make_shared<RangePredicateParams<T>>();
}

template <class T>
void RangePredicate<T>::set_range_params(ColumnPredicate* pred, T value) {
    RangeParams<T> params = {value, pred->type(), pred};
    if (pred->type() == PredicateType::LE || pred->type() == PredicateType::LT) {
        _predicate_params->_upper_value = params;
    } else if (pred->type() == PredicateType::GE || pred->type() == PredicateType::GT) {
        _predicate_params->_lower_value = params;
    }
}

template <class T>
Status RangePredicate<T>::evaluate(const Schema& schema, InvertedIndexIterator* iterator,
                                   uint32_t num_rows,
                                   roaring::Roaring* bitmap) const {
    if (iterator == nullptr) {
        return Status::OK();
    }
    auto column_desc = schema.column(_column_id);
    std::string column_name = column_desc->name();
    roaring::Roaring roaring;
    InvertedIndexQueryType query_type;
    auto lower_value = _predicate_params->_lower_value;
    auto upper_value = _predicate_params->_upper_value;
    if (lower_value._type == PredicateType::GT && upper_value._type == PredicateType::LT) {
        query_type = InvertedIndexQueryType::RANGE_QUERY;
    } else if (lower_value._type == PredicateType::GT && upper_value._type == PredicateType::LE) {
        query_type = InvertedIndexQueryType::RANGE_LESS_EQUAL_QUERY;
    } else if (lower_value._type == PredicateType::GE && upper_value._type == PredicateType::LT) {
        query_type = InvertedIndexQueryType::RANGE_GREATER_EQUAL_QUERY;
    } else {
        query_type = InvertedIndexQueryType::RANGE_LESS_GREATER_EQUAL_QUERY;
    }
    RETURN_IF_ERROR(
            iterator->read_from_inverted_index(column_name,
                                               &lower_value._value,
                                               query_type,
                                               num_rows,
                                               &roaring,
                                               &upper_value._value));

    *bitmap &= roaring;
    return Status::OK();
}

template <class T>
PredicateType RangePredicate<T>::type() const {
    return PredicateType::RANGE;
}
} // namespace doris