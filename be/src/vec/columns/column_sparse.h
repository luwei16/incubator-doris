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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Columns/ColumnSparse.h
// and modified by Doris

#pragma once

#include <vec/columns/column.h>
#include <vec/columns/column_impl.h>
#include <vec/columns/columns_number.h>
#include <vec/common/assert_cast.h>
#include <vec/common/typeid_cast.h>

namespace doris::vectorized {
/** Column for spare representation.
 *  It stores column with non-default values and column
 *  with their sorted positions in original column. Column with
 *  values contains also one default value at 0 position to make
 *  implementation of execution of functions and sorting more convenient.
 */

class ColumnSparse final : public COWHelper<IColumn, ColumnSparse> {
private:
    friend class COWHelper<IColumn, ColumnSparse>;
    explicit ColumnSparse(MutableColumnPtr&& values_);
    ColumnSparse(MutableColumnPtr&& values_, MutableColumnPtr&& offsets_, size_t size_);
    ColumnSparse(const ColumnSparse&) = default;

public:
    static constexpr auto DEFAULT_ROWS_SEARCH_SAMPLE_RATIO = 0.1;
    static constexpr auto DEFAULT_RATIO_FOR_SPARSE_SERIALIZATION = 0.95;
    using Base = COWHelper<IColumn, ColumnSparse>;
    static Ptr create(const ColumnPtr& values_, const ColumnPtr& offsets_, size_t size_) {
        return Base::create(values_->assume_mutable(), offsets_->assume_mutable(), size_);
    }
    static Ptr create(const ColumnPtr& values_) { return Base::create(values_->assume_mutable()); }
    template <typename TColumnPtr,
              typename = std::enable_if_t<IsMutableColumns<TColumnPtr>::value> >
    static MutablePtr create(TColumnPtr&& values_, TColumnPtr&& offsets_, size_t size_) {
        return Base::create(std::forward<TColumnPtr>(values_), std::forward<TColumnPtr>(offsets_),
                            size_);
    }
    template <typename TColumnPtr,
              typename = std::enable_if_t<IsMutableColumns<TColumnPtr>::value> >
    static MutablePtr create(TColumnPtr&& values_) {
        return Base::create(std::forward<TColumnPtr>(values_));
    }
    ColumnPtr index(const IColumn& indexes, size_t limit) const override;
    template <typename Type>
    ColumnPtr index_impl(const PaddedPODArray<Type>& indexes, size_t limit) const;
    bool is_sparse() const override { return true; }
    const char* get_family_name() const override { return "Sparse"; }
    std::string get_name() const override { return "Sparse(" + values->get_name() + ")"; }
    TypeIndex get_data_type() const override { return values->get_data_type(); }
    MutableColumnPtr clone_resized(size_t new_size) const override;
    size_t size() const override { return _size; }
    bool is_default_at(size_t n) const override;
    bool is_null_at(size_t n) const override;
    Field operator[](size_t n) const override;
    void get(size_t n, Field& res) const override;
    bool get_bool(size_t n) const override;
    Float64 get_float64(size_t n) const override;
    UInt64 get_uint(size_t n) const override;
    Int64 get_int(size_t n) const override;
    UInt64 get64(size_t n) const override;
    StringRef get_data_at(size_t n) const override;
    ColumnPtr convert_to_full_column_if_sparse() const override;
    /// Will insert null value if pos=nullptr
    void insert_data(const char* pos, size_t length) override;
    void insert_range_from(const IColumn& src, size_t start, size_t length) override;
    void insert(const Field& x) override;
    void insert_from(const IColumn& src, size_t n) override;
    void insert_default() override;
    void insert_many_defaults(size_t length) override;
    void pop_back(size_t n) override;
    size_t byte_size() const override;
    size_t allocated_bytes() const override;
    void protect() override;
    /// Returns indices of values in column, that not equal to default value of column.
    void get_indices_of_non_default_rows(Offsets& indices, size_t from,
                                         size_t limit) const override;
    /// Return position of element in 'values' columns,
    /// that corresponds to n-th element of full column.
    /// O(log(offsets.size())) complexity,
    size_t get_value_index(size_t n) const;
    const IColumn& get_value_column() const { return *values; }
    IColumn& get_value_column() { return *values; }
    const ColumnPtr& get_values_ptr() const { return values; }
    ColumnPtr& get_values_ptr() { return values; }
    const IColumn::Offsets& get_offsets_data() const;
    IColumn::Offsets& get_offsets_data();
    const ColumnPtr& get_offsets_ptr() const { return offsets; }
    ColumnPtr& get_offsets_ptr() { return offsets; }
    const IColumn& get_offsets_column() const { return *offsets; }
    IColumn& get_offsets_column() { return *offsets; }

    /// This class helps to iterate over all values in ColumnSparse.
    class Iterator {
    public:
        Iterator(const PaddedPODArray<UInt64>& offsets_, size_t size_, size_t current_offset_,
                 size_t current_row_)
                : offsets(offsets_),
                  size(size_),
                  current_offset(current_offset_),
                  current_row(current_row_) {}
        bool ALWAYS_INLINE is_default() const {
            return current_offset == offsets.size() || current_row != offsets[current_offset];
        }
        size_t ALWAYS_INLINE get_value_index() const {
            return is_default() ? 0 : current_offset + 1;
        }
        size_t ALWAYS_INLINE get_current_row() const { return current_row; }
        size_t ALWAYS_INLINE get_current_offset() const { return current_offset; }
        bool operator==(const Iterator& other) const {
            return size == other.size && current_offset == other.current_offset &&
                   current_row == other.current_row;
        }
        bool operator!=(const Iterator& other) const { return !(*this == other); }
        Iterator operator++() {
            if (!is_default()) ++current_offset;
            ++current_row;
            return *this;
        }

    private:
        const PaddedPODArray<UInt64>& offsets;
        const size_t size;
        size_t current_offset;
        size_t current_row;
    };
    Iterator begin() const { return Iterator(get_offsets_data(), _size, 0, 0); }
    Iterator end() const {
        return Iterator(get_offsets_data(), _size, get_offsets_data().size(), _size);
    }

    StringRef serialize_value_into_arena(size_t n, Arena& arena,
                                         char const*& begin) const override {
        LOG(FATAL) << "should not call the method in column object";
        return StringRef();
    }

    const char* deserialize_and_insert_from_arena(const char* pos) override {
        LOG(FATAL) << "should not call the method in column object";
        return nullptr;
    }

    void update_hash_with_value(size_t n, SipHash& hash) const override {
        LOG(FATAL) << "should not call the method in column object";
    }

    ColumnPtr filter(const Filter&, ssize_t) const override {
        LOG(FATAL) << "should not call the method in column object";
        return nullptr;
    }

    ColumnPtr permute(const Permutation&, size_t) const override {
        LOG(FATAL) << "should not call the method in column object";
        return nullptr;
    }

    int compare_at(size_t n, size_t m, const IColumn& rhs, int nan_direction_hint) const override {
        LOG(FATAL) << "should not call the method in column object";
        return 0;
    }

    void get_permutation(bool reverse, size_t limit, int nan_direction_hint,
                         Permutation& res) const override {
        LOG(FATAL) << "should not call the method in column object";
    }

    MutableColumns scatter(ColumnIndex, const Selector&) const override {
        LOG(FATAL) << "should not call the method in column object";
        return {};
    }

    void replace_column_data(const IColumn&, size_t row, size_t self_row) override {
        LOG(FATAL) << "should not call the method in column object";
    }

    void replace_column_data_default(size_t self_row) override {
        LOG(FATAL) << "should not call the method in column object";
    }

    virtual void get_extremes(Field& min, Field& max) const override {
        LOG(FATAL) << "should not call the method in column object";
    }

    [[noreturn]] ColumnPtr replicate(const Offsets& offsets) const override {
        LOG(FATAL) << "should not call the method replicate in column object";
    }

    void insert_indices_from(const IColumn& src, const int* indices_begin,
                             const int* indices_end) override {
        LOG(FATAL) << "should not call the method in column object";
    }

private:
    using Inserter = std::function<void(IColumn&)>;
    /// Inserts value to 'values' column via callback.
    /// Properly handles cases, when inserted value is default.
    /// Used, when it's unknown in advance if inserted value is default.
    void insert_single_value(const Inserter& inserter);
    /// Contains default value at 0 position.
    /// It's convenient, because it allows to execute, e.g functions or sorting,
    /// for this column without handling different cases.
    WrappedPtr values;
    /// Sorted offsets of non-default values in the full column.
    /// 'offsets[i]' corresponds to 'values[i + 1]'.
    WrappedPtr offsets;
    size_t _size; /// NOLINT
};

ColumnPtr recursiveRemoveSparse(const ColumnPtr& column);
} // namespace doris::vectorized
