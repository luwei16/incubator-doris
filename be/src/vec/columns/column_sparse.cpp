#include <vec/columns/column_sparse.h>
#include <vec/columns/columns_common.h>

#include <algorithm>
#include <bit>
namespace doris::vectorized {

ColumnSparse::ColumnSparse(MutableColumnPtr&& values_) : values(std::move(values_)), _size(0) {
    if (!values->empty()) {
        LOG(FATAL) << "Not empty values passed to ColumnSparse, but no offsets passed";
    }
    values->insert_default();
    offsets = ColumnUInt64::create();
}
ColumnSparse::ColumnSparse(MutableColumnPtr&& values_, MutableColumnPtr&& offsets_, size_t size_)
        : values(std::move(values_)), offsets(std::move(offsets_)), _size(size_) {
    const ColumnUInt32* offsets_concrete = typeid_cast<const ColumnUInt32*>(offsets.get());
    if (!offsets_concrete)
        LOG(FATAL) << fmt::format("'offsets' column must be a ColumnUInt32, got: {}",
                                  offsets->get_name());
    /// 'values' should contain one extra element: default value at 0 position.
    if (offsets->size() + 1 != values->size())
        LOG(FATAL) << fmt::format("Values size ({}) is inconsistent with offsets size ({})",
                                  values->size(), offsets->size());
    if (_size < offsets->size())
        LOG(FATAL) << fmt::format(
                "Size of sparse column ({}) cannot be lower than number of non-default values ({})",
                _size, offsets->size());
    if (!offsets_concrete->empty() && _size <= offsets_concrete->get_data().back())
        LOG(FATAL) << fmt::format(
                "Size of sparse column ({}) should be greater than last position of non-default "
                "value ({})",
                _size, offsets_concrete->get_data().back());
#ifndef NDEBUG
    const IColumn::Offsets64& offsets_data = get_offsets_data();
    const auto it =
            std::adjacent_find(offsets_data.begin(), offsets_data.end(), std::greater_equal<>());
    if (it != offsets_data.end()) LOG(FATAL) << "Offsets of ColumnSparse must be strictly sorted";
#endif
}
MutableColumnPtr ColumnSparse::clone_resized(size_t new_size) const {
    if (new_size == 0) return ColumnSparse::create(values->clone_empty());
    if (new_size >= _size)
        return ColumnSparse::create(IColumn::mutate(values), IColumn::mutate(offsets), new_size);
    auto res = ColumnSparse::create(values->clone_empty());
    res->insert_range_from(*this, 0, new_size);
    return res;
}
bool ColumnSparse::is_default_at(size_t n) const {
    return get_value_index(n) == 0;
}
bool ColumnSparse::is_null_at(size_t n) const {
    return values->is_null_at(get_value_index(n));
}
Field ColumnSparse::operator[](size_t n) const {
    return (*values)[get_value_index(n)];
}
void ColumnSparse::get(size_t n, Field& res) const {
    values->get(get_value_index(n), res);
}
bool ColumnSparse::get_bool(size_t n) const {
    return values->get_bool(get_value_index(n));
}
Float64 ColumnSparse::get_float64(size_t n) const {
    return values->get_float64(get_value_index(n));
}
UInt64 ColumnSparse::get_uint(size_t n) const {
    return values->get_uint(get_value_index(n));
}
Int64 ColumnSparse::get_int(size_t n) const {
    return values->get_int(get_value_index(n));
}
UInt64 ColumnSparse::get64(size_t n) const {
    return values->get64(get_value_index(n));
}
StringRef ColumnSparse::get_data_at(size_t n) const {
    return values->get_data_at(get_value_index(n));
}
ColumnPtr ColumnSparse::convert_to_full_column_if_sparse() const {
    return values->create_with_offsets(get_offsets_data(), (*values)[0], _size, /*shift=*/1);
}
void ColumnSparse::insert_single_value(const Inserter& inserter) {
    inserter(*values);
    size_t last_idx = values->size() - 1;
    if (values->is_default_at(last_idx))
        values->pop_back(1);
    else
        get_offsets_data().push_back(_size);
    ++_size;
}
void ColumnSparse::insert_data(const char* pos, size_t length) {
    insert_single_value([&](IColumn& column) { column.insert_data(pos, length); });
}
void ColumnSparse::insert_range_from(const IColumn& src, size_t start, size_t length) {
    if (length == 0) return;
    if (start + length > src.size())
        LOG(FATAL) << "Parameter out of bound in IColumnString::insertRangeFrom method.";
    auto& offsets_data = get_offsets_data();
    size_t end = start + length;
    if (const auto* src_sparse = typeid_cast<const ColumnSparse*>(&src)) {
        const auto& src_offsets = src_sparse->get_offsets_data();
        const auto& src_values = src_sparse->get_value_column();
        size_t offset_start = std::lower_bound(src_offsets.begin(), src_offsets.end(), start) -
                              src_offsets.begin();
        size_t offset_end =
                std::lower_bound(src_offsets.begin(), src_offsets.end(), end) - src_offsets.begin();
        assert(offset_start <= offset_end);
        if (offset_start != offset_end) {
            offsets_data.reserve(offsets_data.size() + offset_end - offset_start);
            insert_many_defaults(src_offsets[offset_start] - start);
            offsets_data.push_back(_size);
            ++_size;
            for (size_t i = offset_start + 1; i < offset_end; ++i) {
                size_t current_diff = src_offsets[i] - src_offsets[i - 1];
                insert_many_defaults(current_diff - 1);
                offsets_data.push_back(_size);
                ++_size;
            }
            /// 'end' <= 'src_offsets[offsets_end]', but end is excluded, so index is 'offsets_end' - 1.
            /// Since 'end' is excluded, need to subtract one more row from result.
            insert_many_defaults(end - src_offsets[offset_end - 1] - 1);
            values->insert_range_from(src_values, offset_start + 1, offset_end - offset_start);
        } else {
            insert_many_defaults(length);
        }
    } else {
        for (size_t i = start; i < end; ++i) {
            if (!src.is_default_at(i)) {
                values->insert_from(src, i);
                offsets_data.push_back(_size);
            }
            ++_size;
        }
    }
}
void ColumnSparse::insert(const Field& x) {
    insert_single_value([&](IColumn& column) { column.insert(x); });
}
void ColumnSparse::insert_from(const IColumn& src, size_t n) {
    if (const auto* src_sparse = typeid_cast<const ColumnSparse*>(&src)) {
        if (size_t value_index = src_sparse->get_value_index(n)) {
            get_offsets_data().push_back(_size);
            values->insert_from(src_sparse->get_value_column(), value_index);
        }
    } else {
        if (!src.is_default_at(n)) {
            values->insert_from(src, n);
            get_offsets_data().push_back(_size);
        }
    }
    ++_size;
}
void ColumnSparse::insert_default() {
    ++_size;
}
void ColumnSparse::insert_many_defaults(size_t length) {
    _size += length;
}
void ColumnSparse::pop_back(size_t n) {
    assert(n < _size);
    auto& offsets_data = get_offsets_data();
    size_t new_size = _size - n;
    size_t removed_values = 0;
    while (!offsets_data.empty() && offsets_data.back() >= new_size) {
        offsets_data.pop_back();
        ++removed_values;
    }
    if (removed_values) values->pop_back(removed_values);
    _size = new_size;
}

ColumnPtr ColumnSparse::index(const IColumn& indexes, size_t limit) const {
    return select_index_impl(*this, indexes, limit);
}

template <typename Type>
ColumnPtr ColumnSparse::index_impl(const PaddedPODArray<Type>& indexes, size_t limit) const {
    assert(limit <= indexes.size());
    if (limit == 0) return ColumnSparse::create(values->clone_empty());
    if (offsets->empty()) {
        auto res = clone_empty();
        res->insert_many_defaults(limit);
        return res;
    }
    auto res_offsets = offsets->clone_empty();
    auto& res_offsets_data = assert_cast<ColumnUInt32&>(*res_offsets).get_data();
    auto res_values = values->clone_empty();
    res_values->insert_default();
    auto bit_width = [](const auto& x) {
        return std::numeric_limits<decltype(x)>::digits - std::__countl_zero(x);
    };
    /// If we need to permute full column, or if limit is large enough,
    /// it's better to save indexes of values in O(size)
    /// and avoid binary search for obtaining every index.
    /// 3 is just a guess for overhead on copying indexes.
    bool execute_linear = limit == _size || limit * bit_width(offsets->size()) > _size * 3;
    if (execute_linear) {
        PaddedPODArray<UInt64> values_index(_size);
        auto offset_it = begin();
        for (size_t i = 0; i < _size; ++i, ++offset_it)
            values_index[i] = offset_it.get_value_index();
        for (size_t i = 0; i < limit; ++i) {
            size_t index = values_index[indexes[i]];
            if (index != 0) {
                res_values->insert_from(*values, index);
                res_offsets_data.push_back(i);
            }
        }
    } else {
        for (size_t i = 0; i < limit; ++i) {
            size_t index = get_value_index(indexes[i]);
            if (index != 0) {
                res_values->insert_from(*values, index);
                res_offsets_data.push_back(i);
            }
        }
    }
    return ColumnSparse::create(std::move(res_values), std::move(res_offsets), limit);
}
size_t ColumnSparse::byte_size() const {
    return values->byte_size() + offsets->byte_size() + sizeof(_size);
}
size_t ColumnSparse::allocated_bytes() const {
    return values->allocated_bytes() + offsets->allocated_bytes() + sizeof(_size);
}
void ColumnSparse::protect() {
    values->protect();
    offsets->protect();
}
void ColumnSparse::get_indices_of_non_default_rows(IColumn::Offsets64& indices, size_t from,
                                                   size_t limit) const {
    const auto& offsets_data = get_offsets_data();
    const auto start = from ? std::lower_bound(offsets_data.begin(), offsets_data.end(), from)
                            : offsets_data.begin();
    const auto end =
            limit ? std::lower_bound(offsets_data.begin(), offsets_data.end(), from + limit)
                  : offsets_data.end();
    indices.insert(start, end);
}
const IColumn::Offsets64& ColumnSparse::get_offsets_data() const {
    return assert_cast<const ColumnUInt64&>(*offsets).get_data();
}
IColumn::Offsets64& ColumnSparse::get_offsets_data() {
    return assert_cast<ColumnUInt64&>(*offsets).get_data();
}
size_t ColumnSparse::get_value_index(size_t n) const {
    assert(n < _size);
    const auto& offsets_data = get_offsets_data();
    const auto it = std::lower_bound(offsets_data.begin(), offsets_data.end(), n);
    if (it == offsets_data.end() || *it != n) return 0;
    return it - offsets_data.begin() + 1;
}

} // namespace doris::vectorized
