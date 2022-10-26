#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <memory>
#include <random>
#include <roaring/roaring.hh>

namespace doris::cloud {

// A fast range iterator for roaring bitmap. Output ranges use closed-open form, like [from, to).
// Example:
//   input bitmap:  [0 1 4 5 6 7 10 15 16 17 18 19]
//   output ranges: [0,2), [4,8), [10,11), [15,20) (when max_range_size=10)
//   output ranges: [0,2), [4,7), [7,8), [10,11), [15,18), [18,20) (when max_range_size=3)
class BitmapRangeIterator {
public:
    BitmapRangeIterator() = default;
    virtual ~BitmapRangeIterator() = default;

    explicit BitmapRangeIterator(const roaring::Roaring& bitmap) {
        roaring_init_iterator(&bitmap.roaring, &_iter);
        _read_next_batch();
    }

    bool has_more_range() const { return !_eof; }

    std::vector<std::pair<uint32_t, uint32_t>> get_all_contiguous_ranges() {
        std::vector<std::pair<uint32_t, uint32_t>> ranges;
        std::pair<uint32_t, uint32_t> range;
        while (next_contiguous_range(&range.first, &range.second)) {
            ranges.push_back(range);
        }
        return ranges;
    }

    virtual bool next_contiguous_range(uint32_t* from, uint32_t* to) {
        if (_eof) {
            return false;
        }

        *from = _buf[_buf_pos];
        uint32_t range_size = 0;
        uint32_t expect_val = _buf[_buf_pos]; // this initial value just make first batch valid

        // if array is contiguous sequence then the following conditions need to be met :
        // a_0: x
        // a_1: x+1
        // a_2: x+2
        // ...
        // a_p: x+p
        // so we can just use (a_p-a_0)-p to check conditions
        // and should notice the previous batch needs to be continuous with the current batch
        while (!_eof && expect_val == _buf[_buf_pos] &&
               _buf[_buf_size - 1] - _buf[_buf_pos] == _buf_size - 1 - _buf_pos) {
            range_size += _buf_size - _buf_pos;
            expect_val = _buf[_buf_size - 1] + 1;
            _read_next_batch();
        }

        // promise remain range not will reach next batch
        if (!_eof && expect_val == _buf[_buf_pos]) {
            do {
                _buf_pos++;
                range_size++;
            } while (_buf[_buf_pos] == _buf[_buf_pos - 1] + 1);
        }
        *to = *from + range_size;
        return true;
    }

    // read next range into [*from, *to) whose size <= max_range_size.
    // return false when there is no more range.
    virtual bool next_range(const uint32_t max_range_size, uint32_t* from, uint32_t* to) {
        if (_eof) {
            return false;
        }

        *from = _buf[_buf_pos];
        uint32_t range_size = 0;
        uint32_t expect_val = _buf[_buf_pos]; // this initial value just make first batch valid

        // if array is contiguous sequence then the following conditions need to be met :
        // a_0: x
        // a_1: x+1
        // a_2: x+2
        // ...
        // a_p: x+p
        // so we can just use (a_p-a_0)-p to check conditions
        // and should notice the previous batch needs to be continuous with the current batch
        while (!_eof && range_size + _buf_size - _buf_pos <= max_range_size &&
               expect_val == _buf[_buf_pos] &&
               _buf[_buf_size - 1] - _buf[_buf_pos] == _buf_size - 1 - _buf_pos) {
            range_size += _buf_size - _buf_pos;
            expect_val = _buf[_buf_size - 1] + 1;
            _read_next_batch();
        }

        // promise remain range not will reach next batch
        if (!_eof && range_size < max_range_size && expect_val == _buf[_buf_pos]) {
            do {
                _buf_pos++;
                range_size++;
            } while (range_size < max_range_size && _buf[_buf_pos] == _buf[_buf_pos - 1] + 1);
        }
        *to = *from + range_size;
        return true;
    }

private:
    void _read_next_batch() {
        _buf_pos = 0;
        _buf_size = roaring::api::roaring_read_uint32_iterator(&_iter, _buf, kBatchSize);
        _eof = (_buf_size == 0);
    }

    static const uint32_t kBatchSize = 256;
    roaring::api::roaring_uint32_iterator_t _iter;
    uint32_t _buf[kBatchSize];
    uint32_t _buf_pos = 0;
    uint32_t _buf_size = 0;
    bool _eof = false;
};

// A backward range iterator for roaring bitmap. Output ranges use closed-open form, like [from, to).
// Example:
//   input bitmap:  [0 1 4 5 6 7 10 15 16 17 18 19]
//   output ranges: , [15,20), [10,11), [4,8), [0,2) (when max_range_size=10)
//   output ranges: [17,20), [15,17), [10,11), [5,8), [4, 5), [0,2) (when max_range_size=3)
class BackwardBitmapRangeIterator : public BitmapRangeIterator {
public:
    explicit BackwardBitmapRangeIterator(const roaring::Roaring& bitmap) {
        roaring_init_iterator_last(&bitmap.roaring, &_riter);
    }

    bool has_more_range() const { return !_riter.has_value; }

    bool next_contiguous_range(uint32_t* from, uint32_t* to) override {
        if (!_riter.has_value) {
            return false;
        }

        *to = _riter.current_value + 1;

        do {
            *from = _riter.current_value;
            roaring_previous_uint32_iterator(&_riter);
        } while (_riter.has_value && _riter.current_value + 1 == *from);

        return true;
    }

    // read next range into [*from, *to) whose size <= max_range_size.
    // return false when there is no more range.
    bool next_range(const uint32_t max_range_size, uint32_t* from, uint32_t* to) override {
        if (!_riter.has_value) {
            return false;
        }

        uint32_t range_size = 0;
        *to = _riter.current_value + 1;

        do {
            *from = _riter.current_value;
            range_size++;
            roaring_previous_uint32_iterator(&_riter);
        } while (range_size < max_range_size && _riter.has_value &&
                 _riter.current_value + 1 == *from);

        return true;
    }

private:
    roaring::api::roaring_uint32_iterator_t _riter;
};

class SegmentIteratorMock {
public:
    SegmentIteratorMock(BitmapRangeIterator* range_iter, bool read_orderby_key_reverse)
            : _read_orderby_key_reverse(read_orderby_key_reverse) {
        _ranges = range_iter->get_all_contiguous_ranges();
        if (!_ranges.empty()) {
            _range_rowid = _read_orderby_key_reverse ? _ranges[0].second : _ranges[0].first;
        }
    }

    bool next_range(const uint32_t max_range_size, uint32_t* from, uint32_t* to) {
        if (!_read_orderby_key_reverse) {
            if (_range_idx == _ranges.size() || (_range_idx == (_ranges.size() - 1) &&
                                                 _range_rowid == _ranges[_range_idx].second)) {
                return false;
            }
            if (_range_rowid == _ranges[_range_idx].second) {
                _range_idx++;
                _range_rowid = _ranges[_range_idx].first;
            }
            *from = _range_rowid;
            uint32_t limit = _range_rowid + max_range_size;
            _range_rowid = limit < _ranges[_range_idx].second ? limit : _ranges[_range_idx].second;
            *to = _range_rowid;
            return true;
        } else {
            if (_range_idx == _ranges.size() ||
                (_range_idx == (_ranges.size() - 1) && _range_rowid == _ranges[_range_idx].first)) {
                return false;
            }
            if (_range_rowid == _ranges[_range_idx].first) {
                _range_idx++;
                _range_rowid = _ranges[_range_idx].second;
            }
            *to = _range_rowid;
            size_t left_size = _range_rowid - _ranges[_range_idx].first;
            _range_rowid = left_size > max_range_size ? _range_rowid - max_range_size
                                                      : _ranges[_range_idx].first;
            *from = _range_rowid;
            return true;
        }
    }

private:
    std::vector<std::pair<uint32_t, uint32_t>> _ranges;
    size_t _range_idx = 0;
    size_t _range_rowid = 0;
    bool _read_orderby_key_reverse = false;
};

const uint32_t NROWS_READ_LIMIT = 1024;
const uint32_t PAGE_ROW_COUNT = 16384;
const uint32_t PAGE_NUM = 5;
const uint32_t NROWS = PAGE_ROW_COUNT * PAGE_NUM;

void test_func(BitmapRangeIterator* range_iter, SegmentIteratorMock& mock) {
    bool has_more_range1 = true;
    do {
        uint32_t from1 = 0;
        uint32_t to1 = 0;
        uint32_t from2 = 0;
        uint32_t to2 = 0;
        has_more_range1 = range_iter->next_range(NROWS_READ_LIMIT, &from1, &to1);
        bool has_more_range2 = mock.next_range(NROWS_READ_LIMIT, &from2, &to2);
        EXPECT_EQ(has_more_range1, has_more_range2);
        EXPECT_EQ(from1, from2);
        EXPECT_EQ(to1, to2);
    } while (has_more_range1);
}

roaring::Roaring gen_random_roaring() {
    auto rng = std::default_random_engine {
            static_cast<uint32_t>(std::chrono::steady_clock::now().time_since_epoch().count())};
    std::uniform_int_distribution<uint32_t> u(0, NROWS);
    roaring::Roaring row_bitmap;
    row_bitmap.addRange(0, NROWS);
    uint32_t remove_size = u(rng);
    for (uint32_t j = 0; j < remove_size; j++) {
        uint32_t remove_num = NROWS;
        for (int k = 0; k < 10; k++) {
            uint32_t tmp = u(rng);
            if (!row_bitmap.contains(tmp)) {
                continue;
            }
            remove_num = tmp;
            break;
        }
        if (remove_num != NROWS) {
            row_bitmap.remove(remove_num);
        }
    }
    return row_bitmap;
}

TEST(SegmentIteratorPrefetchTest, full_forward) {
    roaring::Roaring row_bitmap;
    row_bitmap.addRange(0, NROWS);
    auto range_iter1 = std::make_unique<BitmapRangeIterator>(row_bitmap);
    auto range_iter2 = std::make_unique<BitmapRangeIterator>(row_bitmap);
    SegmentIteratorMock mock(range_iter2.get(), false);
    test_func(range_iter1.get(), mock);
}

TEST(SegmentIteratorPrefetchTest, full_backward) {
    roaring::Roaring row_bitmap;
    row_bitmap.addRange(0, NROWS);
    auto range_iter1 = std::make_unique<BackwardBitmapRangeIterator>(row_bitmap);
    auto range_iter2 = std::make_unique<BackwardBitmapRangeIterator>(row_bitmap);
    SegmentIteratorMock mock(range_iter2.get(), true);
    test_func(range_iter1.get(), mock);
}

TEST(SegmentIteratorPrefetchTest, random_forward) {
    // test 50 times
    for (int i = 0; i < 50; i++) {
        roaring::Roaring row_bitmap = gen_random_roaring();
        auto range_iter1 = std::make_unique<BitmapRangeIterator>(row_bitmap);
        auto range_iter2 = std::make_unique<BitmapRangeIterator>(row_bitmap);
        SegmentIteratorMock mock(range_iter2.get(), false);
        test_func(range_iter1.get(), mock);
    }
}

TEST(SegmentIteratorPrefetchTest, random_backward) {
    // test 50 times
    for (int i = 0; i < 50; i++) {
        roaring::Roaring row_bitmap = gen_random_roaring();
        auto range_iter1 = std::make_unique<BackwardBitmapRangeIterator>(row_bitmap);
        auto range_iter2 = std::make_unique<BackwardBitmapRangeIterator>(row_bitmap);
        SegmentIteratorMock mock(range_iter2.get(), true);
        test_func(range_iter1.get(), mock);
    }
}

} // namespace doris::cloud
