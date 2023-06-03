// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/filename.h"
#include <string>
#include <cassert>
#include <cstdio>
#include<iostream>
#include <random>
#include <sstream>
#include <chrono>
#include <map>
#include <string>
#include <iomanip>


#include "leveldb/iterator.h"

using namespace std;

namespace leveldb {

//用来构造随机生成字符串的迭代器
class TestIter : public Iterator {
    private:
    vector<pair<string, string>> data_;
    Status status_;
    // std::string key_;
    // std::string value_;
    int cur_;


    public:
    TestIter(const vector<pair<string, string>> &data) : data_(data) {cur_ = 0;}


    bool Valid() const override { return (cur_ < data_.size()) && (cur_ >= 0); }
    Status status() const override { return status_; }
    Slice key() const override {
        assert(Valid()); 
        return data_[cur_].first;
    }
    Slice value() const override {
        assert(Valid());
        return data_[cur_].second;
    }

    void Next() override {
        assert(Valid());
        cur_++;
    }

    void Prev() override {
        assert(Valid());
        cur_--;
    }

    void Seek(const Slice& target) override {
        cur_ = 0;
        while (cur_ < data_.size() && data_[cur_].first < target.ToString()) {
            cur_++;
        }
    }
    void SeekToFirst() override {
        cur_ = 0;
    }

    void SeekToLast() override {
        cur_ = data_.size() - 1;
    }

    //返回键值对数量
    int size() const { return data_.size(); }

 private:
  
};

class Iter : public Iterator {
 private:
  const Comparator* const comparator_;
  const char* const data_;       // underlying block contents
  const size_t size_;            // block size
  uint32_t key_length_;
  uint32_t value_length_;
  uint32_t num_entries_;

  // current_ is offset in data_ of current entry.  >= size_ if !Valid
  uint32_t current_;
  Status status_;

  inline int Compare(const Slice& a, const Slice& b) const {
    return comparator_->Compare(a, b);
  }

  // Return the offset in data_ just past the end of the current entry.
  inline uint32_t NextEntryOffset() const {
    return current_ + key_length_ + value_length_;
  }
  inline uint32_t LastEntryOffset() const {
    return current_ - key_length_ - value_length_;
  }

 public:
  Iter(const Comparator* comparator, const char* data, size_t size, uint32_t key_length,
       uint32_t value_length, uint32_t num_entries)
      : comparator_(comparator),
        data_(data),
        size_(size),
        key_length_(key_length),
        value_length_(value_length),
        num_entries_(num_entries),
        current_(0) {
  }

  bool Valid() const override { return current_ < size_; }
  Status status() const override { return status_; }
  Slice key() const override {
    assert(Valid());
    return Slice(data_ + current_, key_length_);
  }
  Slice value() const override {
    assert(Valid());
    return Slice(data_ + current_ + key_length_, value_length_);
  }

  void Next() override {
    assert(Valid());
    current_ = NextEntryOffset();
  }

  void Prev() override {
    assert(Valid());
    current_ = LastEntryOffset();
  }

  void Seek(const Slice& target) override {
    uint32_t left = 0;
    uint32_t right = num_entries_;

    while (left < right) {
      uint32_t mid = left + (right - left) / 2;
      uint32_t mid_offset = mid * (key_length_ + value_length_);
      Slice mid_key(data_ + mid_offset, key_length_);

      int cmp = Compare(mid_key, target);
      if (cmp < 0) {
        left = mid + 1;
      } else {
        right = mid;
      }
    }

    current_ = left * (key_length_ + value_length_);
    if (current_ >= size_) {
      // 如果超出范围，设置为无效
      current_ = size_;
    }
  }

  void SeekToFirst() override {
    current_ = 0;
  }

  void SeekToLast() override {
    current_ = (num_entries_ - 1) * (key_length_ + value_length_);
  }
};



//用来记录合并阶段的各部分时间
class TimeRecorder {
public:
    enum Operation {
        READ,
        CRC,
        DECOMP,
        SORT,
        COMP,
        RE_CRC,
        WRITE,
        NUM_OPERATIONS
    };
    std::map<Operation, std::chrono::duration<double>> durations_;
private:
    
    std::map<Operation, std::chrono::time_point<std::chrono::steady_clock>> start_times_;

public:
    TimeRecorder() {
        for (int i = 0; i < NUM_OPERATIONS; ++i) {
            durations_[static_cast<Operation>(i)] = std::chrono::duration<double>::zero();
        }
    }

    void start(Operation op) {
        start_times_[op] = std::chrono::steady_clock::now();
    }

    void stop(Operation op) {
        auto end_time = std::chrono::steady_clock::now();
        durations_[op] += end_time - start_times_[op];
    }

    void print_durations() {
        static const std::map<Operation, std::string> operation_names = {
            {READ, "Read"},
            {CRC, "Checksum"},
            {DECOMP, "Decompression"},
            {SORT, "Sort"},
            {COMP, "Compression"},
            {RE_CRC, "Re-Checksum"},
            {WRITE, "Write"}
        };

        auto total_duration = std::chrono::duration<double>::zero();
        for (const auto& duration : durations_) {
            total_duration += duration.second;
        }

        for (const auto& duration : durations_) {
            std::cout << operation_names.at(duration.first) << " time: " << duration.second.count() << "s"
                      << " percentage: " << std::fixed << std::setprecision(2)
                      << (duration.second.count() / total_duration.count()) * 100 << "%" << std::endl;
        }
    }
};

}
