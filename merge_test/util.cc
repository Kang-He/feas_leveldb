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

private:
    std::map<Operation, std::chrono::duration<double>> durations_;
    std::chrono::time_point<std::chrono::steady_clock> start_time_;

public:
    TimeRecorder() {
        for (int i = 0; i < NUM_OPERATIONS; ++i) {
            durations_[static_cast<Operation>(i)] = std::chrono::duration<double>::zero();
        }
    }

    void start() {
        start_time_ = std::chrono::steady_clock::now();
    }

    void stop(Operation op) {
        auto end_time = std::chrono::steady_clock::now();
        durations_[op] += end_time - start_time_;
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
