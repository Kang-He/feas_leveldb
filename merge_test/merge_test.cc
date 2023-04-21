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

#include "db/builder.h"

#include "db/dbformat.h"
#include "db/filename.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "leveldb/comparator.h"
#include "table/format.h"
#include "util/coding.h"
#include "util/logging.h"
//#include "merge_test/fix_table_builder.cc"
using namespace std;
using namespace leveldb;
namespace leveldb {
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
//全局公用变量
std::string dbname = "C:/Users/86158/Desktop/Code/LSM/dbData";
Env* env = Env::Default();
Options options;
TableCache* table_cache = new TableCache(dbname, options, 100000);
TimeRecorder t_recorder;

Status MyBuildTable(const std::string& dbname, Env* env, const Options& options,
                  TableCache* table_cache, Iterator* iter, FileMetaData* meta) {
    Status s;
    meta->file_size = 0;
    iter->SeekToFirst();
    std::string fname = TableFileName(dbname, meta->number);
    cout << "fname = " << fname << endl;
    if (iter->Valid()) {
        //开始计时
        t_recorder.start();

        WritableFile* file;
        s = env->NewWritableFile(fname, &file);
        if (!s.ok()) {
        return s;
        }
        TableBuilder* builder = new TableBuilder(options, file);
        meta->smallest.DecodeFrom(iter->key());
        Slice key;
        for (; iter->Valid(); iter->Next()) {
            key = iter->key();
            builder->Add(key, iter->value());
        }
        if (!key.empty()) {
            meta->largest.DecodeFrom(key);
        }

        // Finish and check for builder errors
        s = builder->Finish();

        //停止计时
        t_recorder.stop(TimeRecorder::Operation::COMP);
        if (s.ok()) {
        meta->file_size = builder->FileSize();
        assert(meta->file_size > 0);
        }
        delete builder;

        //开始计时
        t_recorder.start();

        // Finish and check for file errors
        if (s.ok()) {
        s = file->Sync();
        }
        if (s.ok()) {
        s = file->Close();
        }
        delete file;
        file = nullptr;
        //停止计时
        t_recorder.stop(TimeRecorder::Operation::WRITE);
        if (s.ok()) {
        // Verify that the table is usable
        Iterator* it = table_cache->NewIterator(ReadOptions(), meta->number,
                                                meta->file_size);
        s = it->status();
        delete it;
        }
    }

    // Check for input iterator errors
    if (!iter->status().ok()) {
        s = iter->status();
    }

    if (s.ok() && meta->file_size > 0) {
        // Keep it
    } else {
        env->RemoveFile(fname);
    }
    return s;
}


// Add this function to generate random key-value pairs
vector<pair<string, string>> generate_random_kv_pairs(int num_entries, int kv_length) {
  vector<pair<string, string>> kv_pairs;
  random_device rd;
  mt19937 generator(rd());
  uniform_int_distribution<> dist(0, 25);

  for (int i = 0; i < num_entries; ++i) {
    stringstream key_stream, value_stream;
    for (int j = 0; j < kv_length; ++j) {
      key_stream << static_cast<char>('a' + dist(generator));
      value_stream << static_cast<char>('a' + dist(generator));
    }
    kv_pairs.emplace_back(key_stream.str(), value_stream.str());
  }

  return kv_pairs;
}

// 函数：构建表格并填充随机键值对
// 参数：
// num_entries - 随机生成的键值对数量
// kv_length - 键值对的长度
// dbname - 数据库名称
// meta_number - 元数据编号
void build_table_with_random_kv_pairs(int num_entries, int kv_length, const std::string& dbname, uint64_t meta_number) {
  // 生成随机键值对
  vector<pair<string, string>> kv_pairs = generate_random_kv_pairs(num_entries, kv_length);
  sort(kv_pairs.begin(), kv_pairs.end());

  // 将生成的键值对传递给TestIter
  Iterator* iter = new TestIter(kv_pairs);

  // 设置元数据
  FileMetaData* meta = new FileMetaData();
  meta->number = meta_number;

  // 构建表格
  MyBuildTable(dbname, env, options, table_cache, iter, meta);
}

void build_table_with_kv_pairs(Iterator* iter, const std::string& dbname, uint64_t meta_number) {
  
  // 设置元数据
  FileMetaData* meta = new FileMetaData();
  meta->number = meta_number;

  // 构建表格
  MyBuildTable(dbname, env, options, table_cache, iter, meta);
}

inline Iterator* read_sst_file(const std::string& dbname, const std::string& file_name, leveldb::Table* table) {
  Options options;
  std::string file_path = dbname + "/" + file_name;
  uint64_t file_size;
  
  env->GetFileSize(file_path, &file_size);
  cout << "File path: " << file_path << endl;
  cout << "File size: " << file_size << endl;
  // Open the file using Table::Open
  leveldb::RandomAccessFile* file;
  env->NewRandomAccessFile(file_path, &file);
  //leveldb::Cache* cache = leveldb::NewLRUCache(100000000); // 100MB cache
  
  leveldb::Table::Open(options, file, file_size, &table);

  // Create an iterator using Table::NewIterator
  ReadOptions read_options;
  //read_options.fill_cache = false;
  Iterator* iterator = table->NewIterator(read_options);
  iterator->SeekToFirst();
//   cout << "Iterator is valid: " << iterator->Valid() << endl;
//   cout << "Iterator key: " << iterator->key().ToString() << endl;

  // Read the data from the iterator and store it in a vector

  // Clean up
  //delete table;
  //delete cache;
  //delete file;

  return iterator;
}
// 函数：合并排序两个迭代器
TestIter* merge_iterators(Iterator* iter1, Iterator* iter2) {
  std::vector<std::pair<std::string, std::string>> merged_data;

  iter1->SeekToFirst();
  iter2->SeekToFirst();
  while (iter1->Valid() && iter2->Valid()) {
    int comp = iter1->key().compare(iter2->key());
    if (comp < 0) {
      merged_data.push_back({iter1->key().ToString(), iter1->value().ToString()});
      iter1->Next();
    } else {
      merged_data.push_back({iter2->key().ToString(), iter2->value().ToString()});
      iter2->Next();
    }
  }

  while (iter1->Valid()) {
    merged_data.push_back({iter1->key().ToString(), iter1->value().ToString()});
    iter1->Next();
  }

  while (iter2->Valid()) {
    merged_data.push_back({iter2->key().ToString(), iter2->value().ToString()});
    iter2->Next();
  }

  return new TestIter(merged_data);
}


void generate_two_sst_files(int num_entries, int kv_length) {
    // 生成第一个SST文件
    uint64_t meta_number = 1;
    build_table_with_random_kv_pairs(num_entries, kv_length, dbname, meta_number);
    cout << "build table 1" << endl;

    // 生成第二个SST文件
    meta_number = 2;
    build_table_with_random_kv_pairs(num_entries, kv_length, dbname, meta_number);
    cout << "build table 2" << endl;
}


int main() {
    using namespace std::chrono;
    
    //test_hello();

    std::cout << "Hello World" << std::endl;
    int num_entries = 1000;
    int kv_length = 64;

    // 生成2个SST文件
    generate_two_sst_files(num_entries, kv_length);
    

    // 开始计时
    t_recorder.start();

    // 读取第一个SST文件
    leveldb::Table* table1 = nullptr;
    Iterator* iter1 = read_sst_file(dbname, "000001.ldb", table1);

    // 结束计时并记录时间
    t_recorder.stop(TimeRecorder::READ);

    // 开始计时
    t_recorder.start();

    // 读取第二个SST文件
    leveldb::Table* table2 = nullptr;
    Iterator* iter2 = read_sst_file(dbname, "000002.ldb", table2);

    // 结束计时并记录时间
    t_recorder.stop(TimeRecorder::READ);

    // 合并两个SST文件
    // 开始计时
    t_recorder.start();

    TestIter* merged_iter = merge_iterators(iter1, iter2);

    // 结束计时并记录时间
    t_recorder.stop(TimeRecorder::SORT);

    // 输出合并后的SST文件
    build_table_with_kv_pairs(merged_iter, dbname, 3);

    // 输出时间记录
    t_recorder.print_durations();

    // 释放内存
    delete iter1;
    delete table1;
    delete iter2;
    delete table2;
    delete merged_iter;

    return 0;
}
