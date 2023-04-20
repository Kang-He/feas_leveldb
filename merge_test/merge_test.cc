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


Status BuildTable(const std::string& dbname, Env* env, const Options& options,
                  TableCache* table_cache, Iterator* iter, FileMetaData* meta) {
    Status s;
    meta->file_size = 0;
    iter->SeekToFirst();
    // cout << "iter->Valid() = " << iter->Valid() << endl;
    // cout << "iter->key() = " << iter->key().ToString() << endl;
    // cout << "iter->value() = " << iter->value().ToString() << endl;
    std::string fname = TableFileName(dbname, meta->number);
    cout << "fname = " << fname << endl;
    if (iter->Valid()) {
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
        if (s.ok()) {
        meta->file_size = builder->FileSize();
        assert(meta->file_size > 0);
        }
        delete builder;

        // Finish and check for file errors
        if (s.ok()) {
        s = file->Sync();
        }
        if (s.ok()) {
        s = file->Close();
        }
        delete file;
        file = nullptr;

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



}
//全局公用变量
std::string dbname = "C:/Users/86158/Desktop/Code/LSM/dbData";
Env* env = Env::Default();
Options options;
TableCache* table_cache = new TableCache(dbname, options, 100000);
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
  leveldb::BuildTable(dbname, env, options, table_cache, iter, meta);
}

void build_table_with_kv_pairs(Iterator* iter, const std::string& dbname, uint64_t meta_number) {
  
  // 设置元数据
  FileMetaData* meta = new FileMetaData();
  meta->number = meta_number;

  // 构建表格
  leveldb::BuildTable(dbname, env, options, table_cache, iter, meta);
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
#include <iostream>
#include <chrono>

// ... (其他必要的包含和定义) ...

int main() {
    using namespace std::chrono;

    std::cout << "Hello World" << std::endl;
    int num_entries = 1000;
    int kv_length = 64;

    //生成第一个SST文件
    uint64_t meta_number = 1;
    build_table_with_random_kv_pairs(num_entries, kv_length, dbname, meta_number);
    cout << "build table 1" << endl;

    //生成第二个SST文件
    meta_number = 2;
    build_table_with_random_kv_pairs(num_entries, kv_length, dbname, meta_number);
    cout << "build table 2" << endl;

    // Start timing
    auto start = high_resolution_clock::now();

    // Read the first SST file
    leveldb::Table* table1 = nullptr;
    Iterator* iter1 = read_sst_file(dbname, "000001.ldb", table1);

    // End timing and calculate duration
    auto end = high_resolution_clock::now();
    auto read1_duration = duration_cast<microseconds>(end - start).count();
    std::cout << "Read the first SST file in: " << read1_duration << " microseconds" << std::endl;

    // Start timing
    start = high_resolution_clock::now();

    // Read the second SST file
    leveldb::Table* table2 = nullptr;
    Iterator* iter2 = read_sst_file(dbname, "000002.ldb", table2);

    // End timing and calculate duration
    end = high_resolution_clock::now();
    auto read2_duration = duration_cast<microseconds>(end - start).count();
    std::cout << "Read the second SST file in: " << read2_duration << " microseconds" << std::endl;

    // Merge the two SST files
    // Start timing
    start = high_resolution_clock::now();

    TestIter* merged_iter = merge_iterators(iter1, iter2);

    // End timing and calculate duration
    end = high_resolution_clock::now();
    auto merge_duration = duration_cast<microseconds>(end - start).count();
    std::cout << "Merge the two SST files in: " << merge_duration << " microseconds" << std::endl;

    // Output the merged SST file
    // Start timing
    start = high_resolution_clock::now();

    build_table_with_kv_pairs(merged_iter, dbname, 3);

    // End timing and calculate duration
    end = high_resolution_clock::now();
    auto build_duration = duration_cast<microseconds>(end - start).count();
    std::cout << "Output the merged SST file in: " << build_duration << " microseconds" << std::endl;

    // Calculate total duration and percentage
    auto total_duration = read1_duration + read2_duration + merge_duration + build_duration;
    std::cout << "Total duration: " << total_duration << " microseconds" << std::endl;
    std::cout << "Read SST files percentage: " << (static_cast<double>(read1_duration + read2_duration) / total_duration) * 100 << "%\n";
    std::cout << "Merge the two SST files percentage: " << (static_cast<double>(merge_duration) / total_duration) * 100 << "%" << std::endl;
    std::cout << "Output the merged SST file percentage: " << (static_cast<double>(build_duration) / total_duration) * 100 << "%" << std::endl;

    // Free memory
    delete iter1;
    delete table1;
    delete iter2;
    delete table2;
    delete merged_iter;

    return 0;
}
