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

//#include "db/builder.h"

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
#include "merge_test/fix_table_builder.h"
#include "merge_test/fix_table.h"

#include "merge_test/util.cc"
using namespace std;
using namespace leveldb;

class SSTMergeTester {
public:
    enum class SSTType {
        FixedLength,
        VariableLength
    };

    SSTMergeTester(int num_entries, int kv_length, SSTType sst_type)
        : num_entries_(num_entries),
            kv_length_(kv_length),
            sst_type_(sst_type),
            dbname_("C:/Users/86158/Desktop/Code/LSM/dbData"),
            env_(Env::Default()),
            options_(),
            table_cache_(new TableCache(dbname_, options_, 100000)),
            t_recorder_() {}

    void TestMergeProcess() {
        // 生成2个SST文件
        generate_two_sst_files(num_entries_, kv_length_);

        // 开始计时
        t_recorder_.start();

        // 读取第一个SST文件
        Iterator* iter1 = read_sst_file(dbname_, "000001.ldb");

        // 结束计时并记录时间
        t_recorder_.stop(TimeRecorder::READ);

        // 开始计时
        t_recorder_.start();

        // 读取第二个SST文件
        Iterator* iter2 = read_sst_file(dbname_, "000002.ldb");

        // 结束计时并记录时间
        t_recorder_.stop(TimeRecorder::READ);

        // 合并两个SST文件
        // 开始计时
        t_recorder_.start();

        TestIter* merged_iter = merge_iterators(iter1, iter2);

        // 结束计时并记录时间
        t_recorder_.stop(TimeRecorder::SORT);

        // 输出合并后的SST文件
        build_table_with_kv_pairs(merged_iter, dbname_, 3);

        // 输出时间记录
        t_recorder_.print_durations();

        // 释放内存
        delete iter1;
        delete iter2;
        delete merged_iter;
        //test_merge_process(num_entries_, kv_length_);
    }

private:
    int num_entries_;
    int kv_length_;
    SSTType sst_type_;
    std::string dbname_;
    Env* env_;
    Options options_;
    TableCache* table_cache_;
    TimeRecorder t_recorder_;

    // 将原先的全局变量和函数声明为类的私有成员和成员函数
    // ...
    void test_fixed_length_merge_process(){
        
    }
    Status MyBuildTable(const std::string& dbname, Env* env, const Options& options,
                    TableCache* table_cache, Iterator* iter, FileMetaData* meta) {
        Status s;
        meta->file_size = 0;
        iter->SeekToFirst();
        std::string fname = TableFileName(dbname, meta->number);
        cout << "fname = " << fname << endl;
        if (iter->Valid()) {
            //开始计时
            t_recorder_.start();

            WritableFile* file;
            s = env->NewWritableFile(fname, &file);
            if (!s.ok()) {
            return s;
            }
            //如果是变长键值长度SST文件
            if(sst_type_ == SSTType::VariableLength){
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
                t_recorder_.stop(TimeRecorder::Operation::COMP);
                if (s.ok()) {
                meta->file_size = builder->FileSize();
                assert(meta->file_size > 0);
                }
                delete builder;
            }else{
                FixTableBuilder* builder = new FixTableBuilder(options, file);
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
                t_recorder_.stop(TimeRecorder::Operation::COMP);
                if (s.ok()) {
                meta->file_size = builder->FileSize();
                assert(meta->file_size > 0);
                }
                delete builder;
            }
            

            //开始计时
            t_recorder_.start();

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
            t_recorder_.stop(TimeRecorder::Operation::WRITE);
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
        MyBuildTable(dbname_, env_, options_, table_cache_, iter, meta);
    }

    void build_table_with_kv_pairs(Iterator* iter, const std::string& dbname, uint64_t meta_number) {
        
        // 设置元数据
        FileMetaData* meta = new FileMetaData();
        meta->number = meta_number;

        // 构建表格
        MyBuildTable(dbname_, env_, options_, table_cache_, iter, meta);
    }

    inline Iterator* read_sst_file(const std::string& dbname, const std::string& file_name) {
        std::string file_path = dbname + "/" + file_name;
        uint64_t file_size;
        
        env_->GetFileSize(file_path, &file_size);
        cout << "File path: " << file_path << endl;
        cout << "File size: " << file_size << endl;
        // Open the file using Table::Open
        leveldb::RandomAccessFile* file;
        env_->NewRandomAccessFile(file_path, &file);
        //leveldb::Cache* cache = leveldb::NewLRUCache(100000000); // 100MB cache
        if(sst_type_ == SSTType::VariableLength){
            leveldb::Table* table = nullptr;
            leveldb::Table::Open(options_, file, file_size, &table);
            ReadOptions read_options;
            Iterator* iterator = table->NewIterator(read_options);
            iterator->SeekToFirst();
            return iterator;
        }else{
            FixTable* table = nullptr;
            FixTable::Open(options_, file, file_size, &table);
            ReadOptions read_options;
            Iterator* iterator = table->NewIterator(read_options);
            iterator->SeekToFirst();
            return iterator;
        }
  
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
        build_table_with_random_kv_pairs(num_entries, kv_length, dbname_, meta_number);
        cout << "build table 1" << endl;

        // 生成第二个SST文件
        meta_number = 2;
        build_table_with_random_kv_pairs(num_entries, kv_length, dbname_, meta_number);
        cout << "build table 2" << endl;
    }

};

int main() {
  //SSTMergeTester tester(1000, 64, SSTMergeTester::SSTType::VariableLength);
  SSTMergeTester tester(1000, 64, SSTMergeTester::SSTType::FixedLength);
  tester.TestMergeProcess();
  return 0;
}





