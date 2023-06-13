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
#include "table/block.h"
#include "util/coding.h"
#include "util/logging.h"
#include "merge_test/fix_table_builder.h"
#include "merge_test/fix_table.h"
//#include "merge_test/fix_table.cc"
#include "merge_test/fix_block.h"

#include "merge_test/util.cc"
using namespace std;
using namespace leveldb;

class SSTMergeTester {
public:
    enum class SSTType {
        FixedLength,
        VariableLength
    };
    TimeRecorder t_recorder_;


    SSTMergeTester(std::string dbname, int num_entries, int key_length, int value_length, SSTType sst_type)
        : num_entries_(num_entries),
            key_length_(key_length),
            value_length_(value_length),
            sst_type_(sst_type),
            dbname_(dbname),
            env_(Env::Default()),
            options_(),
            table_cache_(new TableCache(dbname_, options_, 100000)),
            t_recorder_() {
                options_.key_length = key_length_;
                options_.value_length = value_length_;
                read_options_.t_recorder =(char*)(&t_recorder_);
            }

    void TestMergeProcess() {
        // 生成2个SST文件
        generate_two_sst_files();

        // 开始计时
        t_recorder_.start(TimeRecorder::DECOMP);

        // 读取第一个SST文件
        Iterator* iter1 = read_sst_file(dbname_, "000001.ldb");

        // 结束计时并记录时间
        t_recorder_.stop(TimeRecorder::DECOMP);

        // 开始计时
        t_recorder_.start(TimeRecorder::DECOMP);

        // 读取第二个SST文件
        Iterator* iter2 = read_sst_file(dbname_, "000002.ldb");

        // 结束计时并记录时间
        t_recorder_.stop(TimeRecorder::DECOMP);

        // 合并两个SST文件
        // 开始计时
        t_recorder_.start(TimeRecorder::SORT);
        Iterator* merged_iter = nullptr;
        if (sst_type_ == SSTType::FixedLength) {
            merged_iter = fixed_merge_iterators(iter1, iter2);
        } else {
            merged_iter = var_merge_iterators(iter1, iter2);
        }
        //TestIter* merged_iter = merge_iterators(iter1, iter2);

        // 结束计时并记录时间
        t_recorder_.stop(TimeRecorder::SORT);

        // 输出合并后的SST文件
        build_table_with_kv_pairs(merged_iter, dbname_, 3);

        // 输出时间记录
        // 由于read只记录了datablock的读取，因此为估计实际值，将read时间乘以2
        t_recorder_.durations_[TimeRecorder::READ] *= 1;
        //t_recorder_.durations_[TimeRecorder::DECOMP] = t_recorder_.durations_[TimeRecorder::DECOMP] - t_recorder_.durations_[TimeRecorder::READ];
        t_recorder_.print_durations();

        // 释放内存
        delete iter1;
        delete iter2;
        delete merged_iter;
        //test_merge_process(num_entries_, kv_length_);
    }

private:
    int num_entries_;
    int key_length_;
    int value_length_;

    SSTType sst_type_;
    std::string dbname_;
    Env* env_;
    Options options_;
    TableCache* table_cache_;
    ReadOptions read_options_;

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
        std::cout << "fname = " << fname << endl;
        if (iter->Valid()) {
            //开始计时
            t_recorder_.start(TimeRecorder::Operation::COMP);

            WritableFile* file;
            s = env->NewWritableFile(fname, &file);
            if (!s.ok()) {
                std::cout << "s = " << s.ToString() << endl;
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
            t_recorder_.start(TimeRecorder::Operation::WRITE);

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
    
    std::vector<std::pair<std::string, std::string>> generate_random_kv_pairs() {
        std::vector<std::pair<std::string, std::string>> kv_pairs;
        std::random_device rd;
        std::mt19937 generator(rd());
        std::uniform_int_distribution<> dist(0, 25);

        for (int i = 0; i < num_entries_; ++i) {
            std::stringstream key_stream, value_stream;
            for (uint32_t j = 0; j < key_length_; ++j) {
            key_stream << static_cast<char>('a' + dist(generator));
            }
            for (uint32_t j = 0; j < value_length_; ++j) {
            value_stream << static_cast<char>('a' + dist(generator));
            }
            kv_pairs.emplace_back(key_stream.str(), value_stream.str());
        }

        return kv_pairs;
    }


    // 函数：构建表格并填充随机键值对
    void build_table_with_random_kv_pairs(uint64_t meta_number) {
        // 生成随机键值对
        vector<pair<string, string>> kv_pairs = generate_random_kv_pairs();
        sort(kv_pairs.begin(), kv_pairs.end());
        // //输出键值对
        // for(auto kv : kv_pairs){
        //     cout << kv.first << " " << kv.second << endl;
        // }
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
            //ReadOptions read_options;
            Iterator* iterator = table->NewIterator(read_options_);
            iterator->SeekToFirst();
            return iterator;
        }else{
            FixTable* table = nullptr;
            FixTable::Open(options_, file, file_size, &table);
            //ReadOptions read_options;
            Iterator* iterator = table->NewIterator(read_options_);
            iterator->SeekToFirst();
            return iterator;
        }
  
    }
// 函数：合并排序两个迭代器
    TestIter* var_merge_iterators(Iterator* iter1, Iterator* iter2) {
        std::vector<std::pair<std::string, std::string>> merged_data;
        iter1->SeekToFirst();
        iter2->SeekToFirst();
        int i = 0;
        // //输出iter1键值对
        // i = 0;
        // cout << "iter1 kvs:" << endl;
        // while(iter1->Valid()){
        //     i++;
        //     cout << "i: " << i << " ";
        //     cout << "key: " << iter1->key().ToString() << "  value: " << iter1->value().ToString() << endl;
        //     iter1->Next();
        // }
        // //输出iter2键值对
        // i = 0;
        // cout << "iter2 kvs:" << endl;
        // while(iter2->Valid()){
        //     i++;
        //     cout << "i: " << i << " ";
        //     cout << "key: " << iter2->key().ToString() << "  value: " << iter2->value().ToString() << endl;
        //     iter2->Next();
        // }

        iter1->SeekToFirst();
        iter2->SeekToFirst();
        i = 0;
        while (iter1->Valid() && iter2->Valid()) {
            // i++;
            // cout << "i: " << i << endl;
            // cout << "key: " << iter1->key().ToString() << "value: " << iter1->value().ToString() << endl;
            // cout << "key: " << iter2->key().ToString() << "value: " << iter2->value().ToString() << endl;
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

    Iterator* fixed_merge_iterators(Iterator* iter1, Iterator* iter2) {
        size_t data_size = 0;
        data_size = num_entries_ * (key_length_ + value_length_) * 2;

        char* merged_data = new char[data_size];
        size_t offset = 0;

        iter1->SeekToFirst();
        iter2->SeekToFirst();

        while (iter1->Valid() && iter2->Valid()) {
            int comp = iter1->key().compare(iter2->key());
            if (comp < 0) {
                memcpy(merged_data + offset, iter1->key().data(), key_length_);
                memcpy(merged_data + offset + key_length_, iter1->value().data(), value_length_);
                iter1->Next();
            } else {
                memcpy(merged_data + offset, iter2->key().data(), key_length_);
                memcpy(merged_data + offset + key_length_, iter2->value().data(), value_length_);
                iter2->Next();
            }
            offset += key_length_ + value_length_;
        }

        while (iter1->Valid()) {
            memcpy(merged_data + offset, iter1->key().data(), key_length_);
            memcpy(merged_data + offset + key_length_, iter1->value().data(), value_length_);
            iter1->Next();
            offset += key_length_ + value_length_;
        }

        while (iter2->Valid()) {
            memcpy(merged_data + offset, iter2->key().data(), key_length_);
            memcpy(merged_data + offset + key_length_, iter2->value().data(), value_length_);
            iter2->Next();
            offset += key_length_ + value_length_;
        }

        return new Iter(options_.comparator, merged_data, data_size, key_length_, value_length_, num_entries_);
    }


    void generate_two_sst_files() {
        // 生成第一个SST文件
        uint64_t meta_number = 1;
        build_table_with_random_kv_pairs(meta_number);
        cout << "build table 1" << endl;

        // 生成第二个SST文件
        meta_number = 2;
        build_table_with_random_kv_pairs(meta_number);
        cout << "build table 2" << endl;
    }

};
void print_comparative_durations(TimeRecorder& varTester, TimeRecorder& fixedTester) {
    std::vector<TimeRecorder::Operation> operations = {
        TimeRecorder::READ,
        TimeRecorder::DECOMP,
        TimeRecorder::SORT,
        TimeRecorder::COMP,
        TimeRecorder::WRITE
    };

    std::map<TimeRecorder::Operation, std::string> operation_names = {
        {TimeRecorder::READ, "Read"},
        {TimeRecorder::DECOMP, "Decompression"},
        {TimeRecorder::SORT, "Sort"},
        {TimeRecorder::COMP, "Compression"},
        {TimeRecorder::WRITE, "Write"}
    };

    auto total_duration_var = std::chrono::duration<double>::zero();
    auto total_duration_fixed = std::chrono::duration<double>::zero();

    for (auto op : operations) {
        total_duration_var += varTester.durations_[op];
        total_duration_fixed += fixedTester.durations_[op];
    }

    double total_speedup_ratio = total_duration_var.count() / total_duration_fixed.count();

    std::cout << std::fixed << std::setprecision(4);
    std::cout << std::left << std::setw(14) << "Operation"
              << std::setw(14) << "Var Time (us)"
              << std::setw(14) << "Fixed Time (us)"
              << std::setw(14) << "Var Percent"
              << std::setw(14) << "Fixed Percent"
              << "Speedup Ratio" << std::endl;

    for (auto op : operations) {
        double var_time = varTester.durations_[op].count() * 1000000;
        double fixed_time = fixedTester.durations_[op].count() * 1000000;
        double var_percent = (varTester.durations_[op].count() / total_duration_var.count()) * 100;
        double fixed_percent = (fixedTester.durations_[op].count() / total_duration_fixed.count()) * 100;
        double speedup_ratio = var_time / fixed_time;

        std::cout << std::left << std::setw(14) << operation_names[op]
                  << std::setw(14) << var_time
                  << std::setw(14) << fixed_time
                  << std::setw(14) << std::setprecision(1) << var_percent << "%"
                  << std::setw(14) << fixed_percent << "%"
                  << std::setprecision(4) << speedup_ratio << std::endl;
    }

    std::cout << std::left << std::setw(14) << "Total"
              << std::setw(14) << total_duration_var.count() * 1000000
              << std::setw(14) << total_duration_fixed.count() * 1000000
              << std::setw(14) << ""
              << std::setw(14) << ""
              << total_speedup_ratio << std::endl;
}

int main() {
  int num_enties = 10000;
  int key_length = 64;
  int value_length = 64;
  
  SSTMergeTester varTester("/users/hys/exp/dbdata/ssdtest/varDB", num_enties, key_length, value_length, SSTMergeTester::SSTType::VariableLength);
  varTester.TestMergeProcess();

  SSTMergeTester fixedTester("/users/hys/exp/dbdata/ssdtest/fixedDB", num_enties, key_length, value_length, SSTMergeTester::SSTType::FixedLength);
  fixedTester.TestMergeProcess();
  
  
  //打印实验结果
  print_comparative_durations(varTester.t_recorder_, fixedTester.t_recorder_);
  return 0;
}





