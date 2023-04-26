// feasdb.cc
#include "feasdb/feasdb.h"
#include "feasdb/multilsm_tree_iterator.h"
#include <iostream>

namespace leveldb {

FeasDBImpl::FeasDBImpl(const Options& options, const std::string& dbname)
    : options_(options), dbname_(dbname), var_length_lsm_tree_(nullptr) {
  min_key_size_ = options_.min_key_size + seq_num_length_;
  max_key_size_ = options_.max_key_size + seq_num_length_;
  key_interval_size_ = options_.key_interval_size;
  min_value_size_ = options_.min_value_size;
  max_value_size_ = options_.max_value_size;
  value_interval_size_ = options_.value_interval_size;

  Open();
}

FeasDBImpl::~FeasDBImpl() {
  Close();
}

Status FeasDBImpl::Put(const WriteOptions& options, const Slice& key, const Slice& value) {
  Status s;
  int match_key_size = MatchKeySize(key.size());
  int match_value_size = MatchValueSize(value.size());
  WriteOptions write_options;
  //如果key或value的大小不匹配，就在var_length_lsm_tree中插入
  if(match_key_size == -1 || match_value_size == -1){
    return var_length_lsm_tree_->Put(options, key, value); 
  }
  //如果lsm_tree不存在，就创建一个新的lsm_tree
  DB* new_tree = nullptr;
  if(lsm_trees_.find({match_key_size, match_value_size}) == lsm_trees_.end()){
    Options lsm_tree_options = options_;
    lsm_tree_options.fix_block_enable = true;
    lsm_tree_options.key_length = match_key_size + seq_num_length_;
    lsm_tree_options.value_length = match_value_size;
    
    lsm_trees_[{match_key_size, match_value_size}] = nullptr;
    leveldb::DB::Open(lsm_tree_options, dbname_ + "/lsm_tree_" + std::to_string(match_key_size) + "_" + std::to_string(match_value_size), &new_tree);
    lsm_trees_[{match_key_size, match_value_size}] = new_tree;
    num_lsm_trees_++;
  }
  //按空格补齐key和value,仍旧使用slice
  std::string padded_key(key.data(), key.size());
  padded_key.append(match_key_size - key.size(), ' ');

  std::string padded_value(value.data(), value.size());
  padded_value.append(match_value_size - value.size(), ' ');

  // return (lsm_trees_[{match_key_size, match_value_size}])->Put(
  //     leveldb::WriteOptions(), padded_key, padded_value);
  s = lsm_trees_[{match_key_size, match_value_size}]->Put(leveldb::WriteOptions(), padded_key, padded_value);
  return s;
}

Status FeasDBImpl::Delete(const WriteOptions& options, const Slice& key) {
  Status s;
  //计算key、value的匹配大小，然后在相应的lsm_tree中删除
  int match_key_size = MatchKeySize(key.size());
  //如果key的大小不匹配，就在var_length_lsm_tree中删除
  if(match_key_size == -1) return var_length_lsm_tree_->Delete(options, key);

  //按空格补齐key
  std::string padded_key(key.data(), key.size());
  padded_key.append(match_key_size - key.size(), ' ');

  //在所有的key_size为match_key_size的lsm_tree中删除
  for(auto& lsm_tree : lsm_trees_){
    if(lsm_tree.first.first == match_key_size){
      s = lsm_tree.second->Delete(options, padded_key);
      //如果删除成功，就返回
      if(s.ok()) return s;
    }
  }
  return s;
}

Status FeasDBImpl::Get(const ReadOptions& options, const Slice& key, std::string* value) {
  Status s;
  //计算key、value的匹配大小，然后在相应的lsm_tree中查找
  int match_key_size = MatchKeySize(key.size());
  //如果key的大小不匹配，就在var_length_lsm_tree中查找
  if(match_key_size == -1) return var_length_lsm_tree_->Get(options, key, value);

  //按空格补齐key
  std::string padded_key(key.data(), key.size());
  padded_key.append(match_key_size - key.size(), ' ');

  //在所有的key_size为match_key_size的lsm_tree中查找
  for(auto& lsm_tree : lsm_trees_){
    if(lsm_tree.first.first == match_key_size){
      s = lsm_tree.second->Get(options, padded_key, value);
      //如果查找成功，就返回
      if(s.ok()) return s;
    }
  }
  return s;
}


Status FeasDBImpl::Write(const WriteOptions& options, WriteBatch* updates) {
  // TODO: Implement batch writes for FeasDBImpl.
  return Status::NotSupported("Batch writes are not supported for FeasDBImpl.");
}



Iterator* FeasDBImpl::NewIterator(const ReadOptions& options) {
  std::vector<Iterator*> iterators;
  for (auto& lsm_tree : lsm_trees_) {
    iterators.push_back(lsm_tree.second->NewIterator(options));
  }
  iterators.push_back(var_length_lsm_tree_->NewIterator(options));
  return new MultiLSMTreeIterator(iterators);
}

Status FeasDBImpl::Open() {
  // 打开变长lsm_tree
  Status s;
  Options var_length_options = options_;
  var_length_options.fix_block_enable = false;
  s = leveldb::DB::Open(var_length_options, dbname_ + "/var_length_lsm_tree", &var_length_lsm_tree_);
  num_lsm_trees_++;
  //检查参数并修正参数
  CheckSizeParameters(); 
  return s;
}

void FeasDBImpl::Close() {
  for (auto& lsm_tree : lsm_trees_) {
    delete lsm_tree.second;
  }
  lsm_trees_.clear();
  delete var_length_lsm_tree_;
  var_length_lsm_tree_ = nullptr;
}


void FeasDBImpl::CheckSizeParameters() {
    if (min_key_size_ <= 0 || max_key_size_ <= 0 || key_interval_size_ <= 0 ||
        min_value_size_ <= 0 || max_value_size_ <= 0) {
      std::cerr << "Size parameters should be greater than 0." << std::endl;
      exit(EXIT_FAILURE);
    }

    if (min_key_size_ > max_key_size_ || min_value_size_ > max_value_size_) {
      std::cerr << "Min size should not be greater than max size." << std::endl;
      exit(EXIT_FAILURE);
    }

    // Adjust max_key_size_ and max_value_size_ to be multiples of their respective intervals
    int key_diff = (max_key_size_ - min_key_size_) % key_interval_size_;
    int value_diff = (max_value_size_ - min_value_size_) % key_interval_size_;
    
    max_key_size_ += (key_interval_size_ - key_diff) % key_interval_size_;
    max_value_size_ += (key_interval_size_ - value_diff) % key_interval_size_;
  }

int FeasDBImpl::MatchKeySize(int key_size) {
    if (key_size < min_key_size_ || key_size > max_key_size_) {
      return -1;
    }

    int matched_size = ((key_size - min_key_size_ + key_interval_size_ - 1) / key_interval_size_) * key_interval_size_ + min_key_size_;
    return matched_size;
  }

  int FeasDBImpl::MatchValueSize(int value_size) {
    if (value_size < min_value_size_ || value_size > max_value_size_) {
      return -1;
    }

    int matched_size = ((value_size - min_value_size_ + value_interval_size_ - 1) / value_interval_size_) * value_interval_size_ + min_value_size_;
    return matched_size;
  }

}  // namespace leveldb

