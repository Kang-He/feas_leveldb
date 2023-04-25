// feasdb.cc
#include "feasdb/feasdb.h"


namespace leveldb {

FeasDBImpl::FeasDBImpl(const Options& options, const std::string& dbname)
    : options_(options), dbname_(dbname), var_length_lsm_tree_(nullptr) {
  Open();
}

FeasDBImpl::~FeasDBImpl() {
  Close();
}

Status FeasDBImpl::Put(const WriteOptions& options, const Slice& key, const Slice& value) {
  size_t index = SelectLSMTreeIndex(key, value);
  if (index < lsm_trees_.size()) {
    return lsm_trees_[index]->Put(options, key, value);
  } else {
    return var_length_lsm_tree_->Put(options, key, value);
  }
}

Status FeasDBImpl::Delete(const WriteOptions& options, const Slice& key) {
  // Note: This implementation assumes that the key exists in only one LSM tree.
  // If your design allows for a key to exist in multiple LSM trees, you will need to
  // modify this method to iterate through all LSM trees and delete the key from each one.
  for (auto& lsm_tree : lsm_trees_) {
    if (lsm_tree->Get(ReadOptions(), key, nullptr).ok()) {
      return lsm_tree->Delete(options, key);
    }
  }
  return var_length_lsm_tree_->Delete(options, key);
}

Status FeasDBImpl::Write(const WriteOptions& options, WriteBatch* updates) {
  // TODO: Implement batch writes for FeasDBImpl.
  return Status::NotSupported("Batch writes are not supported for FeasDBImpl.");
}

Status FeasDBImpl::Get(const ReadOptions& options, const Slice& key, std::string* value) {
  for (auto& lsm_tree : lsm_trees_) {
    if (lsm_tree->Get(options, key, value).ok()) {
      return Status::OK();
    }
  }
  return var_length_lsm_tree_->Get(options, key, value);
}

Iterator* FeasDBImpl::NewIterator(const ReadOptions& options) {
  std::vector<Iterator*> iterators;
  for (auto& lsm_tree : lsm_trees_) {
    iterators.push_back(lsm_tree->NewIterator(options));
  }
  iterators.push_back(var_length_lsm_tree_->NewIterator(options));
  return new MultiLSMTreeIterator(iterators);
}

Status FeasDBImpl::Open() {
  // Open N-1 fixed-length LSM trees and the variable-length LSM tree
  // based on your design.
  // ...
  return Status::OK();
}

void FeasDBImpl::Close() {
  for (auto& lsm_tree : lsm_trees_) {
    delete lsm_tree;
  }
  lsm_trees_.clear();
  delete var_length_lsm_tree_;
  var_length_lsm_tree_ = nullptr;
}

size_t FeasDBImpl::SelectLSMTreeIndex(const Slice& key, const Slice& value) {
  int lsm_tree_index = 0;
  Status status = key_value_length_selector_.GetMatchingLSMTree(key, value, &lsm_tree_index);
  
  // 如果未找到匹配的LSM-tree，则使用变长键值长度LSM-tree
  if (!status.ok()) {
    lsm_tree_index = 0;
  }

  return static_cast<size_t>(lsm_tree_index);
}


}  // namespace leveldb

