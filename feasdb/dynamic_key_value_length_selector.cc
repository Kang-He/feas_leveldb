#include "dynamic_key_value_length_selector.h"

namespace leveldb {

DynamicKeyValueLengthSelector::DynamicKeyValueLengthSelector(
    int min_key_size, int max_key_size, int key_interval_size,
    int min_value_size, int max_value_size, int value_interval_size)
    : min_key_size_(min_key_size),
      max_key_size_(max_key_size),
      key_interval_size_(key_interval_size),
      min_value_size_(min_value_size),
      max_value_size_(max_value_size),
      value_interval_size_(value_interval_size) {
  // 初始化键值长度对数组
  for (int key_size = min_key_size_; key_size <= max_key_size_; key_size += key_interval_size_) {
    for (int value_size = min_value_size_; value_size <= max_value_size_; value_size += value_interval_size_) {
      key_value_size_pairs_.emplace_back(key_size, value_size);
    }
  }
}

DynamicKeyValueLengthSelector::~DynamicKeyValueLengthSelector() {}

Status DynamicKeyValueLengthSelector::GetMatchingLSMTree(const Slice& key, const Slice& value, int* lsm_tree_index) {
  int ks = static_cast<int>(key.size());
  int vs = static_cast<int>(value.size());
  int match_key_size = (ks + key_interval_size_ - 1) / key_interval_size_ * key_interval_size_;
  int match_value_size = (vs + value_interval_size_ - 1) / value_interval_size_ * value_interval_size_;

  for (size_t i = 0; i < key_value_size_pairs_.size(); ++i) {
    if (key_value_size_pairs_[i].first == match_key_size && key_value_size_pairs_[i].second == match_value_size) {
      *lsm_tree_index = static_cast<int>(i);
      return Status::OK();
    }
  }

  return Status::NotFound("Matching LSM tree not found");
}

}  // namespace leveldb
