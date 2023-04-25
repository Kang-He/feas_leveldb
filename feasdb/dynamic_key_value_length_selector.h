#ifndef LEVELDB_DYNAMIC_KEY_VALUE_LENGTH_SELECTOR_H_
#define LEVELDB_DYNAMIC_KEY_VALUE_LENGTH_SELECTOR_H_

#include <vector>
#include "leveldb/status.h"
#include "leveldb/slice.h"

namespace leveldb {

class DynamicKeyValueLengthSelector {
 public:
  DynamicKeyValueLengthSelector(int min_key_size, int max_key_size, int key_interval_size,
                                 int min_value_size, int max_value_size, int value_interval_size);
  ~DynamicKeyValueLengthSelector();

  Status GetMatchingLSMTree(const Slice& key, const Slice& value, int* lsm_tree_index);

 private:
  int min_key_size_;
  int max_key_size_;
  int key_interval_size_;
  int min_value_size_;
  int max_value_size_;
  int value_interval_size_;
  std::vector<std::pair<int, int>> key_value_size_pairs_;
};

}  // namespace leveldb

#endif  // LEVELDB_DYNAMIC_KEY_VALUE_LENGTH_SELECTOR_H_
