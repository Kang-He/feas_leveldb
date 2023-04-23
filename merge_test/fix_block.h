// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_MERGE_TEST_FIX_BLOCK_H_
#define STORAGE_LEVELDB_MERGE_TEST_FIX_BLOCK_H_

#include <cstddef>
#include <cstdint>

#include "leveldb/iterator.h"

namespace leveldb {

struct BlockContents;
class Comparator;

class FixBlock {
 public:
  // Initialize the block with the specified contents.
  explicit FixBlock(const BlockContents& contents, uint32_t key_length, uint32_t value_length);

  FixBlock(const FixBlock&) = delete;
  FixBlock& operator=(const FixBlock&) = delete;

  ~FixBlock();

  size_t size() const { return size_; }
  Iterator* NewIterator(const Comparator* comparator);

 private:
  class Iter;

  uint32_t NumRestarts() const;

  const char* data_;
  size_t size_;
  uint32_t key_length_;  
  uint32_t value_length_;
  bool owned_;               // Block owns data_[]
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_BLOCK_H_
