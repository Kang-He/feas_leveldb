// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_MERGE_TEST_FIX_BLOCK_BUILDER_H_
#define STORAGE_LEVELDB_MERGE_TEST_FIX_BLOCK_BUILDER_H_

#include <cstdint>
#include <vector>

#include "leveldb/slice.h"

namespace leveldb {

struct Options;

class FixBlockBuilder {
 public:
  FixBlockBuilder(const Options* options);

  FixBlockBuilder(const FixBlockBuilder&) = delete;
  FixBlockBuilder& operator=(const FixBlockBuilder&) = delete;

  void Reset();

  void Add(const Slice& key, const Slice& value);

  Slice Finish();

  size_t CurrentSizeEstimate() const;

  bool empty() const { return buffer_.empty(); }

 private:
  const Options* options_;
  std::string buffer_;
  uint32_t key_length_;
  uint32_t value_length_;
  size_t num_entries_;
  bool finished_;
};



}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_BLOCK_BUILDER_H_
