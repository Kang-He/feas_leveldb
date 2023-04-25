// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// TableBuilder provides the interface used to build a Table
// (an immutable and sorted map from keys to values).
//
// Multiple threads can invoke const methods on a TableBuilder without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same TableBuilder must use
// external synchronization.

#ifndef STORAGE_LEVELDB_MERGE_TEST_BASE_TABLE_BUILDER_H_
#define STORAGE_LEVELDB_MERGE_TEST_BASE_TABLE_BUILDER_H_

#include <cstdint>

#include "leveldb/export.h"
#include "leveldb/options.h"
#include "leveldb/status.h"
#include "merge_test/fix_block_builder.h"
#include "table/block_builder.h"

namespace leveldb {

class BlockBuilder;
class BlockHandle;
class WritableFile;

class LEVELDB_EXPORT BaseTableBuilder {
 public:
  virtual ~BaseTableBuilder() {}

  virtual Status ChangeOptions(const Options& options) = 0;
  virtual void Add(const Slice& key, const Slice& value) = 0;
  virtual void Flush() = 0;
  virtual Status status() const = 0;
  virtual Status Finish() = 0;
  virtual void Abandon() = 0;
  virtual uint64_t NumEntries() const = 0;
  virtual uint64_t FileSize() const = 0;

 protected:
  BaseTableBuilder() = default;
};


}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_TABLE_BUILDER_H_
