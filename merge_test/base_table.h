// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_MERGE_TEST_BASE_TABLE_H_
#define STORAGE_LEVELDB_MERGE_TEST_BASE_TABLE_H_

#include <cstdint>

#include "leveldb/export.h"
#include "leveldb/iterator.h"

namespace leveldb {

class Block;
class BlockHandle;
class Footer;
struct Options;
class RandomAccessFile;
struct ReadOptions;
class TableCache;

// A Table is a sorted map from strings to strings.  Tables are
// immutable and persistent.  A Table may be safely accessed from
// multiple threads without external synchronization.
class BaseTable {
 public:
  // The client should delete the returned table when no longer needed.
  virtual ~BaseTable() {}

  // Returns a new iterator over the table contents.
  virtual Iterator* NewIterator(const ReadOptions& options) const = 0;

  // Given a key, return an approximate byte offset in the file where
  // the data for that key begins (or would begin if the key were
  // present in the file). The returned value is in terms of file
  // bytes, and so includes effects like compression of the underlying data.
  virtual uint64_t ApproximateOffsetOf(const Slice& key) const = 0;

  // Calls (*handle_result)(arg, ...) with the entry found after a call
  // to Seek(key).  May not make such a call if filter policy says
  // that key is not present.
  virtual Status InternalGet(const ReadOptions&, const Slice& key, void* arg,
                     void (*handle_result)(void* arg, const Slice& k,
                                           const Slice& v)) = 0;

  // Other common methods can be added here as needed
};
}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_FixTable_H_
