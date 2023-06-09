// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// BlockBuilder generates blocks where keys are prefix-compressed:
//
// When we store a key, we drop the prefix shared with the previous
// string.  This helps reduce the space requirement significantly.
// Furthermore, once every K keys, we do not apply the prefix
// compression and store the entire key.  We call this a "restart
// point".  The tail end of the block stores the offsets of all of the
// restart points, and can be used to do a binary search when looking
// for a particular key.  Values are stored as-is (without compression)
// immediately following the corresponding key.
//
// An entry for a particular key-value pair has the form:
//     shared_bytes: varint32
//     unshared_bytes: varint32
//     value_length: varint32
//     key_delta: char[unshared_bytes]
//     value: char[value_length]
// shared_bytes == 0 for restart points.
//
// The trailer of the block has the form:
//     restarts: uint32[num_restarts]
//     num_restarts: uint32
// restarts[i] contains the offset within the block of the ith restart point.

#include "merge_test/fix_block_builder.h"

#include <algorithm>
#include <cassert>

#include "leveldb/comparator.h"
#include "leveldb/options.h"
#include "util/coding.h"

namespace leveldb {
FixBlockBuilder::FixBlockBuilder(const Options* options)
    : options_(options),
      key_length_(options->key_length),
      value_length_(options->value_length),
      num_entries_(0),
      finished_(false) {}

void FixBlockBuilder::Reset() {
  buffer_.clear();
  num_entries_ = 0;
  finished_ = false;
}

size_t FixBlockBuilder::CurrentSizeEstimate() const {
  return buffer_.size();
}

Slice FixBlockBuilder::Finish() {
  finished_ = true;
  return Slice(buffer_);
}

void FixBlockBuilder::Add(const Slice& key, const Slice& value) {
  assert(!finished_);
  assert(key.size() == key_length_);
  assert(value.size() == value_length_);

  buffer_.append(key.data(), key.size());
  buffer_.append(value.data(), value.size());

  num_entries_++;
}

}  // namespace leveldb
