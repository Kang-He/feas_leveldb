// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Decodes the blocks generated by block_builder.cc.

#include "merge_test/fix_block.h"

#include <algorithm>
#include <cstdint>
#include <vector>

#include "leveldb/comparator.h"
#include "table/format.h"
#include "util/coding.h"
#include "util/logging.h"

namespace leveldb {

inline uint32_t FixBlock::NumRestarts() const {
  assert(size_ >= sizeof(uint32_t));
  return DecodeFixed32(data_ + size_ - sizeof(uint32_t));
}

FixBlock::FixBlock(const BlockContents& contents, uint32_t key_length, uint32_t value_length)
    : data_(contents.data.data()),
      size_(contents.data.size()),
      owned_(contents.heap_allocated), 
      key_length_(key_length),
      value_length_(value_length){
  if (size_ < sizeof(uint32_t)) {
    size_ = 0;  // Error marker
  } 
}

FixBlock::~FixBlock() {
  if (owned_) {
    delete[] data_;
  }
}

// Helper routine: decode the next block entry starting at "p",
// storing the number of shared key bytes, non_shared key bytes,
// and the length of the value in "*shared", "*non_shared", and
// "*value_length", respectively.  Will not dereference past "limit".
//
// If any errors are detected, returns nullptr.  Otherwise, returns a
// pointer to the key delta (just past the three decoded values).
static inline const char* DecodeEntry(const char* p, const char* limit,
                                      uint32_t* shared, uint32_t* non_shared,
                                      uint32_t* value_length) {
  if (limit - p < 3) return nullptr;
  *shared = reinterpret_cast<const uint8_t*>(p)[0];
  *non_shared = reinterpret_cast<const uint8_t*>(p)[1];
  *value_length = reinterpret_cast<const uint8_t*>(p)[2];
  if ((*shared | *non_shared | *value_length) < 128) {
    // Fast path: all three values are encoded in one byte each
    p += 3;
  } else {
    if ((p = GetVarint32Ptr(p, limit, shared)) == nullptr) return nullptr;
    if ((p = GetVarint32Ptr(p, limit, non_shared)) == nullptr) return nullptr;
    if ((p = GetVarint32Ptr(p, limit, value_length)) == nullptr) return nullptr;
  }

  if (static_cast<uint32_t>(limit - p) < (*non_shared + *value_length)) {
    return nullptr;
  }
  return p;
}

class FixBlock::Iter : public Iterator {
 private:
  const Comparator* const comparator_;
  const char* const data_;       // underlying block contents
  const size_t size_;            // block size
  uint32_t key_length_;
  uint32_t value_length_;
  uint32_t num_entries_;

  // current_ is offset in data_ of current entry.  >= size_ if !Valid
  uint32_t current_;
  Status status_;

  inline int Compare(const Slice& a, const Slice& b) const {
    return comparator_->Compare(a, b);
  }

  // Return the offset in data_ just past the end of the current entry.
  inline uint32_t NextEntryOffset() const {
    return current_ + key_length_ + value_length_;
  }
  inline uint32_t LastEntryOffset() const {
    return current_ - key_length_ - value_length_;
  }

 public:
  Iter(const Comparator* comparator, const char* data, size_t size, uint32_t key_length,
       uint32_t value_length, uint32_t num_entries)
      : comparator_(comparator),
        data_(data),
        size_(size),
        key_length_(key_length),
        value_length_(value_length),
        num_entries_(num_entries),
        current_(0) {
  }

  bool Valid() const override { return current_ < size_; }
  Status status() const override { return status_; }
  Slice key() const override {
    assert(Valid());
    return Slice(data_ + current_, key_length_);
  }
  Slice value() const override {
    assert(Valid());
    return Slice(data_ + current_ + key_length_, value_length_);
  }

  void Next() override {
    assert(Valid());
    current_ = NextEntryOffset();
  }

  void Prev() override {
    assert(Valid());
    current_ = LastEntryOffset();
  }

  void Seek(const Slice& target) override {
    uint32_t left = 0;
    uint32_t right = num_entries_;

    while (left < right) {
      uint32_t mid = left + (right - left) / 2;
      uint32_t mid_offset = mid * (key_length_ + value_length_);
      Slice mid_key(data_ + mid_offset, key_length_);

      int cmp = Compare(mid_key, target);
      if (cmp < 0) {
        left = mid + 1;
      } else {
        right = mid;
      }
    }

    current_ = left * (key_length_ + value_length_);
    if (current_ >= size_) {
      // 如果超出范围，设置为无效
      current_ = size_;
    }
  }

  void SeekToFirst() override {
    current_ = 0;
  }

  void SeekToLast() override {
    current_ = (num_entries_ - 1) * (key_length_ + value_length_);
  }
};

Iterator* FixBlock::NewIterator(const Comparator* comparator) {
  return new Iter(comparator, data_, size_, key_length_, value_length_, size_ / (key_length_ + value_length_));
}


}  // namespace leveldb
