// multilsm_tree_iterator.cc
#include "feasdb/multilsm_tree_iterator.h"

namespace leveldb {

MultiLSMTreeIterator::MultiLSMTreeIterator(const std::vector<Iterator*>& iterators)
    : iterators_(iterators), current_(0) {
  SeekToFirst();
}

MultiLSMTreeIterator::~MultiLSMTreeIterator() {
  for (auto& iterator : iterators_) {
    delete iterator;
  }
}

bool MultiLSMTreeIterator::Valid() const {
  return current_ < iterators_.size() && iterators_[current_]->Valid();
}

void MultiLSMTreeIterator::SeekToFirst() {
  for (auto& iterator : iterators_) {
    iterator->SeekToFirst();
  }
  current_ = 0;
  while (current_ + 1 < iterators_.size() && !iterators_[current_]->Valid()) {
    ++current_;
  }
}

void MultiLSMTreeIterator::SeekToLast() {
  for (auto& iterator : iterators_) {
    iterator->SeekToLast();
  }
  current_ = iterators_.size() - 1;
  while (current_ > 0 && !iterators_[current_]->Valid()) {
    --current_;
  }
}

void MultiLSMTreeIterator::Seek(const Slice& target) {
  for (auto& iterator : iterators_) {
    iterator->Seek(target);
  }
  current_ = 0;
  while (current_ + 1 < iterators_.size() && !iterators_[current_]->Valid()) {
    ++current_;
  }
}

void MultiLSMTreeIterator::Next() {
  if (Valid()) {
    iterators_[current_]->Next();
    while (current_ + 1 < iterators_.size() && !iterators_[current_]->Valid()) {
      ++current_;
    }
  }
}

void MultiLSMTreeIterator::Prev() {
  if (Valid()) {
    iterators_[current_]->Prev();
    while (current_ > 0 && !iterators_[current_]->Valid()) {
      --current_;
    }
  }
}

Slice MultiLSMTreeIterator::key() const {
  if (Valid()) {
    return iterators_[current_]->key();
  } else {
    return Slice();
  }
}

Slice MultiLSMTreeIterator::value() const {
  if (Valid()) {
    return iterators_[current_]->value();
  } else {
    return Slice();
  }
}

Status MultiLSMTreeIterator::status() const {
  for (const auto& iterator : iterators_) {
    Status s = iterator->status();
    if (!s.ok()) {
      return s;
    }
  }
  return Status::OK();
}

}  // namespace leveldb

