// multilsm_tree_iterator.h
#ifndef LEVELDB_MULTILSM_TREE_ITERATOR_H
#define LEVELDB_MULTILSM_TREE_ITERATOR_H

#include <vector>
#include "leveldb/iterator.h"

namespace leveldb {

class MultiLSMTreeIterator : public Iterator {
 public:
  explicit MultiLSMTreeIterator(const std::vector<Iterator*>& iterators);
  virtual ~MultiLSMTreeIterator();

  virtual bool Valid() const;
  virtual void SeekToFirst();
  virtual void SeekToLast();
  virtual void Seek(const Slice& target);
  virtual void Next();
  virtual void Prev();

  virtual Slice key() const;
  virtual Slice value() const;
  virtual Status status() const;

 private:
  std::vector<Iterator*> iterators_;
  size_t current_;

  // No copying allowed
  MultiLSMTreeIterator(const MultiLSMTreeIterator&);
  void operator=(const MultiLSMTreeIterator&);
};

}  // namespace leveldb

#endif  // LEVELDB_MULTILSM_TREE_ITERATOR_H
