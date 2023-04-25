// feasdb.h
#pragma once

#include "db/db_impl.h"

#include <vector>
#include "leveldb/db.h"
#include "leveldb/options.h"
#include "leveldb/iterator.h"
#include "leveldb/write_batch.h"
#include "feasdb/dynamic_key_value_length_selector.h"
#include "feasdb/multilsm_tree_iterator.h"

namespace leveldb {

class FeasDBImpl : public DB {
 public:
  FeasDBImpl(const Options& options, const std::string& dbname);
  virtual ~FeasDBImpl();

  // Implement the DB interface
  virtual Status Put(const WriteOptions& options, const Slice& key, const Slice& value) override;
  virtual Status Delete(const WriteOptions& options, const Slice& key) override;
  virtual Status Write(const WriteOptions& options, WriteBatch* updates) override;
  virtual Status Get(const ReadOptions& options, const Slice& key, std::string* value) override;
  virtual Iterator* NewIterator(const ReadOptions& options) override;

 private:
  Options options_;
  std::string dbname_;
  std::vector<DB*> lsm_trees_;
  DB* var_length_lsm_tree_;
  DynamicKeyValueLengthSelector key_value_length_selector_;

  // Helper methods
  Status Open();
  void Close();
  size_t SelectLSMTreeIndex(const Slice& key, const Slice& value);
};

}  // namespace leveldb

