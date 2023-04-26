// feasdb.h
#ifndef STORAGE_LEVELDB_FEASDB_FEASDB_H_
#define STORAGE_LEVELDB_FEASDB_FEASDB_H_

//#include "db/db_impl.h"

#include <vector>
#include <map>
#include <utility>

#include "leveldb/db.h"
#include "leveldb/options.h"
#include "leveldb/iterator.h"
#include "leveldb/write_batch.h"
#include "db/dbformat.h"
#include "db/log_writer.h"
#include "db/snapshot.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "port/port.h"
#include "port/thread_annotations.h"

#include "feasdb/multilsm_tree_iterator.h"


namespace leveldb {

class FeasDBImpl {
 public:
  FeasDBImpl(const Options& options, const std::string& dbname);
  ~FeasDBImpl();

  // Implement the DB interface
  Status Put(const WriteOptions& options, const Slice& key, const Slice& value) ;
  Status Delete(const WriteOptions& options, const Slice& key) ;
  Status Write(const WriteOptions& options, WriteBatch* updates) ;
  Status Get(const ReadOptions& options, const Slice& key, std::string* value) ;
  Iterator* NewIterator(const ReadOptions& options) ;

 private:
  // Options and dbname used to open the database
  Options options_;
  std::string dbname_;

  // LSM trees
  std::map<std::pair<int, int>, DB*> lsm_trees_;
  DB* var_length_lsm_tree_;

  // Parameters
  int min_key_size_ = 16;
  int max_key_size_ = 512;
  int key_interval_size_ = 16;
  int min_value_size_ = 16;
  int max_value_size_ = 512;
  int value_interval_size_ = 16;
  int num_lsm_trees_ = 0;
  const int seq_num_length_ = 8;

  // Helper methods
  Status Open();
  void Close();
  void CheckSizeParameters();
  int MatchKeySize(int key_size);
  int MatchValueSize(int value_size);
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_FEASDB_FEASDB_H_

