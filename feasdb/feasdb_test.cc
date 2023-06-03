#include <iostream>
#include <random>
#include <chrono>
#include <string>
#include <cassert>

#include "leveldb/db.h"

#include "feasdb/feasdb.h"

std::string random_string(std::size_t length) {
    //static const std::string alphanums = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    static const std::string alphanums = "0123456789";
    static std::mt19937 generator{std::random_device{}()};
    static std::uniform_int_distribution<std::size_t> distribution(0, alphanums.size() - 1);

    std::string str(length, '\0');
    for (std::size_t i = 0; i < length; ++i) {
        str[i] = alphanums[distribution(generator)];
    }

    return str;
}
void SingleTreeTest(bool fixed_enable){
    //输出传入参数
    std::cout << "fixed_enable: " << fixed_enable << std::endl;
    // Customize these parameters as needed.
    const int num_entries = 100000;
    const int seq_num_length = 8;
    const int key_length = 128;
    const int value_length = 128;

    leveldb::DB* db;
    leveldb::Options options;
    options.create_if_missing = true;
    options.key_length = key_length + seq_num_length;
    options.value_length = value_length;
    options.fix_block_enable = fixed_enable;
    leveldb::Status status = leveldb::DB::Open(options, "test_db", &db);
    assert(status.ok());

    auto start_time = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < num_entries; ++i) {
        std::string key = random_string(key_length);
        std::string value = random_string(value_length);

        status = db->Put(leveldb::WriteOptions(), key, value);
        assert(status.ok());
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::micro> elapsed = end_time - start_time;

    double write_latency = elapsed.count() / num_entries;
    double write_iops = num_entries / (elapsed.count() / 1e6) * 1e5;
    double write_bandwidth = (key_length + value_length) * num_entries / elapsed.count() * 1e6 / (1024 * 1024);

    std::cout << "Write latency (micros/op): " << write_latency << std::endl;
    std::cout << "Write IOPS (10^5 ops/s): " << write_iops << std::endl;
    std::cout << "Write bandwidth (MB/s): " << write_bandwidth << std::endl;

    delete db;

    // Destroy the test_db after completing the test
    leveldb::Status destroy_status = leveldb::DestroyDB("test_db", options);
}

std::string random_string_range(int min_length, int max_length) {
    int length = rand() % (max_length - min_length + 1) + min_length;
    std::string str;
    str.resize(length);
    for (int i = 0; i < length; ++i) {
        str[i] = ' ' + rand() % ('~' - ' ' + 1);
    }
    return str;
}

void MultiTreeTest() {
  // Customize these parameters as needed.
  const int num_entries = 500000;
  std::string dbname = "C:/Users/86158/Desktop/Code/LSM/dbData/test_db_multi_tree";
  leveldb::FeasDBImpl* db;
  leveldb::Options options;

  options.min_key_size = 99;
  options.max_key_size = 512;
  options.key_interval_size = 30;
  options.min_value_size = 99;
  options.max_value_size = 512;
  options.value_interval_size = 30;

  options.create_if_missing = true;
  db = new leveldb::FeasDBImpl(options, dbname);

  auto start_time = std::chrono::high_resolution_clock::now();

  size_t total_bytes = 0;  // Total bytes of all keys and values

  for (int i = 0; i < num_entries; ++i) {
    std::string key, value;
    int r = rand() % 100;
    if (r < 10) {  // 10% of entries
        key = random_string_range(1, 16);
        value = random_string_range(1, 16);
    } else if (r < 90) {  // 80% of entries
        key = random_string_range(100, 128);
        value = random_string_range(100, 128);
    } else {  // 10% of entries
        key = random_string_range(600, 1000);
        value = random_string_range(600, 1000);
    }

    total_bytes += key.size() + value.size();  // Accumulate the total bytes

    leveldb::Status status = db->Put(leveldb::WriteOptions(), key, value);
    assert(status.ok());
  }

  auto end_time = std::chrono::high_resolution_clock::now();
  std::chrono::duration<double, std::micro> elapsed = end_time - start_time;

  double write_latency = elapsed.count() / num_entries;
  double write_iops = num_entries / (elapsed.count() / 1e6) * 1e5;
  double write_bandwidth = total_bytes / elapsed.count() * 1e6 / (1024 * 1024);

  std::cout << "FeasDB Write latency (micros/op): " << write_latency << std::endl;
  std::cout << "FeasDB Write IOPS (10^5 ops/s): " << write_iops << std::endl;
  std::cout << "FeasDB Write bandwidth (MB/s): " << write_bandwidth << std::endl;

  db->DestroyDB();
  delete db;

}

void MultiTreeCompareTest() {
  // Customize these parameters as needed.
  const int num_entries = 500000;
  std::string dbname = "C:/Users/86158/Desktop/Code/LSM/dbData/test_db_multi_tree_compare";
  leveldb::DB* db;
  leveldb::Options options;
  options.create_if_missing = true;
  leveldb::Status status = leveldb::DB::Open(options, dbname, &db);
  assert(status.ok());

  auto start_time = std::chrono::high_resolution_clock::now();

  size_t total_bytes = 0;  // Total bytes of all keys and values

  for (int i = 0; i < num_entries; ++i) {
    std::string key, value;
    int r = rand() % 100;
    if (r < 10) {  // 10% of entries
        key = random_string_range(1, 16);
        value = random_string_range(1, 16);
    } else if (r < 90) {  // 80% of entries
        key = random_string_range(100, 128);
        value = random_string_range(100, 128);
    } else {  // 10% of entries
        key = random_string_range(600, 1000);
        value = random_string_range(600, 1000);
    }

    total_bytes += key.size() + value.size();  // Accumulate the total bytes

    status = db->Put(leveldb::WriteOptions(), key, value);
    assert(status.ok());
  }

  auto end_time = std::chrono::high_resolution_clock::now();
  std::chrono::duration<double, std::micro> elapsed = end_time - start_time;

  double write_latency = elapsed.count() / num_entries;
  double write_iops = num_entries / (elapsed.count() / 1e6) * 1e5;
  double write_bandwidth = total_bytes / elapsed.count() * 1e6 / (1024 * 1024);

  std::cout << "LevelDB Write latency (micros/op): " << write_latency << std::endl;
  std::cout << "LevelDB Write IOPS (10^5 ops/s): " << write_iops << std::endl;
  std::cout << "LevelDB Write bandwidth (MB/s): " << write_bandwidth << std::endl;

  delete db;
  leveldb::DestroyDB(dbname, options);
}


int main() {
    
    //SingleTreeTest(false);
    //SingleTreeTest(true);
    //MultiTreeTest();
    MultiTreeCompareTest();

    return 0;
}
