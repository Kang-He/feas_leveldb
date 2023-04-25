#include <iostream>
#include <random>
#include <chrono>
#include <string>
#include <cassert>
#include <leveldb/db.h>

std::string random_string(std::size_t length) {
    static const std::string alphanums = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    static std::mt19937 generator{std::random_device{}()};
    static std::uniform_int_distribution<std::size_t> distribution(0, alphanums.size() - 1);

    std::string str(length, '\0');
    for (std::size_t i = 0; i < length; ++i) {
        str[i] = alphanums[distribution(generator)];
    }

    return str;
}

int main() {
    // Customize these parameters as needed.
    const int num_entries = 1000;
    const int seq_num_length = 8;
    const int key_length = 64 - seq_num_length;
    const int value_length = 64;

    leveldb::DB* db;
    leveldb::Options options;
    options.create_if_missing = true;
    options.key_length = key_length;
    options.value_length = value_length;
    options.fix_block_enable = true;
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

    return 0;
}
