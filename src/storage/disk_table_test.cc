//
// table_test.cc
// Copyright (C) 2017 4paradigm.com
// Author yangjun
// Date 2018-01-07
//

#include "storage/disk_table.h"
#include <iostream>
#include <utility>
#include "base/file_util.h"
#include "gtest/gtest.h"
#include "base/glog_wapper.h"  // NOLINT
#include "common/timer.h"    // NOLINT
#include "codec/schema_codec.h"
#include <gflags/gflags.h>
#include "codec/sdk_codec.h"
#include "storage/ticket.h"
#include "test/util.h"

using ::openmldb::codec::SchemaCodec;

DECLARE_string(ssd_root_path);
DECLARE_string(hdd_root_path);
DECLARE_uint32(max_traverse_cnt);
DECLARE_int32(gc_safe_offset);

namespace openmldb {
namespace storage {

inline uint32_t GenRand() {
    srand((unsigned)time(NULL));
    return rand() % 10000000 + 1;
}

void RemoveData(const std::string& path) {
    ::openmldb::base::RemoveDir(path + "/data");
    ::openmldb::base::RemoveDir(path);
    ::openmldb::base::RemoveDir(FLAGS_hdd_root_path);
    ::openmldb::base::RemoveDir(FLAGS_ssd_root_path);
}

class DiskTableTest : public ::testing::Test {
 public:
    DiskTableTest() {}
    ~DiskTableTest() {}
};

TEST_F(DiskTableTest, ParseKeyAndTs) {
    std::string combined_key = CombineKeyTs("abcdexxx11", 1552619498000);
    std::string key;
    uint64_t ts;
    ASSERT_EQ(0, ParseKeyAndTs(combined_key, key, ts));
    ASSERT_EQ("abcdexxx11", key);
    ASSERT_EQ(1552619498000, (int64_t)ts);
    combined_key = CombineKeyTs("abcdexxx11", 1);
    ASSERT_EQ(0, ParseKeyAndTs(combined_key, key, ts));
    ASSERT_EQ("abcdexxx11", key);
    ASSERT_EQ(1, (int64_t)ts);
    combined_key = CombineKeyTs("0", 0);
    ASSERT_EQ(0, ParseKeyAndTs(combined_key, key, ts));
    ASSERT_EQ("0", key);
    ASSERT_EQ(0, (int64_t)ts);
    ASSERT_EQ(-1, ParseKeyAndTs("abc", key, ts));
    combined_key = CombineKeyTs("", 1122);
    ASSERT_EQ(0, ParseKeyAndTs(combined_key, key, ts));
    ASSERT_TRUE(key.empty());
    ASSERT_EQ(1122, (int64_t)ts);
}

TEST_F(DiskTableTest, Put) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    DiskTable* table = new DiskTable(
        "yjtable1", 1, 1, mapping, 10, ::openmldb::type::TTLType::kAbsoluteTime,
        ::openmldb::common::StorageMode::kHDD, FLAGS_hdd_root_path);
    ASSERT_TRUE(table->Init());
    for (int idx = 0; idx < 100; idx++) {
        std::string key = "test" + std::to_string(idx);
        uint64_t ts = 9537;
        for (int k = 0; k < 10; k++) {
            ASSERT_TRUE(table->Put(key, ts + k, "value", 5));
        }
    }
    std::string raw_key = "test35";
    Ticket ticket;
    TableIterator* it = table->NewIterator(raw_key, ticket);
    it->SeekToFirst();
    for (int k = 0; k < 10; k++) {
        ASSERT_TRUE(it->Valid());
        std::string pk = it->GetPK();
        ASSERT_EQ(pk, raw_key);
        ASSERT_EQ(9537 + 9 - k, (int64_t)(it->GetKey()));
        std::string value1 = it->GetValue().ToString();
        ASSERT_EQ("value", value1);
        it->Next();
    }
    ASSERT_FALSE(it->Valid());
    delete it;
    delete table;
    std::string path = FLAGS_hdd_root_path + "/1_1";
    RemoveData(path);
}

TEST_F(DiskTableTest, MultiDimensionPut) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    mapping.insert(std::make_pair("idx1", 1));
    mapping.insert(std::make_pair("idx2", 2));
    DiskTable* table = new DiskTable(
        "yjtable2", 2, 1, mapping, 10, ::openmldb::type::TTLType::kAbsoluteTime,
        ::openmldb::common::StorageMode::kHDD, FLAGS_hdd_root_path);
    ASSERT_TRUE(table->Init());
    ASSERT_EQ(3, (int64_t)table->GetIdxCnt());
    //    ASSERT_EQ(0, table->GetRecordIdxCnt());
    //    ASSERT_EQ(0, table->GetRecordCnt());
    Dimensions dimensions;
    ::openmldb::api::Dimension* d0 = dimensions.Add();
    d0->set_key("yjdim0");
    d0->set_idx(0);

    ::openmldb::api::Dimension* d1 = dimensions.Add();
    d1->set_key("yjdim1");
    d1->set_idx(1);

    ::openmldb::api::Dimension* d2 = dimensions.Add();
    d2->set_key("yjdim2");
    d2->set_idx(2);
    bool ok = table->Put(1, "yjtestvalue", dimensions);
    ASSERT_TRUE(ok);
    //    ASSERT_EQ(3, table->GetRecordIdxCnt());
    Ticket ticket;
    TableIterator* it = table->NewIterator(0, "yjdim0", ticket);
    it->SeekToFirst();
    ASSERT_TRUE(it->Valid());
    uint64_t ts = it->GetKey();
    ASSERT_EQ(1, (int64_t)ts);
    std::string value1 = it->GetValue().ToString();
    ASSERT_EQ("yjtestvalue", value1);
    it->Next();
    ASSERT_FALSE(it->Valid());
    delete it;

    it = table->NewIterator(1, "yjdim1", ticket);
    it->SeekToFirst();
    ASSERT_TRUE(it->Valid());
    ASSERT_EQ(1, (int64_t)it->GetKey());
    value1 = it->GetValue().ToString();
    ASSERT_EQ("yjtestvalue", value1);
    it->Next();
    ASSERT_FALSE(it->Valid());
    delete it;

    it = table->NewIterator(2, "yjdim2", ticket);
    it->SeekToFirst();
    ASSERT_TRUE(it->Valid());
    ts = it->GetKey();
    ASSERT_EQ(1, (int64_t)ts);
    value1 = it->GetValue().ToString();
    ASSERT_EQ("yjtestvalue", value1);
    it->Next();
    ASSERT_FALSE(it->Valid());
    delete it;

    dimensions.Clear();
    d0 = dimensions.Add();
    d0->set_key("key2");
    d0->set_idx(0);

    d1 = dimensions.Add();
    d1->set_key("key1");
    d1->set_idx(1);

    d2 = dimensions.Add();
    d2->set_key("dimxxx1");
    d2->set_idx(2);
    ASSERT_TRUE(table->Put(2, "value2", dimensions));

    it = table->NewIterator(0, "key2", ticket);
    it->SeekToFirst();
    ASSERT_TRUE(it->Valid());
    ts = it->GetKey();
    ASSERT_EQ(2, (int64_t)ts);
    value1 = it->GetValue().ToString();
    ASSERT_EQ("value2", value1);
    delete it;

    it = table->NewIterator(1, "key1", ticket);
    it->Seek(2);
    ASSERT_TRUE(it->Valid());
    delete it;

    std::string val;
    ASSERT_TRUE(table->Get(1, "key1", 2, val));
    ASSERT_EQ("value2", val);

    it = table->NewIterator(2, "dimxxx1", ticket);
    it->SeekToFirst();
    ASSERT_TRUE(it->Valid());
    ts = it->GetKey();
    ASSERT_EQ(2, (int64_t)ts);
    value1 = it->GetValue().ToString();
    ASSERT_EQ("value2", value1);
    delete it;

    it = table->NewIterator(1, "key1", ticket);
    it->Seek(2);
    ASSERT_TRUE(it->Valid());
    ts = it->GetKey();
    ASSERT_EQ(2, (int64_t)ts);
    value1 = it->GetValue().ToString();
    ASSERT_EQ("value2", value1);
    delete it;

    it = table->NewIterator(1, "key1", ticket);
    it->SeekToFirst();
    ASSERT_TRUE(it->Valid());
    ASSERT_EQ(2, (int64_t)it->GetKey());
    value1 = it->GetValue().ToString();
    ASSERT_EQ("value2", value1);
    delete it;

    delete table;
    std::string path = FLAGS_hdd_root_path + "/2_1";
    RemoveData(path);
}

TEST_F(DiskTableTest, LongPut) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    mapping.insert(std::make_pair("idx1", 1));
    DiskTable* table = new DiskTable(
        "yjtable3", 3, 1, mapping, 10, ::openmldb::type::TTLType::kAbsoluteTime,
        ::openmldb::common::StorageMode::kSSD, FLAGS_ssd_root_path);
    ASSERT_TRUE(table->Init());
    for (int idx = 0; idx < 10; idx++) {
        Dimensions dimensions;
        ::openmldb::api::Dimension* d0 = dimensions.Add();
        d0->set_key("ThisIsAVeryLongKeyWhichLengthIsMoreThan40" +
                    std::to_string(idx));
        d0->set_idx(0);

        ::openmldb::api::Dimension* d1 = dimensions.Add();
        d1->set_key("ThisIsAnotherVeryLongKeyWhichLengthIsMoreThan40" +
                    std::to_string(idx));
        d1->set_idx(1);
        uint64_t ts = 1581931824136;
        for (int k = 0; k < 10; k++) {
            ASSERT_TRUE(table->Put(
                ts + k, "ThisIsAVeryLongKeyWhichLengthIsMoreThan40'sValue",
                dimensions));
        }
    }
    for (int idx = 0; idx < 10; idx++) {
        std::string raw_key0 =
            "ThisIsAVeryLongKeyWhichLengthIsMoreThan40" + std::to_string(idx);
        std::string raw_key1 =
            "ThisIsAnotherVeryLongKeyWhichLengthIsMoreThan40" +
            std::to_string(idx);
        Ticket ticket0, ticket1;
        TableIterator* it0 = table->NewIterator(0, raw_key0, ticket0);
        TableIterator* it1 = table->NewIterator(1, raw_key1, ticket1);

        it0->SeekToFirst();
        it1->SeekToFirst();
        for (int k = 0; k < 10; k++) {
            ASSERT_TRUE(it0->Valid());
            ASSERT_TRUE(it1->Valid());
            std::string pk0 = it0->GetPK();
            std::string pk1 = it1->GetPK();
            ASSERT_EQ(pk0, raw_key0);
            ASSERT_EQ(pk1, raw_key1);
            ASSERT_EQ(1581931824136 + 9 - k, (int64_t)it0->GetKey());
            ASSERT_EQ(1581931824136 + 9 - k, (int64_t)it1->GetKey());
            std::string value0 = it0->GetValue().ToString();
            std::string value1 = it1->GetValue().ToString();
            ASSERT_EQ("ThisIsAVeryLongKeyWhichLengthIsMoreThan40'sValue",
                      value0);
            ASSERT_EQ("ThisIsAVeryLongKeyWhichLengthIsMoreThan40'sValue",
                      value1);
            it0->Next();
            it1->Next();
        }
        ASSERT_FALSE(it0->Valid());
        ASSERT_FALSE(it1->Valid());
        delete it0;
        delete it1;
    }
    delete table;
    std::string path = FLAGS_ssd_root_path + "/3_1";
    RemoveData(path);
}

TEST_F(DiskTableTest, Delete) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    mapping.insert(std::make_pair("idx1", 1));
    mapping.insert(std::make_pair("idx2", 2));
    DiskTable* table = new DiskTable(
        "yjtable2", 4, 1, mapping, 10, ::openmldb::type::TTLType::kAbsoluteTime,
        ::openmldb::common::StorageMode::kHDD, FLAGS_hdd_root_path);
    ASSERT_TRUE(table->Init());
    for (int idx = 0; idx < 10; idx++) {
        std::string key = "test" + std::to_string(idx);
        uint64_t ts = 9537;
        for (int k = 0; k < 10; k++) {
            ASSERT_TRUE(table->Put(key, ts + k, "value", 5));
        }
    }
    Ticket ticket;
    TableIterator* it = table->NewIterator("test6", ticket);
    it->SeekToFirst();
    int count = 0;
    while (it->Valid()) {
        std::string pk = it->GetPK();
        ASSERT_EQ("test6", pk);
        count++;
        it->Next();
    }
    ASSERT_EQ(count, 10);
    delete it;
    table->Delete("test6", 0);
    it = table->NewIterator("test6", ticket);
    it->SeekToFirst();
    ASSERT_FALSE(it->Valid());
    delete it;

    delete table;
    std::string path = FLAGS_hdd_root_path + "/4_1";
    RemoveData(path);
}

TEST_F(DiskTableTest, TraverseIterator) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    DiskTable* table = new DiskTable(
        "t1", 5, 1, mapping, 0, ::openmldb::type::TTLType::kAbsoluteTime,
        ::openmldb::common::StorageMode::kHDD, FLAGS_hdd_root_path);
    ASSERT_TRUE(table->Init());
    for (int idx = 0; idx < 100; idx++) {
        std::string key = "test" + std::to_string(idx);
        uint64_t ts = 9537;
        for (int k = 0; k < 10; k++) {
            ASSERT_TRUE(table->Put(key, ts + k, "value", 5));
            if (idx == 10 && k == 5) {
                ASSERT_TRUE(table->Put(key, ts + k, "valu9", 5));
                ASSERT_TRUE(table->Put(key, ts + k, "valu8", 5));
            }
        }
    }
    TableIterator* it = table->NewTraverseIterator(0);
    it->SeekToFirst();
    int count = 0;
    while (it->Valid()) {
        std::string pk = it->GetPK();
        count++;
        it->Next();
    }
    ASSERT_FALSE(it->Valid());
    ASSERT_EQ(1000, count);

    it->Seek("test90", 9543);
    count = 0;
    while (it->Valid()) {
        if (count == 0) {
            std::string pk = it->GetPK();
            ASSERT_EQ("test90", pk);
            ASSERT_EQ(9542, (int64_t)it->GetKey());
        }
        count++;
        it->Next();
    }
    ASSERT_EQ(96, count);

    it->Seek("test90", 9537);
    count = 0;
    while (it->Valid()) {
        if (count == 0) {
            std::string pk = it->GetPK();
            ASSERT_EQ("test91", pk);
            ASSERT_EQ(9546, (int64_t)it->GetKey());
        }
        count++;
        it->Next();
    }
    ASSERT_EQ(90, count);

    it->Seek("test90", 9530);
    count = 0;
    while (it->Valid()) {
        if (count == 0) {
            std::string pk = it->GetPK();
            ASSERT_EQ("test91", pk);
            ASSERT_EQ(9546, (int64_t)it->GetKey());
        }
        count++;
        it->Next();
    }
    ASSERT_EQ(90, count);

    ASSERT_TRUE(table->Put("test98", 9548, "valu8", 5));
    it->Seek("test98", 9547);
    count = 0;
    while (it->Valid()) {
        if (count == 0) {
            std::string pk = it->GetPK();
            ASSERT_EQ("test98", pk);
            ASSERT_EQ(9546, (int64_t)it->GetKey());
        }
        count++;
        it->Next();
    }
    ASSERT_EQ(20, count);
    std::string val;
    ASSERT_TRUE(table->Get(0, "test98", 9548, val));
    ASSERT_EQ("valu8", val);
    delete it;
    delete table;
    std::string path = FLAGS_hdd_root_path + "/5_1";
    RemoveData(path);
}

TEST_F(DiskTableTest, TraverseIteratorCount) {
    uint32_t old_max_traverse = FLAGS_max_traverse_cnt;
    FLAGS_max_traverse_cnt = 50;
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    DiskTable* table = new DiskTable(
        "t1", 6, 1, mapping, 0, ::openmldb::type::TTLType::kAbsoluteTime,
        ::openmldb::common::StorageMode::kHDD, FLAGS_hdd_root_path);
    ASSERT_TRUE(table->Init());
    for (int idx = 0; idx < 100; idx++) {
        std::string key = "test" + std::to_string(idx);
        uint64_t ts = 9537;
        for (int k = 0; k < 10; k++) {
            ASSERT_TRUE(table->Put(key, ts + k, "value", 5));
        }
    }
    TableIterator* it = table->NewTraverseIterator(0);
    it->SeekToFirst();
    int count = 0;
    while (it->Valid()) {
        std::string pk = it->GetPK();
        count++;
        it->Next();
    }
    ASSERT_FALSE(it->Valid());
    ASSERT_EQ(49, count);
    delete it;

    it = table->NewTraverseIterator(0);
    it->Seek("test90", 9543);
    count = 0;
    while (it->Valid()) {
        if (count == 0) {
            std::string pk = it->GetPK();
            ASSERT_EQ("test90", pk);
            ASSERT_EQ(9542, (int64_t)it->GetKey());
        }
        count++;
        it->Next();
    }
    ASSERT_EQ(48, count);
    delete it;

    it = table->NewTraverseIterator(0);
    it->Seek("test90", 9537);
    count = 0;
    while (it->Valid()) {
        if (count == 0) {
            std::string pk = it->GetPK();
            ASSERT_EQ("test91", pk);
            ASSERT_EQ(9546, (int64_t)it->GetKey());
        }
        count++;
        it->Next();
    }
    ASSERT_EQ(48, count);
    delete it;

    it = table->NewTraverseIterator(0);
    it->Seek("test90", 9530);
    count = 0;
    while (it->Valid()) {
        if (count == 0) {
            std::string pk = it->GetPK();
            ASSERT_EQ("test91", pk);
            ASSERT_EQ(9546, (int64_t)it->GetKey());
        }
        count++;
        it->Next();
    }
    ASSERT_EQ(49, count);

    delete it;
    delete table;
    FLAGS_max_traverse_cnt = old_max_traverse;
    std::string path = FLAGS_hdd_root_path + "/6_1";
    RemoveData(path);
}

TEST_F(DiskTableTest, TraverseIteratorCountTTL) {
    uint32_t old_max_traverse = FLAGS_max_traverse_cnt;
    FLAGS_max_traverse_cnt = 50;
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    DiskTable* table = new DiskTable(
        "t1", 7, 1, mapping, 5, ::openmldb::type::TTLType::kAbsoluteTime,
        ::openmldb::common::StorageMode::kHDD, FLAGS_hdd_root_path);
    ASSERT_TRUE(table->Init());
    
    uint64_t cur_time = ::baidu::common::timer::get_micros() / 1000;
    for (int idx = 0; idx < 10; idx++) {
        std::string key = "test" + std::to_string(idx);
        uint64_t ts = cur_time;
        for (int k = 0; k < 60; k++) {
            if (idx == 0) {
                if (k < 30) {
                    ASSERT_TRUE(
                        table->Put(key, ts + k - 6 * 1000 * 60, "value", 5));
                }
                continue;
            }
            if (k < 30) {
                ASSERT_TRUE(table->Put(key, ts + k, "value", 5));
            } else {
                ASSERT_TRUE(
                    table->Put(key, ts + k - 6 * 1000 * 60, "value", 5));
            }
        }
    }
    TableIterator* it = table->NewTraverseIterator(0);
    it->SeekToFirst();
    int count = 0;
    while (it->Valid()) {
        count++;
        it->Next();
    }
    ASSERT_FALSE(it->Valid());
    ASSERT_EQ(47, count);
    ASSERT_EQ(50, (int64_t)it->GetCount());
    delete it;

    it = table->NewTraverseIterator(0);
    it->Seek("test5", cur_time + 10);
    count = 0;
    while (it->Valid()) {
        count++;
        it->Next();
    }
    ASSERT_EQ(46, count);
    ASSERT_EQ(50, (int64_t)it->GetCount());
    delete it;
    delete table;
    FLAGS_max_traverse_cnt = old_max_traverse;
    std::string path = FLAGS_hdd_root_path + "/7_1";
    RemoveData(path);
}

TEST_F(DiskTableTest, TraverseIteratorLatest) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    DiskTable* table = new DiskTable(
        "t1", 8, 1, mapping, 3, ::openmldb::type::TTLType::kLatestTime,
        ::openmldb::common::StorageMode::kHDD, FLAGS_hdd_root_path);
    ASSERT_TRUE(table->Init());
    for (int idx = 0; idx < 100; idx++) {
        std::string key = "test" + std::to_string(idx);
        uint64_t ts = 9537;
        for (int k = 0; k < 5; k++) {
            ASSERT_TRUE(table->Put(key, ts + k, "value", 5));
            if (idx == 10 && k == 2) {
                ASSERT_TRUE(table->Put(key, ts + k, "valu9", 5));
                ASSERT_TRUE(table->Put(key, ts + k, "valu8", 5));
            }
        }
    }
    TableIterator* it = table->NewTraverseIterator(0);
    it->SeekToFirst();
    int count = 0;
    while (it->Valid()) {
        std::string pk = it->GetPK();
        count++;
        it->Next();
    }
    ASSERT_FALSE(it->Valid());
    ASSERT_EQ(300, count);

    it->Seek("test90", 9541);
    count = 0;
    while (it->Valid()) {
        if (count == 0) {
            std::string pk = it->GetPK();
            ASSERT_EQ("test90", pk);
            ASSERT_EQ(9540, (int64_t)it->GetKey());
        }
        count++;
        it->Next();
    }
    ASSERT_EQ(29, count);

    it->Seek("test90", 9537);
    count = 0;
    while (it->Valid()) {
        if (count == 0) {
            std::string pk = it->GetPK();
            ASSERT_EQ("test91", pk);
            ASSERT_EQ(9541, (int64_t)it->GetKey());
        }
        count++;
        it->Next();
    }
    ASSERT_EQ(27, count);
    it->Seek("test90", 9530);
    count = 0;
    while (it->Valid()) {
        if (count == 0) {
            std::string pk = it->GetPK();
            ASSERT_EQ("test91", pk);
            ASSERT_EQ(9541, (int64_t)it->GetKey());
        }
        count++;
        it->Next();
    }
    ASSERT_EQ(27, count);
    delete it;
    delete table;
    std::string path = FLAGS_hdd_root_path + "/8_1";
    RemoveData(path);
}

TEST_F(DiskTableTest, Load) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    DiskTable* table = new DiskTable(
        "t1", 9, 1, mapping, 10, ::openmldb::type::TTLType::kAbsoluteTime,
        ::openmldb::common::StorageMode::kHDD, FLAGS_hdd_root_path);
    ASSERT_TRUE(table->Init());
    for (int idx = 0; idx < 100; idx++) {
        std::string key = "test" + std::to_string(idx);
        uint64_t ts = 9537;
        for (int k = 0; k < 10; k++) {
            ASSERT_TRUE(table->Put(key, ts + k, "value", 5));
        }
    }
    std::string raw_key = "test35";
    Ticket ticket;
    TableIterator* it = table->NewIterator(raw_key, ticket);
    it->SeekToFirst();
    for (int k = 0; k < 10; k++) {
        ASSERT_TRUE(it->Valid());
        std::string pk = it->GetPK();
        ASSERT_EQ(pk, raw_key);
        ASSERT_EQ(9537 + 9 - k, (int64_t)it->GetKey());
        std::string value1 = it->GetValue().ToString();
        ASSERT_EQ("value", value1);
        it->Next();
    }
    ASSERT_FALSE(it->Valid());
    delete it;
    delete table;

    table = new DiskTable(
        "t1", 9, 1, mapping, 10, ::openmldb::type::TTLType::kAbsoluteTime,
        ::openmldb::common::StorageMode::kHDD, FLAGS_hdd_root_path);
    ASSERT_TRUE(table->LoadTable());
    raw_key = "test35";
    it = table->NewIterator(raw_key, ticket);
    it->SeekToFirst();
    for (int k = 0; k < 10; k++) {
        ASSERT_TRUE(it->Valid());
        std::string pk = it->GetPK();
        ASSERT_EQ(pk, raw_key);
        ASSERT_EQ(9537 + 9 - k, (int64_t)it->GetKey());
        std::string value1 = it->GetValue().ToString();
        ASSERT_EQ("value", value1);
        it->Next();
    }
    ASSERT_FALSE(it->Valid());
    delete it;
    delete table;

    std::string path = FLAGS_hdd_root_path + "/9_1";
    RemoveData(path);
}

TEST_F(DiskTableTest, CompactFilter) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    DiskTable* table = new DiskTable(
        "t1", 10, 1, mapping, 10, ::openmldb::type::TTLType::kAbsoluteTime,
        ::openmldb::common::StorageMode::kHDD, FLAGS_hdd_root_path);
    ASSERT_TRUE(table->Init());
    uint64_t cur_time = ::baidu::common::timer::get_micros() / 1000;
    for (int idx = 0; idx < 100; idx++) {
        std::string key = "test" + std::to_string(idx);
        uint64_t ts = cur_time;
        for (int k = 0; k < 5; k++) {
            if (k > 2) {
                ASSERT_TRUE(
                    table->Put(key, ts - k - 10 * 60 * 1000, "value9", 6));
            } else {
                ASSERT_TRUE(table->Put(key, ts - k, "value", 5));
            }
        }
    }
    for (int idx = 0; idx < 100; idx++) {
        std::string key = "test" + std::to_string(idx);
        uint64_t ts = cur_time;
        for (int k = 0; k < 5; k++) {
            std::string value;
            if (k > 2) {
                ASSERT_TRUE(table->Get(key, ts - k - 10 * 60 * 1000, value));
                ASSERT_EQ("value9", value);
            } else {
                ASSERT_TRUE(table->Get(key, ts - k, value));
                ASSERT_EQ("value", value);
            }
        }
    }
    table->CompactDB();
    for (int idx = 0; idx < 100; idx++) {
        std::string key = "test" + std::to_string(idx);
        uint64_t ts = cur_time;
        for (int k = 0; k < 5; k++) {
            std::string value;
            if (k > 2) {
                ASSERT_FALSE(table->Get(key, ts - k - 10 * 60 * 1000, value));
            } else {
                ASSERT_TRUE(table->Get(key, ts - k, value));
                ASSERT_EQ("value", value);
            }
        }
    }
    delete table;
    std::string path = FLAGS_hdd_root_path + "/10_1";
    RemoveData(path);
}

TEST_F(DiskTableTest, CompactFilterMulTs) {
    ::openmldb::api::TableMeta table_meta;
    table_meta.set_tid(11);
    table_meta.set_pid(1);
    table_meta.set_storage_mode(::openmldb::common::kHDD);
    ::openmldb::common::ColumnDesc* column_desc = table_meta.add_column_desc();
    column_desc->set_name("card");
    column_desc->set_data_type(::openmldb::type::kString);
    ::openmldb::common::ColumnDesc* column_desc1 = table_meta.add_column_desc();
    column_desc1->set_name("mcc");
    column_desc1->set_data_type(::openmldb::type::kString);
    ::openmldb::common::ColumnDesc* column_desc2 = table_meta.add_column_desc();
    column_desc2->set_name("ts1");
    column_desc2->set_data_type(::openmldb::type::kBigInt);
    // column_desc2->set_is_ts_col(true);
    // column_desc2->set_ttl(3);
    ::openmldb::common::ColumnDesc* column_desc3 = table_meta.add_column_desc();
    column_desc3->set_name("ts2");
    column_desc3->set_data_type(::openmldb::type::kBigInt);
    // column_desc3->set_is_ts_col(true);
    // column_desc3->set_ttl(5); 
    ::openmldb::common::ColumnKey* column_key = table_meta.add_column_key();
    column_key->set_index_name("card");
    column_key->add_col_name("card");
    column_key->set_ts_name("ts1");
    auto ttl = column_key->mutable_ttl();
    ttl->set_ttl_type(::openmldb::type::TTLType::kAbsoluteTime);
    ttl->set_abs_ttl(3);
    
    ::openmldb::common::ColumnKey* column_key2 = table_meta.add_column_key();
    column_key2->set_index_name("card1");
    column_key->add_col_name("card");
    column_key2->set_ts_name("ts2");
    auto ttl3 = column_key2->mutable_ttl();
    ttl3->set_ttl_type(::openmldb::type::TTLType::kAbsoluteTime);
    ttl3->set_abs_ttl(5);

    ::openmldb::common::ColumnKey* column_key1 = table_meta.add_column_key();
    column_key1->set_index_name("mcc");
    column_key->add_col_name("mcc");
    column_key1->set_ts_name("ts2");
    auto ttl2 = column_key1->mutable_ttl();
    ttl2->set_ttl_type(::openmldb::type::TTLType::kAbsoluteTime);
    ttl2->set_abs_ttl(5);

    DiskTable* table = new DiskTable(table_meta, FLAGS_hdd_root_path);
    ASSERT_TRUE(table->Init());
    uint64_t cur_time = ::baidu::common::timer::get_micros() / 1000;
    for (int idx = 0; idx < 100; idx++) {
        Dimensions dims;
        ::openmldb::api::Dimension* dim = dims.Add();
        dim->set_key("card" + std::to_string(idx));
        dim->set_idx(0);
        ::openmldb::api::Dimension* dim1 = dims.Add();
        dim1->set_key("card" + std::to_string(idx));
        dim1->set_idx(1);
        ::openmldb::api::Dimension* dim2 = dims.Add();
        dim2->set_key("mcc" + std::to_string(idx));
        dim2->set_idx(2);
        std::string key = "test" + std::to_string(idx);
        if (idx == 5 || idx == 10) {
            for (int i = 0; i < 10; i++) {
                ASSERT_TRUE(
                    table->Put(cur_time - i * 60 * 1000, "value" + std::to_string(i), dims));
            }

        } else {
            for (int i = 0; i < 10; i++) {
                ASSERT_TRUE(
                    table->Put(cur_time - i, "value" + std::to_string(i), dims));
            }
        }
    }
    Ticket ticket;
    TableIterator* iter = table->NewIterator(0, "card0", ticket);
    iter->SeekToFirst();
    while (iter->Valid()) {
        // printf("key %s ts %lu\n", iter->GetPK().c_str(), iter->GetKey());
        iter->Next();
    }
    delete iter;
    iter = table->NewIterator(2, "mcc0", ticket);
    iter->SeekToFirst();
    while (iter->Valid()) {
        // printf("key %s ts %lu\n", iter->GetPK().c_str(), iter->GetKey());
        iter->Next();
    }
    delete iter;
    for (int idx = 0; idx < 100; idx++) {
        std::string key = "card" + std::to_string(idx);
        std::string key1 = "mcc" + std::to_string(idx);
        uint64_t ts = cur_time;
        if (idx == 5 || idx == 10) {
            for (int i = 0; i < 10; i++) {
                std::string e_value = "value" + std::to_string(i);
                std::string value;
                ASSERT_TRUE(table->Get(0, key, ts - i * 60 * 1000, value));
                ASSERT_EQ(e_value, value);
                ASSERT_TRUE(table->Get(1, key, ts - i * 60 * 1000, value));
                ASSERT_EQ(e_value, value);
                ASSERT_TRUE(table->Get(2, key1, ts - i * 60 * 1000, value));
            }

        } else {
            for (int i = 0; i < 10; i++) {
                std::string e_value = "value" + std::to_string(i);
                std::string value;
                // printf("idx:%d i:%d key:%s ts:%lu\n", idx, i, key.c_str(), ts
                // - i); printf("idx:%d i:%d key:%s ts:%lu\n", idx, i,
                // key1.c_str(), ts - i);
                ASSERT_TRUE(table->Get(0, key, ts - i, value));
                ASSERT_EQ(e_value, value);
                ASSERT_TRUE(table->Get(1, key, ts - i, value));
                ASSERT_EQ(e_value, value);
                ASSERT_TRUE(table->Get(2, key1, ts - i, value));
            }
        }
    }
    table->CompactDB();
    iter = table->NewIterator(0, "card0", ticket);
    iter->SeekToFirst();
    while (iter->Valid()) {
        // printf("key %s ts %lu\n", iter->GetPK().c_str(), iter->GetKey());
        iter->Next();
    }
    delete iter;
    iter = table->NewIterator(2, "mcc0", ticket);
    iter->SeekToFirst();
    while (iter->Valid()) {
        // printf("key %s ts %lu\n", iter->GetPK().c_str(), iter->GetKey());
        iter->Next();
    }
    delete iter;
    for (int idx = 0; idx < 100; idx++) {
        std::string key = "card" + std::to_string(idx);
        std::string key1 = "mcc" + std::to_string(idx);
        uint64_t ts = cur_time;
        if (idx == 5 || idx == 10) {
            for (int i = 0; i < 10; i++) {
                std::string e_value = "value" + std::to_string(i);
                std::string value;
                uint64_t cur_ts = ts - i * 60 * 1000;
                if (i < 3) {
                    ASSERT_TRUE(table->Get(0, key, cur_ts, value));
                    ASSERT_EQ(e_value, value);
                } else {
                    ASSERT_FALSE(table->Get(0, key, cur_ts, value));
                }
                if (i < 5) {
                    ASSERT_TRUE(table->Get(1, key, cur_ts, value));
                    ASSERT_EQ(e_value, value);
                    ASSERT_TRUE(table->Get(2, key1, cur_ts, value));
                } else {
                    // printf("idx:%lu i:%d key:%s ts:%lu\n", idx, i,
                    // key.c_str(), cur_ts);
                    ASSERT_FALSE(table->Get(1, key, cur_ts, value));
                    // printf("idx:%lu i:%d key:%s ts:%lu\n", idx, i,
                    // key1.c_str(), cur_ts);
                    ASSERT_FALSE(table->Get(2, key1, cur_ts, value));
                }
            }
        } else {
            for (int i = 0; i < 10; i++) {
                std::string e_value = "value" + std::to_string(i);
                std::string value;
                ASSERT_TRUE(table->Get(0, key, ts - i, value));
                ASSERT_EQ(e_value, value);
                ASSERT_TRUE(table->Get(1, key, ts - i, value));
                ASSERT_EQ(e_value, value);
                ASSERT_TRUE(table->Get(2, key1, ts - i, value));
            }
        }
    }
    delete table;
    std::string path = FLAGS_hdd_root_path + "/11_1";
    RemoveData(path);
}

TEST_F(DiskTableTest, GcHeadMulTs) {
    ::openmldb::api::TableMeta table_meta;
    table_meta.set_tid(12);
    table_meta.set_pid(1);
    table_meta.set_ttl_type(::openmldb::type::TTLType::kLatestTime);
    table_meta.set_storage_mode(::openmldb::common::kHDD);
    ::openmldb::common::ColumnDesc* column_desc = table_meta.add_column_desc();
    column_desc->set_name("card");
    column_desc->set_data_type(::openmldb::type::kString);
    ::openmldb::common::ColumnDesc* column_desc1 = table_meta.add_column_desc();
    column_desc1->set_name("mcc");
    column_desc1->set_data_type(::openmldb::type::kString);
    ::openmldb::common::ColumnDesc* column_desc2 = table_meta.add_column_desc();
    column_desc2->set_name("ts1");
    column_desc2->set_data_type(::openmldb::type::kBigInt);
    // column_desc2->set_is_ts_col(true);
    // column_desc2->set_ttl(3);
    ::openmldb::common::ColumnDesc* column_desc3 = table_meta.add_column_desc();
    column_desc3->set_name("ts2");
    column_desc3->set_data_type(::openmldb::type::kBigInt);
    // column_desc3->set_is_ts_col(true);
    // column_desc3->set_ttl(5); 
    ::openmldb::common::ColumnKey* column_key = table_meta.add_column_key();
    column_key->set_index_name("card");
    column_key->add_col_name("card");
    column_key->set_ts_name("ts1");
    auto ttl = column_key->mutable_ttl();
    ttl->set_ttl_type(::openmldb::type::TTLType::kLatestTime);
    ttl->set_lat_ttl(3);
    
    ::openmldb::common::ColumnKey* column_key2 = table_meta.add_column_key();
    column_key2->set_index_name("card1");
    column_key->add_col_name("card");
    column_key2->set_ts_name("ts2");
    auto ttl3 = column_key2->mutable_ttl();
    ttl3->set_ttl_type(::openmldb::type::TTLType::kLatestTime);
    ttl3->set_lat_ttl(5);

    ::openmldb::common::ColumnKey* column_key1 = table_meta.add_column_key();
    column_key1->set_index_name("mcc");
    column_key->add_col_name("mcc");
    column_key1->set_ts_name("ts2");
    auto ttl2 = column_key1->mutable_ttl();
    ttl2->set_ttl_type(::openmldb::type::TTLType::kLatestTime);
    ttl2->set_lat_ttl(5);

    DiskTable* table = new DiskTable(table_meta, FLAGS_hdd_root_path);
    ASSERT_TRUE(table->Init());
    uint64_t cur_time = ::baidu::common::timer::get_micros() / 1000;
    for (int idx = 0; idx < 100; idx++) {
        Dimensions dims;
        ::openmldb::api::Dimension* dim = dims.Add();
        dim->set_key("card" + std::to_string(idx));
        dim->set_idx(0);
        ::openmldb::api::Dimension* dim1 = dims.Add();
        dim1->set_key("card" + std::to_string(idx));
        dim1->set_idx(1);
        ::openmldb::api::Dimension* dim2 = dims.Add();
        dim2->set_key("mcc" + std::to_string(idx));
        dim2->set_idx(2);
        std::string key = "test" + std::to_string(idx);
        for (int i = 0; i < 10; i++) {
            if (idx == 50 && i > 2) {
                break;
            }
            ASSERT_TRUE(table->Put(cur_time -  i, "value" + std::to_string(i), dims));
        }
    }
    Ticket ticket;
    TableIterator* iter = table->NewIterator(0, "card0", ticket);
    iter->SeekToFirst();
    while (iter->Valid()) {
        // printf("key %s ts %lu\n", iter->GetPK().c_str(), iter->GetKey());
        iter->Next();
    }
    delete iter;
    iter = table->NewIterator(1, "mcc0", ticket);
    iter->SeekToFirst();
    while (iter->Valid()) {
        // printf("key %s ts %lu\n", iter->GetPK().c_str(), iter->GetKey());
        iter->Next();
    }
    delete iter;
    for (int idx = 0; idx < 100; idx++) {
        std::string key = "card" + std::to_string(idx);
        std::string key1 = "mcc" + std::to_string(idx);
        for (int i = 0; i < 10; i++) {
            std::string e_value = "value" + std::to_string(i);
            std::string value;
            // printf("idx:%d i:%d key:%s ts:%lu\n", idx, i, key.c_str(), ts -
            // i); printf("idx:%d i:%d key:%s ts:%lu\n", idx, i, key1.c_str(),
            // ts
            // - i);
            if (idx == 50 && i > 2) {
                ASSERT_FALSE(table->Get(0, key, cur_time - i, value));
                ASSERT_FALSE(table->Get(1, key, cur_time - i, value));
                ASSERT_FALSE(table->Get(2, key1, cur_time - i, value));
            } else {
                ASSERT_TRUE(table->Get(0, key, cur_time - i, value));
                ASSERT_EQ(e_value, value);
                ASSERT_TRUE(table->Get(1, key, cur_time - i, value));
                ASSERT_EQ(e_value, value);
                ASSERT_TRUE(table->Get(2, key1, cur_time - i, value));
            }
        }
    }
    table->SchedGc();
    iter = table->NewIterator(0, "card0", ticket);
    iter->SeekToFirst();
    while (iter->Valid()) {
        // printf("key %s ts %lu\n", iter->GetPK().c_str(), iter->GetKey());
        iter->Next();
    }
    delete iter;
    iter = table->NewIterator(1, "mcc0", ticket);
    iter->SeekToFirst();
    while (iter->Valid()) {
        // printf("key %s ts %lu\n", iter->GetPK().c_str(), iter->GetKey());
        iter->Next();
    }
    delete iter;
    for (int idx = 0; idx < 100; idx++) {
        std::string key = "card" + std::to_string(idx);
        std::string key1 = "mcc" + std::to_string(idx);
        for (int i = 0; i < 10; i++) {
            std::string e_value = "value" + std::to_string(i);
            std::string value;
            // printf("idx:%d, i:%d, key:%s, ts:%lu\n", idx, i , key.c_str(),
            // ts-i);
            if (idx == 50 && i > 2) {
                ASSERT_FALSE(table->Get(0, key, cur_time - i, value));
                ASSERT_FALSE(table->Get(1, key, cur_time - i, value));
                ASSERT_FALSE(table->Get(2, key1, cur_time - i, value));
            } else if (i < 3) {
                ASSERT_TRUE(table->Get(0, key, cur_time - i, value));
                ASSERT_EQ(e_value, value);
                ASSERT_TRUE(table->Get(1, key, cur_time - i, value));
                ASSERT_EQ(e_value, value);
                ASSERT_TRUE(table->Get(2, key1, cur_time - i, value));
            } else if (i < 5) {
                ASSERT_FALSE(table->Get(0, key, cur_time - i, value));
                ASSERT_TRUE(table->Get(1, key, cur_time - i, value));
                ASSERT_EQ(e_value, value);
                ASSERT_TRUE(table->Get(2, key1, cur_time - i, value));
            } else {
                ASSERT_FALSE(table->Get(0, key, cur_time - i, value));
                ASSERT_FALSE(table->Get(1, key, cur_time - i, value));
                ASSERT_FALSE(table->Get(2, key1, cur_time - i, value));
            }
        }
    }
    delete table;
    std::string path = FLAGS_hdd_root_path + "/12_1";
    RemoveData(path);
}

TEST_F(DiskTableTest, GcHead) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    DiskTable* table = new DiskTable(
        "t1", 13, 1, mapping, 3, ::openmldb::type::TTLType::kLatestTime,
        ::openmldb::common::StorageMode::kHDD, FLAGS_hdd_root_path);
    ASSERT_TRUE(table->Init());
    for (int idx = 0; idx < 100; idx++) {
        std::string key = "test" + std::to_string(idx);
        uint64_t ts = 9537;
        for (int k = 0; k < 5; k++) {
            ASSERT_TRUE(table->Put(key, ts + k, "value", 5));
            if (idx == 10 && k == 2) {
                ASSERT_TRUE(table->Put(key, ts + k, "value9", 6));
                ASSERT_TRUE(table->Put(key, ts + k, "value8", 6));
            }
        }
    }
    for (int idx = 0; idx < 100; idx++) {
        std::string key = "test" + std::to_string(idx);
        uint64_t ts = 9537;
        for (int k = 0; k < 5; k++) {
            std::string value;
            ASSERT_TRUE(table->Get(key, ts + k, value));
            if (idx == 10 && k == 2) {
                ASSERT_EQ("value8", value);
            } else {
                ASSERT_EQ("value", value);
            }
        }
    }
    table->GcHead();
    for (int idx = 0; idx < 100; idx++) {
        std::string key = "test" + std::to_string(idx);
        uint64_t ts = 9537;
        for (int k = 0; k < 5; k++) {
            std::string value;
            if (k < 2) {
                ASSERT_FALSE(table->Get(key, ts + k, value));
            } else {
                ASSERT_TRUE(table->Get(key, ts + k, value));
                if (idx == 10 && k == 2) {
                    ASSERT_EQ("value8", value);
                } else {
                    ASSERT_EQ("value", value);
                }
            }
        }
    }
    delete table;
    std::string path = FLAGS_hdd_root_path + "/13_1";
    RemoveData(path);
}

TEST_F(DiskTableTest, CheckPoint) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    DiskTable* table = new DiskTable(
        "t1", 15, 1, mapping, 0, ::openmldb::type::TTLType::kAbsoluteTime,
        ::openmldb::common::StorageMode::kHDD, FLAGS_hdd_root_path);
    ASSERT_TRUE(table->Init());
    for (int idx = 0; idx < 100; idx++) {
        std::string key = "test" + std::to_string(idx);
        uint64_t ts = 9537;
        for (int k = 0; k < 10; k++) {
            ASSERT_TRUE(table->Put(key, ts + k, "value", 5));
        }
    }
    std::string raw_key = "test35";
    Ticket ticket;
    TableIterator* it = table->NewIterator(raw_key, ticket);
    it->SeekToFirst();
    for (int k = 0; k < 10; k++) {
        ASSERT_TRUE(it->Valid());
        std::string pk = it->GetPK();
        ASSERT_EQ(pk, raw_key);
        ASSERT_EQ(9537 + 9 - k, (int64_t)it->GetKey());
        std::string value1 = it->GetValue().ToString();
        ASSERT_EQ("value", value1);
        it->Next();
    }
    ASSERT_FALSE(it->Valid());
    delete it;

    std::string snapshot_path = FLAGS_hdd_root_path + "/15_1/snapshot";
    ASSERT_EQ(table->CreateCheckPoint(snapshot_path), 0);
    delete table;

    std::string data_path = FLAGS_hdd_root_path + "/15_1/data";
    ::openmldb::base::RemoveDir(data_path);

    ::openmldb::base::Rename(snapshot_path, data_path);

    table = new DiskTable(
        "t1", 15, 1, mapping, 0, ::openmldb::type::TTLType::kAbsoluteTime,
        ::openmldb::common::StorageMode::kHDD, FLAGS_hdd_root_path);
    ASSERT_TRUE(table->LoadTable());
    raw_key = "test35";
    it = table->NewIterator(raw_key, ticket);
    it->SeekToFirst();
    for (int k = 0; k < 10; k++) {
        ASSERT_TRUE(it->Valid());
        std::string pk = it->GetPK();
        ASSERT_EQ(pk, raw_key);
        ASSERT_EQ(9537 + 9 - k, (int64_t)(it->GetKey()));
        std::string value1 = it->GetValue().ToString();
        ASSERT_EQ("value", value1);
        it->Next();
    }
    ASSERT_FALSE(it->Valid());
    delete it;

    it = table->NewTraverseIterator(0);
    it->SeekToFirst();
    int count = 0;
    while (it->Valid()) {
        std::string pk = it->GetPK();
        count++;
        it->Next();
    }
    ASSERT_FALSE(it->Valid());
    ASSERT_EQ(1000, count);
    delete it;
    delete table;

    std::string path = FLAGS_hdd_root_path + "/15_1";
    RemoveData(path);
}

// add some tests from table_test
TEST_F(DiskTableTest, PutNew) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    DiskTable* table = new DiskTable("tx_log", 16, 1, mapping, 10, ::openmldb::type::kAbsoluteTime,
        ::openmldb::common::StorageMode::kHDD, FLAGS_hdd_root_path);
    table->Init();
    table->Put("test", 9537, "test", 4);
    ASSERT_EQ(1, (int64_t)table->GetRecordCnt());
    Ticket ticket;
    TableIterator* it = table->NewIterator("test", ticket);
    it->SeekToFirst();
    ASSERT_TRUE(it->Valid());
    ASSERT_EQ(9537, (int64_t)it->GetKey());
    std::string value_str(it->GetValue().data(), it->GetValue().size());
    ASSERT_EQ("test", value_str);
    it->Next();
    ASSERT_FALSE(it->Valid());
    delete it;
    delete table;

    std::string path = FLAGS_hdd_root_path + "/16_1";
    RemoveData(path);
}

TEST_F(DiskTableTest, MultiDimissionDeleteNew) {
    ::openmldb::api::TableMeta table_meta;
    table_meta.set_tid(17);
    table_meta.set_pid(1);
    table_meta.set_storage_mode(::openmldb::common::kHDD);
    table_meta.set_mode(::openmldb::api::TableMode::kTableLeader);
    table_meta.set_format_version(1);

    ::openmldb::common::ColumnDesc* desc = table_meta.add_column_desc();
    desc->set_name("card");
    desc->set_data_type(::openmldb::type::kString);
    desc = table_meta.add_column_desc();
    desc->set_name("mcc");
    desc->set_data_type(::openmldb::type::kString);
    desc = table_meta.add_column_desc();
    desc->set_name("price");
    desc->set_data_type(::openmldb::type::kBigInt);
    auto column_key = table_meta.add_column_key();
    column_key->set_index_name("card");
    auto ttl = column_key->mutable_ttl();
    ttl->set_abs_ttl(5);
    column_key = table_meta.add_column_key();
    column_key->set_index_name("mcc");
    ttl = column_key->mutable_ttl();
    ttl->set_abs_ttl(5);
    DiskTable* table = new DiskTable(table_meta, FLAGS_hdd_root_path);
    table->Init();
    table->DeleteIndex("mcc");
    table->SchedGc();
    table->SchedGc();
    table->SchedGc();

    std::string path = FLAGS_hdd_root_path + "/17_1";
    RemoveData(path);
}

TEST_F(DiskTableTest, MultiDimissionPut0New) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    mapping.insert(std::make_pair("idx1", 1));
    mapping.insert(std::make_pair("idx2", 2));
    DiskTable* table = new DiskTable("tx_log", 18, 1,  mapping, 10, ::openmldb::type::kAbsoluteTime, 
        ::openmldb::common::StorageMode::kHDD, FLAGS_hdd_root_path);
    table->Init();
    ASSERT_EQ(3, (int64_t)table->GetIdxCnt());
    ASSERT_EQ(0, (int64_t)table->GetRecordIdxCnt());
    ASSERT_EQ(0, (int64_t)table->GetRecordCnt());
    Dimensions dimensions;
    ::openmldb::api::Dimension* d0 = dimensions.Add();
    d0->set_key("d0");
    d0->set_idx(0);

    ::openmldb::api::Dimension* d1 = dimensions.Add();
    d1->set_key("d1");
    d1->set_idx(1);

    ::openmldb::api::Dimension* d2 = dimensions.Add();
    d2->set_key("d2");
    d2->set_idx(2);
    auto meta = ::openmldb::test::GetTableMeta({"idx0", "idx1", "idx2"});
    ::openmldb::codec::SDKCodec sdk_codec(meta);
    std::string result;
    sdk_codec.EncodeRow({"d0", "d1", "d2"}, &result);
    bool ok = table->Put(1, result, dimensions);
    ASSERT_TRUE(ok);
    ASSERT_EQ(3, (int64_t)table->GetRecordIdxCnt());
    ASSERT_EQ(1, (int64_t)table->GetRecordCnt());

    delete table;
    std::string path = FLAGS_hdd_root_path + "/18_1";
    RemoveData(path);
}

TEST_F(DiskTableTest, ReleaseNew) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    DiskTable* table = new DiskTable("tx_log", 19, 1,  mapping, 10, ::openmldb::type::kAbsoluteTime, 
        ::openmldb::common::StorageMode::kHDD, FLAGS_hdd_root_path);
    table->Init();
    table->Put("test", 9537, "test", 4);
    table->Put("test2", 9537, "test", 4);
    int64_t cnt = table->Release();
    ASSERT_EQ(cnt, 2);
    delete table;
    std::string path = FLAGS_hdd_root_path + "/19_1";
    RemoveData(path);
}

TEST_F(DiskTableTest, IsExpiredNew) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    // table ttl is 1
    DiskTable* table = new DiskTable("tx_log", 20, 1,  mapping, 1, ::openmldb::type::kAbsoluteTime, 
        ::openmldb::common::StorageMode::kHDD, FLAGS_hdd_root_path);
    table->Init();
    uint64_t now_time = ::baidu::common::timer::get_micros() / 1000;
    ::openmldb::api::LogEntry entry;
    uint64_t ts_time = now_time;
    entry.set_ts(ts_time);
    ::openmldb::storage::TTLSt ttl(1 * 60 * 1000, 0, ::openmldb::storage::kAbsoluteTime);
    ASSERT_FALSE(entry.ts() < table->GetExpireTime(ttl));

    // ttl_offset_ is 60 * 1000
    ts_time = now_time - 4 * 60 * 1000;
    entry.set_ts(ts_time);
    ASSERT_TRUE(entry.ts() < table->GetExpireTime(ttl));
    delete table;

    std::string path = FLAGS_hdd_root_path + "/20_1";
    RemoveData(path);
}

TEST_F(DiskTableTest, IteratorNew) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    DiskTable* table = new DiskTable("tx_log", 21, 1,  mapping, 10, ::openmldb::type::kAbsoluteTime, 
        ::openmldb::common::StorageMode::kHDD, FLAGS_hdd_root_path);
    table->Init();

    table->Put("pk", 9527, "test", 4);
    table->Put("pk1", 9527, "test", 4);
    table->Put("pk", 9528, "test0", 5);
    Ticket ticket;
    TableIterator* it = table->NewIterator("pk", ticket);

    it->Seek(9528);
    ASSERT_TRUE(it->Valid());
    std::string value_str(it->GetValue().data(), it->GetValue().size());
    ASSERT_EQ("test0", value_str);
    it->Next();
    std::string value2_str(it->GetValue().data(), it->GetValue().size());
    ASSERT_EQ("test", value2_str);
    it->Next();
    ASSERT_FALSE(it->Valid());
    delete it;
    delete table;

    std::string path = FLAGS_hdd_root_path + "/21_1";
    RemoveData(path);
}

TEST_F(DiskTableTest, Iterator_GetSizeNew) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    DiskTable* table = new DiskTable("tx_log", 22, 1,  mapping, 10, ::openmldb::type::kAbsoluteTime, 
        ::openmldb::common::StorageMode::kHDD, FLAGS_hdd_root_path);
    table->Init();

    table->Put("pk", 9527, "test", 4);
    table->Put("pk", 9527, "test", 4);
    table->Put("pk", 9528, "test0", 5);
    Ticket ticket;
    TableIterator* it = table->NewIterator("pk", ticket);
    int size = 0;
    it->SeekToFirst();
    while (it->Valid()) {
        it->Next();
        size++;
    }
    // TODO(litongxin) why size is 3 ?
    ASSERT_EQ(3, size);
    it->Seek(9528);
    ASSERT_TRUE(it->Valid());
    std::string value_str(it->GetValue().data(), it->GetValue().size());
    ASSERT_EQ("test0", value_str);
    it->Next();
    std::string value2_str(it->GetValue().data(), it->GetValue().size());
    ASSERT_EQ("test", value2_str);
    it->Next();
    ASSERT_TRUE(it->Valid());
    delete it;
    delete table;

    std::string path = FLAGS_hdd_root_path + "/22_1";
    RemoveData(path);
}

TEST_F(DiskTableTest, SchedGcHeadNew) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    DiskTable* table = new DiskTable("tx_log", 23, 1,  mapping, 1, ::openmldb::type::kLatestTime, 
        ::openmldb::common::StorageMode::kHDD, FLAGS_hdd_root_path);
    table->Init();
    std::string value = ::openmldb::test::EncodeKV("test", "test1");
    table->Put("test", 2, value.data(), value.size());
    uint64_t bytes = table->GetRecordByteSize();
    uint64_t record_idx_bytes = table->GetRecordIdxByteSize();
    value = ::openmldb::test::EncodeKV("test", "test2");
    table->Put("test", 1, value.data(), value.size());
    ASSERT_EQ(2, (int64_t)table->GetRecordCnt());
    ASSERT_EQ(2, (int64_t)table->GetRecordIdxCnt());
    ASSERT_EQ(1, (int64_t)table->GetRecordPkCnt());
    table->SchedGc();
    {
        ::openmldb::api::LogEntry entry;
        entry.set_log_index(0);
        entry.set_pk("test");
        entry.set_ts(1);
        entry.set_value(::openmldb::test::EncodeKV("test", "test2"));
        ASSERT_TRUE(table->IsExpire(entry));
    }
    {
        ::openmldb::api::LogEntry entry;
        entry.set_log_index(0);
        entry.set_pk("test");
        entry.set_ts(2);
        entry.set_value(::openmldb::test::EncodeKV("test", "test1"));
        ASSERT_FALSE(table->IsExpire(entry));
    }
    ASSERT_EQ(1, (int64_t)table->GetRecordCnt());
    ASSERT_EQ(1, (int64_t)table->GetRecordIdxCnt());
    ASSERT_EQ(bytes, table->GetRecordByteSize());
    ASSERT_EQ(record_idx_bytes, table->GetRecordIdxByteSize());

    delete table;

    std::string path = FLAGS_hdd_root_path + "/23_1";
    RemoveData(path);
}

TEST_F(DiskTableTest, SchedGcHead1New) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    uint64_t keep_cnt = 500;
    DiskTable* table = new DiskTable("tx_log", 24, 1,  mapping, keep_cnt, ::openmldb::type::kLatestTime, 
        ::openmldb::common::StorageMode::kHDD, FLAGS_hdd_root_path);
    table->Init();
    uint64_t ts = 0;
    for (int i = 0; i < 10; i++) {
        int count = 5000;
        while (count) {
            ts++;
            table->Put("test", ts, "test1", 5);
            count--;
        }
        table->SchedGc();
        Ticket ticket;
        TableIterator* it = table->NewIterator("test", ticket);

        it->Seek(ts + 1);
        ASSERT_TRUE(it->Valid());
        it->Seek(ts);
        ASSERT_TRUE(it->Valid());
        it->Seek(ts - keep_cnt / 2);
        ASSERT_TRUE(it->Valid());
        it->Seek(ts - keep_cnt / 4);
        ASSERT_TRUE(it->Valid());
        it->Seek(ts - keep_cnt + 1);
        ASSERT_TRUE(it->Valid());
        it->Seek(ts - keep_cnt);
        ASSERT_FALSE(it->Valid());
        it->Seek(ts - keep_cnt - 1);
        ASSERT_FALSE(it->Valid());
        delete it;
    }
    table->SchedGc();

    delete table;
    std::string path = FLAGS_hdd_root_path + "/24_1";
    RemoveData(path);
}

TEST_F(DiskTableTest, SchedGcNew) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    DiskTable* table = new DiskTable("tx_log", 25, 1,  mapping, 1, ::openmldb::type::kLatestTime, 
        ::openmldb::common::StorageMode::kHDD, FLAGS_hdd_root_path);
    table->Init();

    uint64_t now = ::baidu::common::timer::get_micros() / 1000;
    table->Put("test", now, "tes2", 4);
    uint64_t bytes = table->GetRecordByteSize();
    uint64_t record_idx_bytes = table->GetRecordIdxByteSize();
    table->Put("test", 9527, "test", 4);
    ASSERT_EQ(2, (int64_t)table->GetRecordCnt());
    ASSERT_EQ(2, (int64_t)table->GetRecordIdxCnt());
    ASSERT_EQ(1, (int64_t)table->GetRecordPkCnt());

    table->SchedGc();
    ASSERT_EQ(1, (int64_t)table->GetRecordCnt());
    ASSERT_EQ(1, (int64_t)table->GetRecordIdxCnt());
    ASSERT_EQ(bytes, table->GetRecordByteSize());
    ASSERT_EQ(record_idx_bytes, table->GetRecordIdxByteSize());

    Ticket ticket;
    TableIterator* it = table->NewIterator("test", ticket);
    it->Seek(now);
    ASSERT_TRUE(it->Valid());
    std::string value_str(it->GetValue().data(), it->GetValue().size());
    ASSERT_EQ("tes2", value_str);
    it->Next();
    ASSERT_FALSE(it->Valid());

    delete table;
    std::string path = FLAGS_hdd_root_path + "/25_1";
    RemoveData(path);
}

TEST_F(DiskTableTest, TableDataCntNew) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    DiskTable* table = new DiskTable("tx_log", 26, 1,  mapping, 1, ::openmldb::type::kAbsoluteTime, 
        ::openmldb::common::StorageMode::kHDD, FLAGS_hdd_root_path);
    table->Init();
    ASSERT_EQ((int64_t)table->GetRecordCnt(), 0);
    uint64_t now = ::baidu::common::timer::get_micros() / 1000;
    table->Put("test", 9527, "test", 4);
    table->Put("test", now, "tes2", 4);
    ASSERT_EQ((int64_t)table->GetRecordCnt(), 2);
    ASSERT_EQ((int64_t)table->GetRecordIdxCnt(), 2);
    table->SchedGc();
    {
        ::openmldb::api::LogEntry entry;
        entry.set_log_index(0);
        entry.set_pk("test");
        entry.set_ts(now - 1 * (60 * 1000) - 1);
        entry.set_value(::openmldb::test::EncodeKV("test", "test"));
        ASSERT_TRUE(table->IsExpire(entry));
    }
    {
        ::openmldb::api::LogEntry entry;
        entry.set_log_index(0);
        entry.set_pk("test");
        entry.set_ts(now);
        entry.set_value(::openmldb::test::EncodeKV("test", "tes2"));
        ASSERT_FALSE(table->IsExpire(entry));
    }
    ASSERT_EQ((int64_t)table->GetRecordCnt(), 1);
    ASSERT_EQ((int64_t)table->GetRecordIdxCnt(), 1);

    delete table;
    std::string path = FLAGS_hdd_root_path + "/26_1";
    RemoveData(path);
}

TEST_F(DiskTableTest, TableUnrefNew) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    DiskTable* table = new DiskTable("tx_log", 27, 1,  mapping, 1, ::openmldb::type::kAbsoluteTime, 
        ::openmldb::common::StorageMode::kHDD, FLAGS_hdd_root_path);
    table->Init();
    table->Put("test", 9527, "test", 4);
    delete table;

    std::string path = FLAGS_hdd_root_path + "/27_1";
    RemoveData(path);
}

TEST_F(DiskTableTest, TableIteratorNew) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    DiskTable* table = new DiskTable("tx_log", 28, 1,  mapping, 0, ::openmldb::type::kAbsoluteTime, 
        ::openmldb::common::StorageMode::kHDD, FLAGS_hdd_root_path);
    table->Init();

    table->Put("pk", 9527, "test1", 5);
    table->Put("pk1", 9527, "test2", 5);
    table->Put("pk", 9528, "test3", 5);
    table->Put("pk1", 100, "test4", 5);
    table->Put("test", 20, "test5", 5);
    // Ticket ticket;
    TableIterator* it = table->NewTraverseIterator(0);
    // it->Test();
    it->SeekToFirst();
    ASSERT_STREQ("pk", it->GetPK().c_str());
    ASSERT_EQ(9528, (int64_t)it->GetKey());
    it->Next();
    ASSERT_STREQ("pk", it->GetPK().c_str());
    ASSERT_EQ(9527, (int64_t)it->GetKey());
    it->Next();
    ASSERT_STREQ("test", it->GetPK().c_str());
    ASSERT_EQ(20, (int64_t)it->GetKey());
    it->Next();
    ASSERT_STREQ("pk1", it->GetPK().c_str());
    ASSERT_EQ(9527, (int64_t)it->GetKey());
    it->Next();
    ASSERT_STREQ("pk1", it->GetPK().c_str());
    ASSERT_EQ(100, (int64_t)it->GetKey());
    it->Next();
    ASSERT_FALSE(it->Valid());

    it->Seek("none", 11111);
    ASSERT_TRUE(it->Valid());

    it->Seek("test", 30);
    ASSERT_TRUE(it->Valid());
    ASSERT_STREQ("test", it->GetPK().c_str());
    ASSERT_EQ(20, (int64_t)it->GetKey());

    it->Seek("test", 20);
    ASSERT_TRUE(it->Valid());
    ASSERT_STREQ("pk1", it->GetPK().c_str());
    ASSERT_EQ(9527, (int64_t)it->GetKey());
    delete it;
    delete table;

    DiskTable* table1 = new DiskTable("tx_log", 29, 1,  mapping, 2, ::openmldb::type::kLatestTime, 
        ::openmldb::common::StorageMode::kHDD, FLAGS_hdd_root_path);
    table1->Init();

    table1->Put("pk", 9527, "test1", 5);
    table1->Put("pk1", 9527, "test2", 5);
    table1->Put("pk", 9528, "test3", 5);
    table1->Put("pk1", 100, "test4", 5);
    table1->Put("test", 20, "test5", 5);
    table1->Put("pk", 200, "test6", 5);
    // Ticket ticket;
    TableIterator* it1 = table1->NewTraverseIterator(0);
    it1->Seek("pk", 9528);
    ASSERT_TRUE(it1->Valid());
    ASSERT_STREQ("pk", it1->GetPK().c_str());
    ASSERT_EQ(9527, (int64_t)it1->GetKey());
    it1->Next();
    ASSERT_TRUE(it1->Valid());
    ASSERT_STREQ("test", it1->GetPK().c_str());
    ASSERT_EQ(20, (int64_t)it1->GetKey());
    it1->Next();
    ASSERT_STREQ("pk1", it1->GetPK().c_str());
    ASSERT_EQ(9527, (int64_t)it1->GetKey());
    delete table1;

    std::string path = FLAGS_hdd_root_path + "/28_1";
    RemoveData(path);
    path = FLAGS_hdd_root_path + "/29_1";
    RemoveData(path);
}

TEST_F(DiskTableTest, TableIteratorNoPkNew) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    DiskTable* table = new DiskTable("tx_log", 30, 1,  mapping, 0, ::openmldb::type::kAbsoluteTime, 
        ::openmldb::common::StorageMode::kHDD, FLAGS_hdd_root_path);
    table->Init();

    table->Put("pk10", 9527, "test10", 5);
    table->Put("pk8", 9526, "test8", 5);
    table->Put("pk6", 9525, "test6", 5);
    table->Put("pk4", 9524, "test4", 5);
    table->Put("pk2", 9523, "test2", 5);
    table->Put("pk0", 9522, "test0", 5);
    // Ticket ticket;
    TableIterator* it = table->NewTraverseIterator(0);
    it->SeekToFirst();
    ASSERT_STREQ("pk10", it->GetPK().c_str());
    ASSERT_EQ(9527, (int64_t)it->GetKey());
    it->Next();
    ASSERT_STREQ("pk4", it->GetPK().c_str());
    ASSERT_EQ(9524, (int64_t)it->GetKey());
    it->Next();
    ASSERT_STREQ("pk8", it->GetPK().c_str());
    ASSERT_EQ(9526, (int64_t)it->GetKey());
    it->Next();
    ASSERT_STREQ("pk0", it->GetPK().c_str());
    ASSERT_EQ(9522, (int64_t)it->GetKey());
    delete it;
    it = table->NewTraverseIterator(0);
    it->Seek("pk4", 9526);
    ASSERT_STREQ("pk4", it->GetPK().c_str());
    ASSERT_EQ(9524, (int64_t)it->GetKey());
    delete it;

    ASSERT_TRUE(table->Delete("pk4", 0));
    it = table->NewTraverseIterator(0);
    it->Seek("pk4", 9526);
    ASSERT_TRUE(it->Valid());
    ASSERT_STREQ("pk8", it->GetPK().c_str());
    ASSERT_EQ(9526, (int64_t)it->GetKey());
    it->Next();
    ASSERT_STREQ("pk0", it->GetPK().c_str());
    ASSERT_EQ(9522, (int64_t)it->GetKey());
    delete it;

    delete table;
    std::string path = FLAGS_hdd_root_path + "/30_1";
    RemoveData(path);

}

TEST_F(DiskTableTest, TableIteratorCountNew) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    DiskTable* table = new DiskTable("tx_log", 31, 1,  mapping, 0, ::openmldb::type::kAbsoluteTime, 
        ::openmldb::common::StorageMode::kHDD, FLAGS_hdd_root_path);
    table->Init();
    for (int i = 0; i < 100000; i = i + 2) {
        std::string key = "pk" + std::to_string(i);
        std::string value = "test" + std::to_string(i);
        table->Put(key, 9527, value.c_str(), value.size());
        table->Put(key, 9528, value.c_str(), value.size());
    }
    TableIterator* it = table->NewTraverseIterator(0);
    it->SeekToFirst();
    int count = 0;
    while (it->Valid()) {
        count++;
        it->Next();
    }
    ASSERT_EQ(100000, count);
    delete it;

    it = table->NewTraverseIterator(0);
    it->Seek("pk500", 9528);
    ASSERT_STREQ("pk500", it->GetPK().c_str());
    ASSERT_EQ(9527, (int64_t)it->GetKey());
    count = 0;
    while (it->Valid()) {
        count++;
        it->Next();
    }
    ASSERT_EQ(44471, count);
    delete it;

    for (int i = 0; i < 200000; i++) {
        TableIterator* cur_it = table->NewTraverseIterator(0);
        std::string key = "pk" + std::to_string(i);
        cur_it->Seek(key, 9528);
        ASSERT_TRUE(cur_it->Valid());
        delete cur_it;
    }

    delete table;
    std::string path = FLAGS_hdd_root_path + "/31_1";
    RemoveData(path);
}

TEST_F(DiskTableTest, TableIteratorTSNew) {

    ::openmldb::api::TableMeta table_meta;
    table_meta.set_tid(32);
    table_meta.set_pid(1);
    table_meta.set_storage_mode(::openmldb::common::kHDD);
    table_meta.set_mode(::openmldb::api::TableMode::kTableLeader);
    table_meta.set_format_version(1);
    ::openmldb::common::ColumnDesc* column_desc = table_meta.add_column_desc();
    column_desc->set_name("card");
    column_desc->set_data_type(::openmldb::type::kString);
    ::openmldb::common::ColumnDesc* column_desc1 = table_meta.add_column_desc();
    column_desc1->set_name("mcc");
    column_desc1->set_data_type(::openmldb::type::kString);
    ::openmldb::common::ColumnDesc* column_desc4 = table_meta.add_column_desc();
    column_desc4->set_name("price");
    column_desc4->set_data_type(::openmldb::type::kBigInt);
    ::openmldb::common::ColumnDesc* column_desc2 = table_meta.add_column_desc();
    column_desc2->set_name("ts1");
    column_desc2->set_data_type(::openmldb::type::kBigInt);
    // column_desc2->set_is_ts_col(true);
    // column_desc2->set_ttl(3);
    ::openmldb::common::ColumnDesc* column_desc3 = table_meta.add_column_desc();
    column_desc3->set_name("ts2");
    column_desc3->set_data_type(::openmldb::type::kBigInt);
    // column_desc3->set_is_ts_col(true);
    // column_desc3->set_ttl(5); 
    ::openmldb::common::ColumnKey* column_key = table_meta.add_column_key();
    column_key->set_index_name("card");
    column_key->add_col_name("card");
    column_key->set_ts_name("ts1");
    auto ttl = column_key->mutable_ttl();
    ttl->set_ttl_type(::openmldb::type::TTLType::kAbsoluteTime);
    ttl->set_abs_ttl(0);
    
    ::openmldb::common::ColumnKey* column_key2 = table_meta.add_column_key();
    column_key2->set_index_name("card1");
    column_key->add_col_name("card");
    column_key2->set_ts_name("ts2");
    auto ttl3 = column_key2->mutable_ttl();
    ttl3->set_ttl_type(::openmldb::type::TTLType::kAbsoluteTime);
    ttl3->set_abs_ttl(0);

    ::openmldb::common::ColumnKey* column_key1 = table_meta.add_column_key();
    column_key1->set_index_name("mcc");
    column_key->add_col_name("mcc");
    column_key1->set_ts_name("ts1");
    auto ttl2 = column_key1->mutable_ttl();
    ttl2->set_ttl_type(::openmldb::type::TTLType::kAbsoluteTime);
    ttl2->set_abs_ttl(0);

    DiskTable* table = new DiskTable(table_meta, FLAGS_hdd_root_path);
    
    table->Init();
    codec::SDKCodec codec(table_meta);

    for (int i = 0; i < 1000; i++) {
        std::vector<std::string> row = {"card" + std::to_string(i % 100), "mcc" + std::to_string(i),
            "13", std::to_string(1000 + i), std::to_string(1000 + i)};
        ::openmldb::api::PutRequest request;
        ::openmldb::api::Dimension* dim = request.add_dimensions();
        dim->set_idx(0);
        dim->set_key(row[0]);
        dim = request.add_dimensions();
        dim->set_idx(1);
        dim->set_key(row[0]);
        dim = request.add_dimensions();
        dim->set_idx(2);
        dim->set_key(row[1]);
        std::string value;
        ASSERT_EQ(0, codec.EncodeRow(row, &value));
        table->Put(0, value, request.dimensions());
    }
    TableIterator* it = table->NewTraverseIterator(0);
    it->SeekToFirst();
    int count = 0;
    while (it->Valid()) {
        count++;
        it->Next();
    }
    ASSERT_EQ(1000, count);
    delete it;

    it = table->NewTraverseIterator(1);
    it->SeekToFirst();
    count = 0;
    while (it->Valid()) {
        count++;
        it->Next();
    }
    ASSERT_EQ(1000, count);
    delete it;

    Ticket ticket;
    TableIterator* iter = table->NewIterator(0, "card5", ticket);
    iter->SeekToFirst();
    count = 0;
    while (iter->Valid()) {
        count++;
        iter->Next();
    }
    ASSERT_EQ(10, count);
    delete iter;
    iter = table->NewIterator(1, "card5", ticket);
    iter->SeekToFirst();
    count = 0;
    while (iter->Valid()) {
        count++;
        iter->Next();
    }
    ASSERT_EQ(10, count);
    delete iter;
    iter = table->NewIterator(2, "mcc10", ticket);
    iter->SeekToFirst();
    count = 0;
    while (iter->Valid()) {
        count++;
        iter->Next();
    }
    ASSERT_EQ(1, count);
    delete iter;
    iter = table->NewIterator(3, "mcc10", ticket);
    ASSERT_EQ(NULL, iter);
    delete iter;

    delete table;
    std::string path = FLAGS_hdd_root_path + "/32_1";
    RemoveData(path);
}

TEST_F(DiskTableTest, TraverseIteratorCountNew) {
    
    uint32_t old_max_traverse = FLAGS_max_traverse_cnt;
    FLAGS_max_traverse_cnt = 50;

    ::openmldb::api::TableMeta table_meta;
    table_meta.set_tid(33);
    table_meta.set_pid(1);
    table_meta.set_storage_mode(::openmldb::common::kHDD);
    table_meta.set_mode(::openmldb::api::TableMode::kTableLeader);
    table_meta.set_format_version(1);
    ::openmldb::common::ColumnDesc* column_desc = table_meta.add_column_desc();
    column_desc->set_name("card");
    column_desc->set_data_type(::openmldb::type::kString);
    ::openmldb::common::ColumnDesc* column_desc1 = table_meta.add_column_desc();
    column_desc1->set_name("mcc");
    column_desc1->set_data_type(::openmldb::type::kString);
    ::openmldb::common::ColumnDesc* column_desc4 = table_meta.add_column_desc();
    column_desc4->set_name("price");
    column_desc4->set_data_type(::openmldb::type::kBigInt);
    ::openmldb::common::ColumnDesc* column_desc2 = table_meta.add_column_desc();
    column_desc2->set_name("ts1");
    column_desc2->set_data_type(::openmldb::type::kBigInt);
    // column_desc2->set_is_ts_col(true);
    // column_desc2->set_ttl(3);
    ::openmldb::common::ColumnDesc* column_desc3 = table_meta.add_column_desc();
    column_desc3->set_name("ts2");
    column_desc3->set_data_type(::openmldb::type::kBigInt);
    // column_desc3->set_is_ts_col(true);
    // column_desc3->set_ttl(5); 
    ::openmldb::common::ColumnKey* column_key = table_meta.add_column_key();
    column_key->set_index_name("card");
    column_key->add_col_name("card");
    column_key->set_ts_name("ts1");
    auto ttl = column_key->mutable_ttl();
    ttl->set_ttl_type(::openmldb::type::TTLType::kAbsoluteTime);
    ttl->set_abs_ttl(0);
    
    ::openmldb::common::ColumnKey* column_key2 = table_meta.add_column_key();
    column_key2->set_index_name("card1");
    column_key->add_col_name("card");
    column_key2->set_ts_name("ts2");
    auto ttl3 = column_key2->mutable_ttl();
    ttl3->set_ttl_type(::openmldb::type::TTLType::kAbsoluteTime);
    ttl3->set_abs_ttl(0);

    ::openmldb::common::ColumnKey* column_key1 = table_meta.add_column_key();
    column_key1->set_index_name("mcc");
    column_key->add_col_name("mcc");
    column_key1->set_ts_name("ts1");
    auto ttl2 = column_key1->mutable_ttl();
    ttl2->set_ttl_type(::openmldb::type::TTLType::kAbsoluteTime);
    ttl2->set_abs_ttl(0);

    DiskTable* table = new DiskTable(table_meta, FLAGS_hdd_root_path);
    
    table->Init();

    codec::SDKCodec codec(table_meta);

    for (int i = 0; i < 1000; i++) {
        std::vector<std::string> row = {"card" + std::to_string(i % 100), "mcc" + std::to_string(i),
            "13", std::to_string(1000 + i), std::to_string(10000 + i)};
        ::openmldb::api::PutRequest request;
        ::openmldb::api::Dimension* dim = request.add_dimensions();
        dim->set_idx(0);
        dim->set_key(row[0]);
        dim = request.add_dimensions();
        dim->set_idx(1);
        dim->set_key(row[0]);
        dim = request.add_dimensions();
        dim->set_idx(2);
        dim->set_key(row[1]);
        std::string value;
        ASSERT_EQ(0, codec.EncodeRow(row, &value));
        table->Put(0, value, request.dimensions());
    }
    TableIterator* it = table->NewTraverseIterator(0);
    it->SeekToFirst();
    int count = 0;
    while (it->Valid()) {
        count++;
        it->Next();
    }
    ASSERT_EQ(1000, count);
    ASSERT_EQ(1100, (int64_t)it->GetCount());
    delete it;

    it = table->NewTraverseIterator(1);
    it->SeekToFirst();
    count = 0;
    while (it->Valid()) {
        count++;
        it->Next();
    }
    ASSERT_EQ(1000, count);
    ASSERT_EQ(1100, (int64_t)it->GetCount());
    delete it;
    FLAGS_max_traverse_cnt = old_max_traverse;

    delete table;
    std::string path = FLAGS_hdd_root_path + "/33_1";
    RemoveData(path);
}

TEST_F(DiskTableTest, UpdateTTLNew) {

    ::openmldb::api::TableMeta table_meta;
    table_meta.set_tid(34);
    table_meta.set_pid(1);
    table_meta.set_storage_mode(::openmldb::common::kHDD);
    table_meta.set_mode(::openmldb::api::TableMode::kTableLeader);
    ::openmldb::common::ColumnDesc* column_desc = table_meta.add_column_desc();
    column_desc->set_name("card");
    column_desc->set_data_type(::openmldb::type::kString);
    ::openmldb::common::ColumnDesc* column_desc1 = table_meta.add_column_desc();
    column_desc1->set_name("mcc");
    column_desc1->set_data_type(::openmldb::type::kString);
    ::openmldb::common::ColumnDesc* column_desc4 = table_meta.add_column_desc();
    column_desc4->set_name("price");
    column_desc4->set_data_type(::openmldb::type::kBigInt);
    ::openmldb::common::ColumnDesc* column_desc2 = table_meta.add_column_desc();
    column_desc2->set_name("ts1");
    column_desc2->set_data_type(::openmldb::type::kBigInt);
    // column_desc2->set_is_ts_col(true);
    // column_desc2->set_ttl(3);
    ::openmldb::common::ColumnDesc* column_desc3 = table_meta.add_column_desc();
    column_desc3->set_name("ts2");
    column_desc3->set_data_type(::openmldb::type::kBigInt);
    // column_desc3->set_is_ts_col(true);
    // column_desc3->set_ttl(5); 
    ::openmldb::common::ColumnKey* column_key = table_meta.add_column_key();
    column_key->set_index_name("card");
    column_key->add_col_name("card");
    column_key->set_ts_name("ts1");
    auto ttl = column_key->mutable_ttl();
    ttl->set_ttl_type(::openmldb::type::TTLType::kAbsoluteTime);
    ttl->set_abs_ttl(10);
    
    ::openmldb::common::ColumnKey* column_key2 = table_meta.add_column_key();
    column_key2->set_index_name("card1");
    column_key->add_col_name("card");
    column_key2->set_ts_name("ts2");
    auto ttl3 = column_key2->mutable_ttl();
    ttl3->set_ttl_type(::openmldb::type::TTLType::kAbsoluteTime);
    ttl3->set_abs_ttl(5);

    ::openmldb::common::ColumnKey* column_key1 = table_meta.add_column_key();
    column_key1->set_index_name("mcc");
    column_key->add_col_name("mcc");
    column_key1->set_ts_name("ts1");
    auto ttl2 = column_key1->mutable_ttl();
    ttl2->set_ttl_type(::openmldb::type::TTLType::kAbsoluteTime);
    ttl2->set_abs_ttl(10);

    DiskTable* table = new DiskTable(table_meta, FLAGS_hdd_root_path);

    table->Init();
    ASSERT_EQ(10, (int64_t)table->GetIndex(0)->GetTTL()->abs_ttl / (10 * 6000));
    ASSERT_EQ(5, (int64_t)table->GetIndex(1)->GetTTL()->abs_ttl / (10 * 6000));
    ::openmldb::storage::UpdateTTLMeta update_ttl(
        ::openmldb::storage::TTLSt(20 * 10 * 6000, 0, ::openmldb::storage::kAbsoluteTime));
    table->SetTTL(update_ttl);
    ASSERT_EQ(10, (int64_t)table->GetIndex(0)->GetTTL()->abs_ttl / (10 * 6000));
    ASSERT_EQ(5, (int64_t)table->GetIndex(1)->GetTTL()->abs_ttl / (10 * 6000));
    table->SchedGc();
    ASSERT_EQ(20, (int64_t)table->GetIndex(0)->GetTTL()->abs_ttl / (10 * 6000));
    ASSERT_EQ(20, (int64_t)table->GetIndex(1)->GetTTL()->abs_ttl / (10 * 6000));

    delete table;
    std::string path = FLAGS_hdd_root_path + "/34_1";
    RemoveData(path);
}

TEST_F(DiskTableTest, AbsAndLatSetGetNew) {
    ::openmldb::api::TableMeta table_meta;
    table_meta.set_tid(35);
    table_meta.set_pid(1);
    table_meta.set_storage_mode(::openmldb::common::kHDD);
    table_meta.set_mode(::openmldb::api::TableMode::kTableLeader);
    table_meta.set_format_version(1);
    ::openmldb::common::ColumnDesc* column_desc = table_meta.add_column_desc();
    column_desc->set_name("card");
    column_desc->set_data_type(::openmldb::type::kString);
    ::openmldb::common::ColumnDesc* column_desc1 = table_meta.add_column_desc();
    column_desc1->set_name("mcc");
    column_desc1->set_data_type(::openmldb::type::kString);
    ::openmldb::common::ColumnDesc* column_desc4 = table_meta.add_column_desc();
    column_desc4->set_name("price");
    column_desc4->set_data_type(::openmldb::type::kBigInt);
    ::openmldb::common::ColumnDesc* column_desc2 = table_meta.add_column_desc();
    column_desc2->set_name("ts1");
    column_desc2->set_data_type(::openmldb::type::kBigInt);
    // column_desc2->set_is_ts_col(true);
    // column_desc2->set_ttl(3);
    ::openmldb::common::ColumnDesc* column_desc3 = table_meta.add_column_desc();
    column_desc3->set_name("ts2");
    column_desc3->set_data_type(::openmldb::type::kBigInt);
    // column_desc3->set_is_ts_col(true);
    // column_desc3->set_ttl(5); 
    ::openmldb::common::ColumnKey* column_key = table_meta.add_column_key();
    column_key->set_index_name("card");
    column_key->add_col_name("card");
    column_key->set_ts_name("ts1");
    auto ttl = column_key->mutable_ttl();
    ttl->set_ttl_type(::openmldb::type::TTLType::kAbsAndLat);
    ttl->set_abs_ttl(10);
    ttl->set_lat_ttl(12);
    
    ::openmldb::common::ColumnKey* column_key2 = table_meta.add_column_key();
    column_key2->set_index_name("mcc");
    column_key->add_col_name("mcc");
    column_key2->set_ts_name("ts1");
    auto ttl3 = column_key2->mutable_ttl();
    ttl3->set_ttl_type(::openmldb::type::TTLType::kAbsAndLat);
    ttl3->set_abs_ttl(10);
    ttl3->set_lat_ttl(12);


    ::openmldb::common::ColumnKey* column_key1 = table_meta.add_column_key();
    column_key1->set_index_name("mcc1");
    column_key->add_col_name("mcc");
    column_key1->set_ts_name("ts2");
    auto ttl2 = column_key1->mutable_ttl();
    ttl2->set_ttl_type(::openmldb::type::TTLType::kAbsAndLat);
    ttl2->set_abs_ttl(2);
    ttl2->set_lat_ttl(10);

    DiskTable* table = new DiskTable(table_meta, FLAGS_hdd_root_path);

    table->Init();

    codec::SDKCodec codec(table_meta);
    uint64_t now = ::baidu::common::timer::get_micros() / 1000;
    for (int i = 0; i < 10; i++) {
        std::vector<std::string> row = {"card", "mcc",
            "13", std::to_string(now - i * (60 * 1000)), std::to_string(now - i * (60 * 1000))};
        ::openmldb::api::PutRequest request;
        ::openmldb::api::Dimension* dim = request.add_dimensions();
        dim->set_idx(0);
        dim->set_key("card");
        dim = request.add_dimensions();
        dim->set_idx(1);
        dim->set_key("mcc");
        std::string value;
        ASSERT_EQ(0, codec.EncodeRow(row, &value));
        table->Put(0, value, request.dimensions());
    }
    // test get and set ttl
    ASSERT_EQ(10, (int64_t)table->GetIndex(0)->GetTTL()->abs_ttl / (10 * 6000));
    ASSERT_EQ(12, (int64_t)table->GetIndex(0)->GetTTL()->lat_ttl);
    ::openmldb::storage::UpdateTTLMeta update_ttl(
        ::openmldb::storage::TTLSt(1 * 60 * 1000, 3, ::openmldb::storage::kAbsAndLat), "card");
    table->SetTTL(update_ttl);
    ASSERT_EQ(10, (int64_t)table->GetIndex(0)->GetTTL()->abs_ttl / (10 * 6000));
    ASSERT_EQ(12, (int64_t)table->GetIndex(0)->GetTTL()->lat_ttl);
    ASSERT_EQ(10, (int64_t)table->GetIndex(1)->GetTTL()->abs_ttl / (10 * 6000));
    ASSERT_EQ(12, (int64_t)table->GetIndex(1)->GetTTL()->lat_ttl);
    ASSERT_EQ(2, (int64_t)table->GetIndex(2)->GetTTL()->abs_ttl / (10 * 6000));
    ASSERT_EQ(10, (int64_t)table->GetIndex(2)->GetTTL()->lat_ttl);
    table->SchedGc();
    {
        ::openmldb::api::LogEntry entry;
        entry.set_log_index(0);
        ::openmldb::api::Dimension* dim = entry.add_dimensions();
        dim->set_idx(0);
        dim->set_key("card");
        dim = entry.add_dimensions();
        dim->set_idx(1);
        dim->set_key("mcc");
        auto value = entry.mutable_value();
        ASSERT_EQ(0, codec.EncodeRow({"card", "mcc", "12", std::to_string(now - 9 * (60 * 1000) - 10),
                    std::to_string(now - 1 * (60 * 1000))}, value));
        ASSERT_FALSE(table->IsExpire(entry));
    }
    {
        ::openmldb::api::LogEntry entry;
        entry.set_log_index(0);
        ::openmldb::api::Dimension* dim = entry.add_dimensions();
        dim->set_idx(0);
        dim->set_key("card");
        dim = entry.add_dimensions();
        dim->set_idx(1);
        dim->set_key("mcc");
        auto value = entry.mutable_value();
        ASSERT_EQ(0, codec.EncodeRow({"card", "mcc", "12", std::to_string(now - 12 * (60 * 1000)),
                    std::to_string(now - 10 * (60 * 1000))}, value));
        // TODO:IsExpire
        // ASSERT_TRUE(table->IsExpire(entry));
    }
    ASSERT_EQ(1, (int64_t)table->GetIndex(0)->GetTTL()->abs_ttl / (10 * 6000));
    ASSERT_EQ(3, (int64_t)table->GetIndex(0)->GetTTL()->lat_ttl);
    ASSERT_EQ(10, (int64_t)table->GetIndex(1)->GetTTL()->abs_ttl / (10 * 6000));
    ASSERT_EQ(12, (int64_t)table->GetIndex(1)->GetTTL()->lat_ttl);
    ASSERT_EQ(2, (int64_t)table->GetIndex(2)->GetTTL()->abs_ttl / (10 * 6000));
    ASSERT_EQ(10, (int64_t)table->GetIndex(2)->GetTTL()->lat_ttl);

    delete table;
    std::string path = FLAGS_hdd_root_path + "/35_1";
    RemoveData(path);

}

TEST_F(DiskTableTest, AbsOrLatSetGetNew) {
    
    ::openmldb::api::TableMeta table_meta;
    table_meta.set_tid(36);
    table_meta.set_pid(1);
    table_meta.set_storage_mode(::openmldb::common::kHDD);
    table_meta.set_mode(::openmldb::api::TableMode::kTableLeader);
    table_meta.set_format_version(1);
    ::openmldb::common::ColumnDesc* column_desc = table_meta.add_column_desc();
    column_desc->set_name("card");
    column_desc->set_data_type(::openmldb::type::kString);
    ::openmldb::common::ColumnDesc* column_desc1 = table_meta.add_column_desc();
    column_desc1->set_name("mcc");
    column_desc1->set_data_type(::openmldb::type::kString);
    ::openmldb::common::ColumnDesc* column_desc4 = table_meta.add_column_desc();
    column_desc4->set_name("price");
    column_desc4->set_data_type(::openmldb::type::kBigInt);
    ::openmldb::common::ColumnDesc* column_desc2 = table_meta.add_column_desc();
    column_desc2->set_name("ts1");
    column_desc2->set_data_type(::openmldb::type::kBigInt);
    // column_desc2->set_is_ts_col(true);
    // column_desc2->set_ttl(3);
    ::openmldb::common::ColumnDesc* column_desc3 = table_meta.add_column_desc();
    column_desc3->set_name("ts2");
    column_desc3->set_data_type(::openmldb::type::kBigInt);
    // column_desc3->set_is_ts_col(true);
    // column_desc3->set_ttl(5); 
    ::openmldb::common::ColumnKey* column_key = table_meta.add_column_key();
    column_key->set_index_name("card");
    column_key->add_col_name("card");
    column_key->set_ts_name("ts1");
    auto ttl = column_key->mutable_ttl();
    ttl->set_ttl_type(::openmldb::type::TTLType::kAbsOrLat);
    ttl->set_abs_ttl(10);
    ttl->set_lat_ttl(12);
    
    ::openmldb::common::ColumnKey* column_key2 = table_meta.add_column_key();
    column_key2->set_index_name("mcc");
    column_key->add_col_name("mcc");
    column_key2->set_ts_name("ts1");
    auto ttl3 = column_key2->mutable_ttl();
    ttl3->set_ttl_type(::openmldb::type::TTLType::kAbsOrLat);
    ttl3->set_abs_ttl(10);
    ttl3->set_lat_ttl(12);


    ::openmldb::common::ColumnKey* column_key1 = table_meta.add_column_key();
    column_key1->set_index_name("mcc1");
    column_key->add_col_name("mcc");
    column_key1->set_ts_name("ts2");
    auto ttl2 = column_key1->mutable_ttl();
    ttl2->set_ttl_type(::openmldb::type::TTLType::kAbsOrLat);
    ttl2->set_abs_ttl(2);
    ttl2->set_lat_ttl(10);

    DiskTable* table = new DiskTable(table_meta, FLAGS_hdd_root_path);

    table->Init();
    
    codec::SDKCodec codec(table_meta);
    uint64_t now = ::baidu::common::timer::get_micros() / 1000;
    for (int i = 0; i < 10; i++) {
        std::vector<std::string> row = {"card", "mcc",
            "13", std::to_string(now - i * (60 * 1000)), std::to_string(now - i * (60 * 1000))};
        ::openmldb::api::PutRequest request;
        ::openmldb::api::Dimension* dim = request.add_dimensions();
        dim->set_idx(0);
        dim->set_key("card");
        dim = request.add_dimensions();
        dim->set_idx(1);
        dim->set_key("mcc");
        std::string value;
        ASSERT_EQ(0, codec.EncodeRow(row, &value));
        table->Put(0, value, request.dimensions());
    }
    // test get and set ttl
    ASSERT_EQ(10, (int64_t)table->GetIndex(0)->GetTTL()->abs_ttl / (10 * 6000));
    ASSERT_EQ(12, (int64_t)table->GetIndex(0)->GetTTL()->lat_ttl);
    ::openmldb::storage::UpdateTTLMeta update_ttl(
        ::openmldb::storage::TTLSt(1 * 60 * 1000, 3, ::openmldb::storage::kAbsOrLat), "card");
    table->SetTTL(update_ttl);
    ASSERT_EQ(10, (int64_t)table->GetIndex(0)->GetTTL()->abs_ttl / (10 * 6000));
    ASSERT_EQ(12, (int64_t)table->GetIndex(0)->GetTTL()->lat_ttl);
    ASSERT_EQ(10, (int64_t)table->GetIndex(1)->GetTTL()->abs_ttl / (10 * 6000));
    ASSERT_EQ(12, (int64_t)table->GetIndex(1)->GetTTL()->lat_ttl);
    ASSERT_EQ(2, (int64_t)table->GetIndex(2)->GetTTL()->abs_ttl / (10 * 6000));
    ASSERT_EQ(10, (int64_t)table->GetIndex(2)->GetTTL()->lat_ttl);
    table->SchedGc();
    ASSERT_EQ(1, (int64_t)table->GetIndex(0)->GetTTL()->abs_ttl / (10 * 6000));
    ASSERT_EQ(3, (int64_t)table->GetIndex(0)->GetTTL()->lat_ttl);
    {
        ::openmldb::api::LogEntry entry;
        entry.set_log_index(0);
        ::openmldb::test::AddDimension(0, "card", &entry);
        ::openmldb::test::AddDimension(1, "mcc", &entry);
        auto value = entry.mutable_value();
        ASSERT_EQ(0, codec.EncodeRow({"card", "mcc", "12", std::to_string(now - 9 * (60 * 1000) - 10),
                    std::to_string(now - 1 * (60 * 1000))}, value));
        ASSERT_FALSE(table->IsExpire(entry));
    }
    {
        ::openmldb::api::LogEntry entry;
        entry.set_log_index(0);
        ::openmldb::test::AddDimension(0, "card", &entry);
        ::openmldb::test::AddDimension(1, "mcc", &entry);
        auto value = entry.mutable_value();
        ASSERT_EQ(0, codec.EncodeRow({"card", "mcc", "12", std::to_string(now - 12 * (60 * 1000)),
                    std::to_string(now - 10 * (60 * 1000))}, value));
        // TODO:IsExpire
        // ASSERT_TRUE(table->IsExpire(entry));
    }
    ASSERT_EQ(1, (int64_t)table->GetIndex(0)->GetTTL()->abs_ttl / (10 * 6000));
    ASSERT_EQ(3, (int64_t)table->GetIndex(0)->GetTTL()->lat_ttl);
    ASSERT_EQ(10, (int64_t)table->GetIndex(1)->GetTTL()->abs_ttl / (10 * 6000));
    ASSERT_EQ(12, (int64_t)table->GetIndex(1)->GetTTL()->lat_ttl);
    ASSERT_EQ(2, (int64_t)table->GetIndex(2)->GetTTL()->abs_ttl / (10 * 6000));
    ASSERT_EQ(10, (int64_t)table->GetIndex(2)->GetTTL()->lat_ttl);

    delete table;
    std::string path = FLAGS_hdd_root_path + "/36_1";
    RemoveData(path);
}

TEST_F(DiskTableTest, GcAbsOrLatNew) {

    int32_t offset = FLAGS_gc_safe_offset;
    FLAGS_gc_safe_offset = 0;

    ::openmldb::api::TableMeta table_meta;
    table_meta.set_tid(37);
    table_meta.set_pid(1);
    table_meta.set_storage_mode(::openmldb::common::kHDD);
    table_meta.set_mode(::openmldb::api::TableMode::kTableLeader);
    ::openmldb::common::ColumnDesc* column_desc = table_meta.add_column_desc();
    column_desc->set_name("idx0");
    column_desc->set_data_type(::openmldb::type::kString);
    ::openmldb::common::ColumnDesc* column_desc1 = table_meta.add_column_desc();
    column_desc1->set_name("value");
    column_desc1->set_data_type(::openmldb::type::kString);
    
    ::openmldb::common::ColumnKey* column_key = table_meta.add_column_key();
    column_key->set_index_name("idx0");
    column_key->add_col_name("idx0");
    auto ttl = column_key->mutable_ttl();
    ttl->set_ttl_type(::openmldb::type::TTLType::kAbsOrLat);
    ttl->set_abs_ttl(4);
    ttl->set_lat_ttl(3);

    DiskTable* table = new DiskTable(table_meta, FLAGS_hdd_root_path);

    table->Init();

    

    uint64_t now = ::baidu::common::timer::get_micros() / 1000;
    table->Put("test1", now - 3 * (60 * 1000) - 1000, "value1", 6);
    table->Put("test1", now - 3 * (60 * 1000) - 1000, "value1", 6);
    table->Put("test1", now - 2 * (60 * 1000) - 1000, "value2", 6);
    table->Put("test1", now - 1 * (60 * 1000) - 1000, "value3", 6);
    table->Put("test2", now - 4 * (60 * 1000) - 1000, "value4", 6);
    table->Put("test2", now - 2 * (60 * 1000) - 1000, "value5", 6);
    table->Put("test2", now - 1 * (60 * 1000) - 1000, "value6", 6);
    ASSERT_EQ(7, (int64_t)table->GetRecordCnt());
    ASSERT_EQ(7, (int64_t)table->GetRecordIdxCnt());
    ASSERT_EQ(2, (int64_t)table->GetRecordPkCnt());
    ::openmldb::storage::UpdateTTLMeta update_ttl(
        ::openmldb::storage::TTLSt(3 * 60 * 1000, 0, ::openmldb::storage::kAbsOrLat));
    table->SetTTL(update_ttl);
    table->SchedGc();
    ASSERT_EQ(5, (int64_t)table->GetRecordCnt());
    ASSERT_EQ(5, (int64_t)table->GetRecordIdxCnt());
    ASSERT_EQ(2, (int64_t)table->GetRecordPkCnt());
    update_ttl = ::openmldb::storage::UpdateTTLMeta(::openmldb::storage::TTLSt(0, 1, ::openmldb::storage::kAbsOrLat));
    table->SetTTL(update_ttl);
    table->SchedGc();
    ASSERT_EQ(4, (int64_t)table->GetRecordCnt());
    ASSERT_EQ(4, (int64_t)table->GetRecordIdxCnt());
    ASSERT_EQ(2, (int64_t)table->GetRecordPkCnt());
    update_ttl = ::openmldb::storage::UpdateTTLMeta(
        ::openmldb::storage::TTLSt(1 * 60 * 1000, 1, ::openmldb::storage::kAbsOrLat));
    table->SetTTL(update_ttl);
    table->SchedGc();
    ASSERT_EQ(2, (int64_t)table->GetRecordCnt());
    ASSERT_EQ(2, (int64_t)table->GetRecordIdxCnt());
    ASSERT_EQ(2, (int64_t)table->GetRecordPkCnt());
    {
        ::openmldb::api::LogEntry entry;
        entry.set_log_index(0);
        entry.set_pk("test1");
        entry.set_ts(now - 5 * (60 * 1000) - 1000);
        entry.set_value(::openmldb::test::EncodeKV("test1", "value1"));
        ASSERT_TRUE(table->IsExpire(entry));
    }
    {
        ::openmldb::api::LogEntry entry;
        entry.set_log_index(0);
        entry.set_pk("test1");
        entry.set_ts(now - 3 * (60 * 1000) - 1000);
        entry.set_value(::openmldb::test::EncodeKV("test1", "value1"));
        ASSERT_TRUE(table->IsExpire(entry));
    }
    {
        ::openmldb::api::LogEntry entry;
        entry.set_log_index(0);
        entry.set_pk("test1");
        entry.set_ts(now - 2 * (60 * 1000) - 1000);
        entry.set_value(::openmldb::test::EncodeKV("test1", "value1"));
        ASSERT_TRUE(table->IsExpire(entry));
    }
    {
        ::openmldb::api::LogEntry entry;
        entry.set_log_index(0);
        entry.set_pk("test1");
        entry.set_ts(now - 1 * (60 * 1000) - 1000);
        entry.set_value(::openmldb::test::EncodeKV("test1", "value1"));
        ASSERT_TRUE(table->IsExpire(entry));
    }
    table->SchedGc();
    ASSERT_EQ(0, (int64_t)table->GetRecordCnt());
    ASSERT_EQ(0, (int64_t)table->GetRecordIdxCnt());
    ASSERT_EQ(2, (int64_t)table->GetRecordPkCnt());
    {
        ::openmldb::api::LogEntry entry;
        entry.set_log_index(0);
        entry.set_pk("test1");
        entry.set_ts(now - 1 * (60 * 1000) - 1000);
        entry.set_value(::openmldb::test::EncodeKV("test1", "value1"));
        ASSERT_TRUE(table->IsExpire(entry));
    }
    FLAGS_gc_safe_offset = offset;

    delete table;
    std::string path = FLAGS_hdd_root_path + "/37_1";
    RemoveData(path);
}

TEST_F(DiskTableTest, GcAbsAndLat) {

    int32_t offset = FLAGS_gc_safe_offset;
    FLAGS_gc_safe_offset = 0;

    ::openmldb::api::TableMeta table_meta;
    table_meta.set_tid(38);
    table_meta.set_pid(1);
    table_meta.set_storage_mode(::openmldb::common::kHDD);
    table_meta.set_mode(::openmldb::api::TableMode::kTableLeader);
    ::openmldb::common::ColumnDesc* column_desc = table_meta.add_column_desc();
    column_desc->set_name("idx0");
    column_desc->set_data_type(::openmldb::type::kString);
    ::openmldb::common::ColumnDesc* column_desc1 = table_meta.add_column_desc();
    column_desc1->set_name("value");
    column_desc1->set_data_type(::openmldb::type::kString);
    
    ::openmldb::common::ColumnKey* column_key = table_meta.add_column_key();
    column_key->set_index_name("idx0");
    column_key->add_col_name("idx0");
    auto ttl = column_key->mutable_ttl();
    ttl->set_ttl_type(::openmldb::type::TTLType::kAbsAndLat);
    ttl->set_abs_ttl(3);
    ttl->set_lat_ttl(3);

    DiskTable* table = new DiskTable(table_meta, FLAGS_hdd_root_path);

    table->Init();

    uint64_t now = ::baidu::common::timer::get_micros() / 1000;
    table->Put("test1", now - 3 * (60 * 1000) - 1000, "value1", 6);
    table->Put("test1", now - 3 * (60 * 1000) - 1000, "value1", 6);
    table->Put("test1", now - 2 * (60 * 1000) - 1000, "value2", 6);
    table->Put("test1", now - 1 * (60 * 1000) - 1000, "value3", 6);
    table->Put("test2", now - 4 * (60 * 1000) - 1000, "value4", 6);
    table->Put("test2", now - 3 * (60 * 1000) - 1000, "value5", 6);
    table->Put("test2", now - 2 * (60 * 1000) - 1000, "value6", 6);
    ASSERT_EQ(7, (int64_t)table->GetRecordCnt());
    ASSERT_EQ(7, (int64_t)table->GetRecordIdxCnt());
    ASSERT_EQ(2, (int64_t)table->GetRecordPkCnt());
    ::openmldb::storage::UpdateTTLMeta update_ttl(
        ::openmldb::storage::TTLSt(1 * 60 * 1000, 0, ::openmldb::storage::kAbsAndLat));
    table->SetTTL(update_ttl);
    table->SchedGc();
    ASSERT_EQ(6, (int64_t)table->GetRecordCnt());
    ASSERT_EQ(6, (int64_t)table->GetRecordIdxCnt());
    ASSERT_EQ(2, (int64_t)table->GetRecordPkCnt());
    {
        ::openmldb::api::LogEntry entry;
        entry.set_log_index(0);
        entry.set_pk("test1");
        entry.set_ts(now - 4 * (60 * 1000) - 1000);
        entry.set_value(::openmldb::test::EncodeKV("test1", "value1"));
        ASSERT_FALSE(table->IsExpire(entry));
    }
    {
        ::openmldb::api::LogEntry entry;
        entry.set_log_index(0);
        entry.set_pk("test1");
        entry.set_ts(now - 3 * (60 * 1000) - 1000);
        entry.set_value(::openmldb::test::EncodeKV("test1", "value1"));
        ASSERT_FALSE(table->IsExpire(entry));
    }
    {
        ::openmldb::api::LogEntry entry;
        entry.set_log_index(0);
        entry.set_pk("test1");
        entry.set_ts(now - 2 * (60 * 1000) - 1000);
        entry.set_value(::openmldb::test::EncodeKV("test1", "value1"));
        ASSERT_FALSE(table->IsExpire(entry));
    }
    update_ttl = ::openmldb::storage::UpdateTTLMeta(::openmldb::storage::TTLSt(0, 1, ::openmldb::storage::kAbsAndLat));
    table->SetTTL(update_ttl);
    table->SchedGc();
    ASSERT_EQ(6, (int64_t)table->GetRecordCnt());
    ASSERT_EQ(6, (int64_t)table->GetRecordIdxCnt());
    ASSERT_EQ(2, (int64_t)table->GetRecordPkCnt());
    update_ttl = ::openmldb::storage::UpdateTTLMeta(
        ::openmldb::storage::TTLSt(1 * 60 * 1000, 1, ::openmldb::storage::kAbsAndLat));
    table->SetTTL(update_ttl);
    table->SchedGc();
    ASSERT_EQ(6, (int64_t)table->GetRecordCnt());
    ASSERT_EQ(6, (int64_t)table->GetRecordIdxCnt());
    ASSERT_EQ(2, (int64_t)table->GetRecordPkCnt());
    table->SchedGc();
    ASSERT_EQ(2, (int64_t)table->GetRecordCnt());
    ASSERT_EQ(2, (int64_t)table->GetRecordIdxCnt());
    ASSERT_EQ(2, (int64_t)table->GetRecordPkCnt());
    {
        ::openmldb::api::LogEntry entry;
        entry.set_log_index(0);
        entry.set_pk("test1");
        entry.set_ts(now - 3 * (60 * 1000) - 1000);
        entry.set_value(::openmldb::test::EncodeKV("test1", "value1"));
        ASSERT_TRUE(table->IsExpire(entry));
    }
    {
        ::openmldb::api::LogEntry entry;
        entry.set_log_index(0);
        entry.set_pk("test1");
        entry.set_ts(now - 2 * (60 * 1000) - 1000);
        entry.set_value(::openmldb::test::EncodeKV("test1", "value1"));
        ASSERT_TRUE(table->IsExpire(entry));
    }
    {
        ::openmldb::api::LogEntry entry;
        entry.set_log_index(0);
        entry.set_pk("test1");
        entry.set_value(::openmldb::test::EncodeKV("test1", "value1"));
        entry.set_ts(now - 1 * (60 * 1000) - 1000);
        ASSERT_FALSE(table->IsExpire(entry));
    }
    FLAGS_gc_safe_offset = offset;

    delete table;
    std::string path = FLAGS_hdd_root_path + "/38_1";
    RemoveData(path);
}


}  // namespace storage
}  // namespace openmldb

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    ::openmldb::base::SetLogLevel(INFO);
    FLAGS_hdd_root_path = "/tmp/" + std::to_string(::openmldb::storage::GenRand());
    FLAGS_ssd_root_path = "/tmp/" + std::to_string(::openmldb::storage::GenRand());
    return RUN_ALL_TESTS();
}
