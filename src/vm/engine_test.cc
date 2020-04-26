/*
 * engine_test.cc
 * Copyright (C) 4paradigm.com 2019 wangtaize <wangtaize@4paradigm.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "vm/engine.h"
#include <utility>
#include <vector>
#include "base/texttable.h"
#include "case/sql_case.h"
#include "codec/list_iterator_codec.h"
#include "codec/row_codec.h"
#include "gtest/gtest.h"
#include "gtest/internal/gtest-param-util.h"
#include "llvm/ExecutionEngine/Orc/LLJIT.h"
#include "llvm/IR/Function.h"
#include "llvm/IR/IRBuilder.h"
#include "llvm/IR/InstrTypes.h"
#include "llvm/IR/LegacyPassManager.h"
#include "llvm/IR/Module.h"
#include "llvm/Support/InitLLVM.h"
#include "llvm/Support/TargetSelect.h"
#include "llvm/Support/raw_ostream.h"
#include "llvm/Transforms/AggressiveInstCombine/AggressiveInstCombine.h"
#include "llvm/Transforms/InstCombine/InstCombine.h"
#include "llvm/Transforms/Scalar.h"
#include "llvm/Transforms/Scalar/GVN.h"
#include "parser/parser.h"
#include "plan/planner.h"
#include "vm/test_base.h"

using namespace llvm;       // NOLINT (build/namespaces)
using namespace llvm::orc;  // NOLINT (build/namespaces)

namespace fesql {
namespace vm {
using fesql::codec::ArrayListV;
using fesql::codec::Row;
using fesql::sqlcase::SQLCase;
enum EngineRunMode { RUNBATCH, RUNONE };

const bool IS_DEBUG = true;
std::vector<SQLCase> InitCases(std::string yaml_path);
std::vector<SQLCase> InitCases(std::string yaml_path) {
    std::vector<SQLCase> cases;
    SQLCase::CreateSQLCasesFromYaml(
        fesql::sqlcase::FindFesqlDirPath() + yaml_path, cases);
    return cases;
}

void CheckSchema(const vm::Schema& schema, const vm::Schema& exp_schema);
void CheckRows(const vm::Schema& schema, const std::vector<Row>& rows,
               const std::vector<Row>& exp_rows);
void PrintRows(const vm::Schema& schema, const std::vector<Row>& rows);
void CheckSchema(const vm::Schema& schema, const vm::Schema& exp_schema) {
    ASSERT_EQ(schema.size(), exp_schema.size());
    for (int i = 0; i < schema.size(); i++) {
        ASSERT_EQ(schema.Get(i).DebugString(), exp_schema.Get(i).DebugString());
    }
}
void PrintRows(const vm::Schema& schema, const std::vector<Row>& rows) {
    std::ostringstream oss;
    RowView row_view(schema);
    ::fesql::base::TextTable t('-', '|', '+');
    // Add Header
    for (int i = 0; i < schema.size(); i++) {
        t.add(schema.Get(i).name());
    }
    t.endOfRow();
    if (rows.empty()) {
        t.add("Empty set");
        t.endOfRow();
        return;
    }

    int cnt = 0;
    for (auto row : rows) {
        row_view.Reset(row.buf());
        for (int idx = 0; idx < schema.size(); idx++) {
            std::string str = row_view.GetAsString(idx);
            t.add(str);
        }
        t.endOfRow();
    }
    oss << t << std::endl;
    LOG(INFO) << "\n" << oss.str() << "\n";
}
void CheckRows(const vm::Schema& schema, const std::vector<Row>& rows,
               const std::vector<Row>& exp_rows) {
    ASSERT_EQ(rows.size(), exp_rows.size());
    RowView row_view(schema);
    RowView row_view_exp(schema);
    for (size_t i = 0; i < rows.size(); i++) {
        row_view.Reset(rows[i].buf());
        row_view_exp.Reset(exp_rows[i].buf());
        ASSERT_EQ(row_view.GetRowString(), row_view_exp.GetRowString());
    }
}
class EngineTest : public ::testing::TestWithParam<SQLCase> {
 public:
    EngineTest() {}
    virtual ~EngineTest() {}


};

void BuildWindow(std::vector<Row>& rows,  // NOLINT
                 int8_t** buf) {
    ::fesql::type::TableDef table;
    BuildRows(table, rows);
    ::fesql::codec::ArrayListV<Row>* w =
        new ::fesql::codec::ArrayListV<Row>(&rows);
    *buf = reinterpret_cast<int8_t*>(w);
}
void BuildT2Window(std::vector<Row>& rows,  // NOLINT
                   int8_t** buf) {
    ::fesql::type::TableDef table;
    BuildT2Rows(table, rows);
    ArrayListV<Row>* w = new ArrayListV<Row>(&rows);
    *buf = reinterpret_cast<int8_t*>(w);
}
void BuildWindowUnique(std::vector<Row>& rows,  // NOLINT
                       int8_t** buf) {
    ::fesql::type::TableDef table;
    BuildTableDef(table);

    {
        codec::RowBuilder builder(table.columns());
        std::string str = "1";
        std::string str0 = "0";
        uint32_t total_size = builder.CalTotalLength(str.size() + str0.size());
        int8_t* ptr = static_cast<int8_t*>(malloc(total_size));

        builder.SetBuffer(ptr, total_size);
        builder.AppendString("0", 1);
        builder.AppendInt32(1);
        builder.AppendInt16(5);
        builder.AppendFloat(1.1f);
        builder.AppendDouble(11.1);
        builder.AppendInt64(1);
        builder.AppendString(str.c_str(), 1);
        rows.push_back(Row(ptr, total_size));
    }
    {
        codec::RowBuilder builder(table.columns());
        std::string str = "22";
        std::string str0 = "0";
        uint32_t total_size = builder.CalTotalLength(str.size() + str0.size());
        int8_t* ptr = static_cast<int8_t*>(malloc(total_size));
        builder.SetBuffer(ptr, total_size);
        builder.AppendString("0", 1);
        builder.AppendInt32(2);
        builder.AppendInt16(5);
        builder.AppendFloat(2.2f);
        builder.AppendDouble(22.2);
        builder.AppendInt64(2);
        builder.AppendString(str.c_str(), str.size());
        rows.push_back(Row(ptr, total_size));
    }
    {
        codec::RowBuilder builder(table.columns());
        std::string str = "333";
        std::string str0 = "0";
        uint32_t total_size = builder.CalTotalLength(str.size() + str0.size());
        int8_t* ptr = static_cast<int8_t*>(malloc(total_size));
        builder.SetBuffer(ptr, total_size);
        builder.AppendString("1", 1);
        builder.AppendInt32(3);
        builder.AppendInt16(5);
        builder.AppendFloat(3.3f);
        builder.AppendDouble(33.3);
        builder.AppendInt64(3);
        builder.AppendString(str.c_str(), str.size());
        rows.push_back(Row(ptr, total_size));
    }
    {
        codec::RowBuilder builder(table.columns());
        std::string str = "4444";
        std::string str0 = "0";
        uint32_t total_size = builder.CalTotalLength(str.size() + str0.size());
        int8_t* ptr = static_cast<int8_t*>(malloc(total_size));
        builder.SetBuffer(ptr, total_size);
        builder.AppendString("1", 1);
        builder.AppendInt32(4);
        builder.AppendInt16(5);
        builder.AppendFloat(4.4f);
        builder.AppendDouble(44.4);
        builder.AppendInt64(4);
        builder.AppendString("4444", str.size());
        rows.push_back(Row(ptr, total_size));
    }
    {
        codec::RowBuilder builder(table.columns());
        std::string str =
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            "aaa"
            "a";
        std::string str0 = "0";
        uint32_t total_size = builder.CalTotalLength(str.size() + str0.size());
        int8_t* ptr = static_cast<int8_t*>(malloc(total_size));
        builder.SetBuffer(ptr, total_size);
        builder.AppendString("2", 1);
        builder.AppendInt32(5);
        builder.AppendInt16(5);
        builder.AppendFloat(5.5f);
        builder.AppendDouble(55.5);
        builder.AppendInt64(5);
        builder.AppendString(str.c_str(), str.size());
        rows.push_back(Row(ptr, total_size));
    }

    ::fesql::codec::ArrayListV<Row>* w =
        new ::fesql::codec::ArrayListV<Row>(&rows);
    *buf = reinterpret_cast<int8_t*>(w);
}
void StoreData(::fesql::storage::Table* table, const std::vector<Row>& rows) {
    ASSERT_TRUE(table->Init());
    for (auto row : rows) {
        ASSERT_TRUE(table->Put(reinterpret_cast<char*>(row.buf()), row.size()));
    }
}
void StoreData(::fesql::storage::Table* table, int8_t* rows) {
    ArrayListV<Row>* window = reinterpret_cast<ArrayListV<Row>*>(rows);
    auto w = window->GetIterator();
    ASSERT_TRUE(w->Valid());
    Row row = w->GetValue();
    w->Next();
    ASSERT_TRUE(table->Put(reinterpret_cast<char*>(row.buf()), row.size()));

    ASSERT_TRUE(w->Valid());
    row = w->GetValue();
    w->Next();
    ASSERT_TRUE(table->Put(reinterpret_cast<char*>(row.buf()), row.size()));

    ASSERT_TRUE(w->Valid());
    row = w->GetValue();
    w->Next();
    ASSERT_TRUE(table->Put(reinterpret_cast<char*>(row.buf()), row.size()));
    ASSERT_TRUE(w->Valid());
    row = w->GetValue();
    w->Next();
    ASSERT_TRUE(table->Put(reinterpret_cast<char*>(row.buf()), row.size()));
    ASSERT_TRUE(w->Valid());
    row = w->GetValue();
    w->Next();
    ASSERT_TRUE(table->Put(reinterpret_cast<char*>(row.buf()), row.size()));
}
void RequestModeCheck(SQLCase& sql_case) {  // NOLINT
    int32_t input_cnt = sql_case.CountInputs();
    // Init catalog
    std::map<std::string, std::shared_ptr<::fesql::storage::Table>>
        name_table_map;
    auto catalog = BuildCommonCatalog();
    for (int32_t i = 0; i < input_cnt; i++) {
        type::TableDef table_def;
        sql_case.ExtractInputTableDef(table_def, i);
        std::shared_ptr<::fesql::storage::Table> table(
            new ::fesql::storage::Table(i + 1, 1, table_def));
        name_table_map[table_def.name()] = table;
        ASSERT_TRUE(AddTable(catalog, table_def, table));
    }

    // Init engine and run session
    std::cout << sql_case.sql_str() << std::endl;
    base::Status get_status;

    Engine engine(catalog);
    RequestRunSession session;
    if (IS_DEBUG) {
        session.EnableDebug();
    }

    bool ok =
        engine.Get(sql_case.sql_str(), sql_case.db(), session, get_status);
    ASSERT_TRUE(ok);

    const std::string& request_name = session.GetRequestName();
    const vm::Schema& request_schema = session.GetRequestSchema();

    std::vector<Row> request_data;
    for (int32_t i = 0; i < input_cnt; i++) {
        auto input = sql_case.inputs()[i];
        if (input.name_ == request_name) {
            ASSERT_TRUE(sql_case.ExtractInputData(request_data, i));
            continue;
        }
        std::vector<Row> rows;
        sql_case.ExtractInputData(rows, i);
        if (!rows.empty()) {
            StoreData(name_table_map[input.name_].get(), rows);
        }
    }

    int32_t ret = -1;
    DLOG(INFO) << "RUN IN MODE REQUEST";
    std::vector<Row> output;
    vm::Schema schema;
    schema = session.GetSchema();
    PrintSchema(schema);
    std::ostringstream oss;
    session.GetPhysicalPlan()->Print(oss, "");
    std::cout << "physical plan:\n" << oss.str() << std::endl;

    std::ostringstream runner_oss;
    session.GetRunner()->Print(runner_oss, "");
    std::cout << "runner plan:\n" << runner_oss.str() << std::endl;

    // Check Output Schema
    std::vector<Row> case_output_data;
    type::TableDef case_output_table;
    ASSERT_TRUE(sql_case.ExtractOutputData(case_output_data));
    ASSERT_TRUE(sql_case.ExtractOutputSchema(case_output_table));
    CheckSchema(schema, case_output_table.columns());

    // Check Output Data
    auto request_table = name_table_map[request_name];
    ASSERT_TRUE(request_table->Init());
    for (auto in_row : request_data) {
        Row out_row;
        ret = session.Run(in_row, &out_row);
        ASSERT_EQ(0, ret);
        ASSERT_TRUE(request_table->Put(in_row.data(), in_row.size()));
        output.push_back(out_row);
    }
    LOG(INFO) << "expect result:\n";
    PrintRows(case_output_table.columns(), case_output_data);
    LOG(INFO) << "real result:\n";
    PrintRows(schema, output);
    CheckRows(schema, output, case_output_data);
}
void BatchModeCheck(SQLCase& sql_case) {  // NOLINT
    int32_t input_cnt = sql_case.CountInputs();

    // Init catalog
    std::map<std::string, std::shared_ptr<::fesql::storage::Table>>
        name_table_map;
    auto catalog = BuildCommonCatalog();
    for (int32_t i = 0; i < input_cnt; i++) {
        type::TableDef table_def;
        sql_case.ExtractInputTableDef(table_def, i);
        std::shared_ptr<::fesql::storage::Table> table(
            new ::fesql::storage::Table(i + 1, 1, table_def));
        name_table_map[table_def.name()] = table;
        ASSERT_TRUE(AddTable(catalog, table_def, table));
    }

    // Init engine and run session
    std::cout << sql_case.sql_str() << std::endl;
    base::Status get_status;

    Engine engine(catalog);
    BatchRunSession session;
    if (IS_DEBUG) {
        session.EnableDebug();
    }

    bool ok =
        engine.Get(sql_case.sql_str(), sql_case.db(), session, get_status);
    ASSERT_TRUE(ok);
    std::vector<Row> request_data;
    for (int32_t i = 0; i < input_cnt; i++) {
        auto input = sql_case.inputs()[i];
        std::vector<Row> rows;
        sql_case.ExtractInputData(rows, i);
        if (!rows.empty()) {
            StoreData(name_table_map[input.name_].get(), rows);
        }
    }

    int32_t ret = -1;
    DLOG(INFO) << "RUN IN MODE BATCH";
    vm::Schema schema;
    schema = session.GetSchema();
    PrintSchema(schema);
    std::ostringstream oss;
    session.GetPhysicalPlan()->Print(oss, "");
    std::cout << "physical plan:\n" << oss.str() << std::endl;

    std::ostringstream runner_oss;
    session.GetRunner()->Print(runner_oss, "");
    std::cout << "runner plan:\n" << runner_oss.str() << std::endl;

    // Check Output Schema
    std::vector<Row> case_output_data;
    type::TableDef case_output_table;
    ASSERT_TRUE(sql_case.ExtractOutputData(case_output_data));
    ASSERT_TRUE(sql_case.ExtractOutputSchema(case_output_table));
    CheckSchema(schema, case_output_table.columns());

    // Check Output Data
    std::vector<Row> output;
    std::vector<int8_t*> output_ptr;
    ASSERT_EQ(0, session.Run(output_ptr));
    for (auto ptr : output_ptr) {
        output.push_back(Row(ptr, RowView::GetSize(ptr)));
    }
    LOG(INFO) << "expect result:\n";
    PrintRows(case_output_table.columns(), case_output_data);
    LOG(INFO) << "real result:\n";
    PrintRows(schema, output);
    CheckRows(schema, output, case_output_data);
}

INSTANTIATE_TEST_CASE_P(
    EngineRequstSimpleQuery, EngineTest,
    testing::ValuesIn(InitCases("/cases/batch_query/simple_query.yaml")));
INSTANTIATE_TEST_CASE_P(
    EngineBatchSimpleQuery, EngineTest,
    testing::ValuesIn(InitCases("/cases/query/simple_query.yaml")));

INSTANTIATE_TEST_CASE_P(
    EngineBatchLastJoinQuery, EngineTest,
    testing::ValuesIn(InitCases("/cases/batch_query/last_join_query.yaml")));

INSTANTIATE_TEST_CASE_P(
    EngineRequestLastJoinQuery, EngineTest,
    testing::ValuesIn(InitCases("/cases/query/last_join_query.yaml")));

INSTANTIATE_TEST_CASE_P(EngineBatchtLastJoinWindowQuery, EngineTest,
                        testing::ValuesIn(InitCases(
                            "/cases/batch_query/last_join_window_query.yaml")));

INSTANTIATE_TEST_CASE_P(
    EngineRequestLastJoinWindowQuery, EngineTest,
    testing::ValuesIn(InitCases("/cases/query/last_join_window_query.yaml")));

INSTANTIATE_TEST_CASE_P(
    EngineBatchWindowQuery, EngineTest,
    testing::ValuesIn(InitCases("/cases/batch_query/window_query.yaml")));

INSTANTIATE_TEST_CASE_P(
    EngineRequestWindowQuery, EngineTest,
    testing::ValuesIn(InitCases("/cases/query/window_query.yaml")));

INSTANTIATE_TEST_CASE_P(
    EngineBatchGroupQuery, EngineTest,
    testing::ValuesIn(InitCases("/cases/batch_query/group_query.yaml")));

TEST_P(EngineTest, test_engine) {
    ParamType sql_case = GetParam();
    LOG(INFO) << sql_case.desc();
    if (sql_case.mode() == "request") {
        RequestModeCheck(sql_case);
    } else if (sql_case.mode() == "batch") {
        BatchModeCheck(sql_case);
    } else {
        LOG(WARNING) << "Invalid Check Mode " << sql_case.mode();
        FAIL();
    }
}
}  // namespace vm
}  // namespace fesql

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    return RUN_ALL_TESTS();
}
