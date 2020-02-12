/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * node_manager_test.cc
 *
 * Author: chenjing
 * Date: 2019/10/28
 *--------------------------------------------------------------------------
 **/

#include "node/node_manager.h"
#include "gtest/gtest.h"

namespace fesql {
namespace node {

class NodeManagerTest : public ::testing::Test {
 public:
    NodeManagerTest() {}

    ~NodeManagerTest() {}
};

TEST_F(NodeManagerTest, MakeSQLNode) {
    NodeManager *manager = new NodeManager();
    manager->MakeSQLNode(node::kSelectStmt);
    manager->MakeSQLNode(node::kOrderBy);
    manager->MakeSQLNode(node::kLimit);

    manager->MakePlanNode(node::kPlanTypeSelect);
    manager->MakePlanNode(node::kProjectList);
    manager->MakePlanNode(node::kProject);
    manager->MakePlanNode(node::kProject);
    manager->MakePlanNode(node::kOpExpr);

    ASSERT_EQ(3, manager->GetParserNodeListSize());
    ASSERT_EQ(5, manager->GetPlanNodeListSize());
    delete manager;
}

}  // namespace node
}  // namespace fesql

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
