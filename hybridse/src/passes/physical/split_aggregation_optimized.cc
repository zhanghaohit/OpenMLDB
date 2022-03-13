/*
 * Copyright 2021 4paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "passes/physical/split_aggregation_optimized.h"
#include "vm/engine.h"
#include "vm/physical_op.h"

namespace hybridse {
namespace passes {

SplitAggregationOptimized::SplitAggregationOptimized(PhysicalPlanContext* plan_ctx) : TransformUpPysicalPass(plan_ctx) {
    std::vector<std::string> windows;
    const auto* options = plan_ctx_->GetOptions();
    boost::split(windows, options->at(vm::LONG_WINDOWS), boost::is_any_of(","));
    for (auto& w : windows) {
        std::vector<std::string> window_info;
        boost::split(window_info, w, boost::is_any_of(":"));
        boost::trim(window_info[0]);
        long_windows_.insert(window_info[0]);
    }
}

bool SplitAggregationOptimized::Transform(PhysicalOpNode* in, PhysicalOpNode** output) {
    *output = in;
    if (vm::kPhysicalOpProject != in->GetOpType()) {
        return false;
    }

    auto project_op = dynamic_cast<vm::PhysicalProjectNode*>(in);
    if (project_op->project_type_ != vm::kAggregation) {
        return false;
    }

    auto project_aggr_op = dynamic_cast<vm::PhysicalAggrerationNode*>(project_op);
    const auto& projects = project_aggr_op->project();
    for (int i = 0; i < projects.size(); i++) {
        const auto* expr = projects.GetExpr(i);
        if (expr->GetExprType() == node::kExprCall) {
            DLOG(INFO) << "expr call = " << expr->GetExprString();
            const auto* call_expr = dynamic_cast<const node::CallExprNode*>(expr);
            const auto* window = call_expr->GetOver();
            if (window == nullptr) continue;

            // skip ANONYMOUS_WINDOW
            if (!window->GetName().empty()) {
                DLOG(INFO) << "func name = " << call_expr->GetFnDef()->GetName()
                           << ", win name = " << window->GetName();

                if (long_windows_.count(window->GetName())) {
                    SplitProjects(project_aggr_op, output);
                    return true;
                }
            }
        }
    }
    return false;
}

bool SplitAggregationOptimized::SplitProjects(vm::PhysicalAggrerationNode* in, PhysicalOpNode** output) {
    const auto& projects = in->project();
    *output = in;

    if (!IsSplitable(in)) {
        return false;
    }

    std::vector<vm::PhysicalAggrerationNode*> aggr_nodes;
    vm::ColumnProjects column_projects;
    column_projects.SetPrimaryFrame(projects.GetPrimaryFrame());
    DLOG(INFO) << "fn_schema size = " << in->project().fn_info().fn_schema()->size();
    for (int i = 0; i < in->project().fn_info().fn_schema()->size(); i++) {
        DLOG(INFO) << "fn_schema " << i << ": " << in->project().fn_info().fn_schema()->Get(i).name();
    }
    for (int i = 0; i < projects.size(); i++) {
        const auto* expr = projects.GetExpr(i);
        column_projects.Add(projects.GetName(i), expr, projects.GetFrame(i));

        if (expr->GetExprType() == node::kExprCall) {
            const auto* call_expr = dynamic_cast<const node::CallExprNode*>(expr);
            const auto* window = call_expr->GetOver();

            if (window) {
                vm::PhysicalAggrerationNode* node = nullptr;
                LOG(WARNING) << "column_projects size = " << column_projects.size() << ", fn_schema = "
                             << column_projects.GetExpr(column_projects.size() - 1)->GetExprString();

                auto status = plan_ctx_->CreateOp<vm::PhysicalAggrerationNode>(
                    &node, in->GetProducer(0), column_projects, in->having_condition_.condition());
                if (!status.isOK()) {
                    LOG(ERROR) << "Fail to create PhysicalAggrerationNode: " << status;
                    return false;
                }

                aggr_nodes.emplace_back(node);
                column_projects.Clear();
                column_projects.SetPrimaryFrame(projects.GetPrimaryFrame());
            }
        }
    }

    if (aggr_nodes.size() < 2) {
        return false;
    }

    vm::PhysicalRequestJoinNode* join = nullptr;
    auto status = plan_ctx_->CreateOp<vm::PhysicalRequestJoinNode>(&join, aggr_nodes[0], aggr_nodes[1],
                                                                   ::hybridse::node::kJoinTypeConcat);
    if (!status.isOK()) {
        LOG(ERROR) << "Fail to create PhysicalRequestJoinNode: " << status;
        return false;
    }

    for (size_t i = 2; i < aggr_nodes.size(); ++i) {
        vm::PhysicalRequestJoinNode* new_join = nullptr;
        status = plan_ctx_->CreateOp<vm::PhysicalRequestJoinNode>(&new_join, join, aggr_nodes[i],
                                                                  ::hybridse::node::kJoinTypeConcat);
        if (!status.isOK()) {
            LOG(ERROR) << "Fail to create PhysicalRequestJoinNode: " << status;
            return false;
        }
        join = new_join;
    }

    *output = join;
    return false;
}

bool SplitAggregationOptimized::IsSplitable(vm::PhysicalAggrerationNode* op) {
    // TODO(zhanghao): currently we only split the aggregation project that depends on a physical table
    return op->project().size() > 1 && op->producers()[0]->GetOpType() == vm::kPhysicalOpRequestUnion;
}

}  // namespace passes
}  // namespace hybridse
