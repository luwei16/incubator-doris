#include "cloud/cloud_stream_load_executor.h"

#include "cloud/utils.h"
#include "runtime/stream_load/stream_load_context.h"

namespace doris::cloud {

CloudStreamLoadExecutor::CloudStreamLoadExecutor(ExecEnv* exec_env)
        : StreamLoadExecutor(exec_env) {}

CloudStreamLoadExecutor::~CloudStreamLoadExecutor() = default;

Status CloudStreamLoadExecutor::pre_commit_txn(StreamLoadContext* ctx) {
    return cloud::meta_mgr()->precommit_txn(ctx->db_id, ctx->txn_id);
}

Status CloudStreamLoadExecutor::operate_txn_2pc(StreamLoadContext* ctx) {
    return cloud::meta_mgr()->commit_txn(ctx->db_id, ctx->txn_id, true);
}

Status CloudStreamLoadExecutor::commit_txn(StreamLoadContext* ctx) {
    return cloud::meta_mgr()->commit_txn(ctx->db_id, ctx->txn_id, false);
}

void CloudStreamLoadExecutor::rollback_txn(StreamLoadContext* ctx) {
    WARN_IF_ERROR(cloud::meta_mgr()->abort_txn(ctx->db_id, ctx->txn_id), "Failed to rollback txn");
}

} // namespace doris::cloud
