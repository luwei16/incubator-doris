#pragma once

#include "runtime/stream_load/stream_load_executor.h"

namespace doris::cloud {

class CloudStreamLoadExecutor final : public StreamLoadExecutor {
public:
    CloudStreamLoadExecutor(ExecEnv* exec_env);

    ~CloudStreamLoadExecutor() override;

    Status pre_commit_txn(StreamLoadContext* ctx) override;

    Status operate_txn_2pc(StreamLoadContext* ctx) override;

    Status commit_txn(StreamLoadContext* ctx) override;

    void rollback_txn(StreamLoadContext* ctx) override;
};

} // namespace doris::cloud
