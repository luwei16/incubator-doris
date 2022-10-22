#include <brpc/controller.h>
#include <fmt/format.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <cstdlib>
#include <limits>
#include <random>

#include "common/config.h"
#include "common/sync_point.h"
#include "common/util.h"
#include "gen_cpp/olap_file.pb.h"
#include "gen_cpp/selectdb_cloud.pb.h"
#include "meta-service/keys.h"
#include "meta-service/mem_txn_kv.h"
#include "meta-service/meta_service.h"
#include "mock_resource_manager.h"

int main(int argc, char** argv) {
    selectdb::config::init(nullptr, true);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

using namespace selectdb;

std::unique_ptr<MetaServiceImpl> get_meta_service() {
    auto txn_kv = std::dynamic_pointer_cast<TxnKv>(std::make_shared<MemTxnKv>());
    [&] { ASSERT_NE(txn_kv.get(), nullptr); }();
    auto rs = std::make_shared<MockResourceManager>(txn_kv);
    auto meta_service = std::make_unique<MetaServiceImpl>(txn_kv, rs);
    return meta_service;
}

TEST(MetaServiceTest, CompactionJobTest) {
    auto meta_service = get_meta_service();
    meta_service->resource_mgr_.reset(); // Do not use resource manager

    std::string instance_id = "tablet_job_test_instance_id";

    [[maybe_unused]] auto sp = SyncPoint::get_instance();
    sp->set_call_back("get_instance_id::pred", [](void* p) { *((bool*)p) = true; });
    sp->set_call_back("get_instance_id", [&](void* p) { *((std::string*)p) = instance_id; });
    sp->enable_processing();

    brpc::Controller cntl;

    int start_compaction_job_cnt = 0;

    int64_t table_id = 1;
    int64_t index_id = 2;
    int64_t partition_id = 3;
    int64_t tablet_id = 4;

    // Start compaction job
    auto start_compaction_job = [&] {
        StartTabletJobRequest req;
        StartTabletJobResponse res;
        std::string job_id = "job_id123";

        auto compaction = req.mutable_job()->add_compaction();
        compaction->set_id(job_id);
        compaction->set_initiator("ip:port");

        meta_service->start_tablet_job(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                       &req, &res, nullptr);
        ASSERT_NE(res.status().msg().find("no valid tablet_id given"), std::string::npos);

        req.mutable_job()->mutable_idx()->set_tablet_id(tablet_id);
        meta_service->start_tablet_job(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                       &req, &res, nullptr);
        if (start_compaction_job_cnt++ < 1) {
            ASSERT_NE(res.status().msg().find("failed to get table id with tablet_id"),
                      std::string::npos);
        }

        auto index_key = meta_tablet_idx_key({instance_id, tablet_id});
        TabletIndexPB idx_pb;
        idx_pb.set_table_id(1);
        idx_pb.set_index_id(2);
        idx_pb.set_partition_id(3);
        idx_pb.set_tablet_id(tablet_id + 1); // error, tablet_id not match
        compaction->set_base_compaction_cnt(10);
        compaction->set_cumulative_compaction_cnt(20);
        std::string idx_val = idx_pb.SerializeAsString();
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv_->create_txn(&txn), 0);
        txn->put(index_key, idx_val);
        std::string stats_key =
                stats_tablet_key({instance_id, table_id, index_id, partition_id, tablet_id});
        TabletStatsPB stats;
        stats.set_base_compaction_cnt(9);
        stats.set_cumulative_compaction_cnt(19);
        txn->put(stats_key, stats.SerializeAsString());
        ASSERT_EQ(txn->commit(), 0);
        meta_service->start_tablet_job(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                       &req, &res, nullptr);
        ASSERT_NE(res.status().msg().find("internal error"), std::string::npos);
        idx_pb.set_tablet_id(tablet_id); // Correct tablet_id
        idx_val = idx_pb.SerializeAsString();
        ASSERT_EQ(meta_service->txn_kv_->create_txn(&txn), 0);
        txn->put(index_key, idx_val);
        ASSERT_EQ(txn->commit(), 0);
        meta_service->start_tablet_job(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                       &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

        ASSERT_EQ(meta_service->txn_kv_->create_txn(&txn), 0);
        auto job_key = job_tablet_key({instance_id, idx_pb.table_id(), idx_pb.index_id(),
                                       idx_pb.partition_id(), idx_pb.tablet_id()});
        std::string job_val;
        ASSERT_EQ(txn->get(job_key, &job_val), 0);
        TabletJobInfoPB job_pb;
        ASSERT_TRUE(job_pb.ParseFromString(job_val));

        LOG(INFO) << proto_to_json(job_pb, true);

        meta_service->start_tablet_job(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                       &req, &res, nullptr);
        ASSERT_NE(res.status().msg().find("already started"), std::string::npos);
    };

    // Finish, this unit test relies on the previous (start_job)
    start_compaction_job();
    {
        FinishTabletJobRequest req;
        FinishTabletJobResponse res;
        std::string job_id = "job_id123";

        auto compaction = req.mutable_job()->add_compaction();
        compaction->set_id(job_id);
        compaction->set_initiator("ip:port");
        meta_service->finish_tablet_job(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                        &req, &res, nullptr);
        ASSERT_NE(res.status().msg().find("no valid tablet_id given"), std::string::npos);

        req.mutable_job()->mutable_idx()->set_table_id(table_id);
        req.mutable_job()->mutable_idx()->set_index_id(index_id);
        req.mutable_job()->mutable_idx()->set_partition_id(partition_id);
        req.mutable_job()->mutable_idx()->set_tablet_id(tablet_id);
        compaction->set_base_compaction_cnt(10);
        compaction->set_cumulative_compaction_cnt(20);
        // Action is not set
        meta_service->finish_tablet_job(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                        &req, &res, nullptr);
        ASSERT_NE(res.status().msg().find("unsupported action"), std::string::npos);

        //======================================================================
        // Test commit
        //======================================================================
        req.set_action(FinishTabletJobRequest::COMMIT);

        // Tablet meta not found, this is unexpected
        meta_service->finish_tablet_job(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                        &req, &res, nullptr);
        ASSERT_NE(res.status().msg().find("failed to get tablet meta"), std::string::npos);

        // Create txn meta, and try again
        auto tablet_meta_key =
                meta_tablet_key({instance_id, table_id, index_id, partition_id, tablet_id});
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv_->create_txn(&txn), 0);
        doris::TabletMetaPB tablet_meta_pb;
        tablet_meta_pb.set_table_id(table_id);
        tablet_meta_pb.set_index_id(index_id);
        tablet_meta_pb.set_partition_id(partition_id);
        tablet_meta_pb.set_tablet_id(tablet_id);
        tablet_meta_pb.set_cumulative_layer_point(50);
        std::string tablet_meta_val = tablet_meta_pb.SerializeAsString();
        ASSERT_FALSE(tablet_meta_val.empty());
        txn->put(tablet_meta_key, tablet_meta_val);
        ASSERT_EQ(txn->commit(), 0);

        meta_service->finish_tablet_job(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                        &req, &res, nullptr);

        // Create create tablet stats, compation job will will update stats
        auto tablet_stats_key =
                stats_tablet_key({instance_id, table_id, index_id, partition_id, tablet_id});
        TabletStatsPB tablet_stats_pb;
        tablet_stats_pb.mutable_idx()->set_table_id(table_id);
        tablet_stats_pb.mutable_idx()->set_index_id(index_id);
        tablet_stats_pb.mutable_idx()->set_partition_id(partition_id);
        tablet_stats_pb.mutable_idx()->set_tablet_id(tablet_id);

        std::mt19937 rng(std::chrono::system_clock::now().time_since_epoch().count());
        std::uniform_int_distribution<int> dist(1, 10000); // Positive numbers
        tablet_stats_pb.set_cumulative_compaction_cnt(dist(rng));
        tablet_stats_pb.set_base_compaction_cnt(dist(rng));
        tablet_stats_pb.set_cumulative_point(tablet_meta_pb.cumulative_layer_point());
        tablet_stats_pb.set_num_rows(dist(rng));
        tablet_stats_pb.set_data_size(dist(rng));
        tablet_stats_pb.set_num_rowsets(dist(rng));
        tablet_stats_pb.set_num_segments(dist(rng));
        tablet_stats_pb.set_last_compaction_time(dist(rng));

        compaction->set_output_cumulative_point(tablet_stats_pb.cumulative_point() + dist(rng));
        compaction->set_num_output_rows(dist(rng));
        compaction->set_num_output_rowsets(dist(rng));
        compaction->set_num_output_segments(dist(rng));
        compaction->set_num_input_rows(dist(rng));
        compaction->set_num_input_rowsets(dist(rng));
        compaction->set_num_input_segments(dist(rng));
        compaction->set_size_input_rowsets(dist(rng));
        compaction->set_size_output_rowsets(dist(rng));
        compaction->set_type((dist(rng) % 10) < 2 ? TabletCompactionJobPB::BASE
                                                  : TabletCompactionJobPB::CUMULATIVE);

        std::string tablet_stats_val = tablet_stats_pb.SerializeAsString();
        ASSERT_FALSE(tablet_stats_val.empty());
        ASSERT_EQ(meta_service->txn_kv_->create_txn(&txn), 0);
        txn->put(tablet_stats_key, tablet_stats_val);
        ASSERT_EQ(txn->commit(), 0);

        // Input rowset not valid
        meta_service->finish_tablet_job(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                        &req, &res, nullptr);
        ASSERT_NE(res.status().msg().find("invalid input"), std::string::npos);

        // Provide input and output rowset info
        int64_t input_version_start = dist(rng);
        int64_t input_version_end = input_version_start + 100;
        compaction->add_input_versions(input_version_start);
        compaction->add_input_versions(input_version_end);
        compaction->add_output_versions(input_version_end);
        compaction->add_output_rowset_ids("output rowset id");

        // Input rowsets must exist, and more than 0
        // Check number input rowsets
        sp->set_call_back("process_compaction_job::loop_input_done", [](void* c) {
            int& num_input_rowsets = *(int*)c;
            ASSERT_EQ(num_input_rowsets, 0); // zero existed rowsets
        });
        sp->set_call_back("process_compaction_job::too_few_rowsets", [](void* c) {
            auto& need_commit = *(bool*)c;
            ASSERT_EQ(need_commit, true);
            need_commit = false; // Donot remove tablet job in order to continue test
        });

        meta_service->finish_tablet_job(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                        &req, &res, nullptr);
        ASSERT_NE(res.status().msg().find("too few input rowsets"), std::string::npos);

        // Provide input rowset KVs, boundary test, 5 input rowsets
        ASSERT_EQ(meta_service->txn_kv_->create_txn(&txn), 0);
        // clang-format off
        std::vector<std::string> input_rowset_keys = {
                meta_rowset_key({instance_id, tablet_id, input_version_start - 1}),
                meta_rowset_key({instance_id, tablet_id, input_version_start}),
                meta_rowset_key({instance_id, tablet_id, input_version_start + 1}),
                meta_rowset_key({instance_id, tablet_id, (input_version_start + input_version_end) / 2}),
                meta_rowset_key({instance_id, tablet_id, input_version_end - 1}),
                meta_rowset_key({instance_id, tablet_id, input_version_end}),
                meta_rowset_key({instance_id, tablet_id, input_version_end + 1}),
        };
        // clang-format on
        std::vector<std::unique_ptr<std::string>> input_rowset_vals;
        for (auto& i : input_rowset_keys) {
            doris::RowsetMetaPB rs_pb;
            rs_pb.set_rowset_id(0);
            rs_pb.set_rowset_id_v2(hex(i));
            input_rowset_vals.emplace_back(new std::string(rs_pb.SerializeAsString()));
            txn->put(i, *input_rowset_vals.back());
        }
        ASSERT_EQ(txn->commit(), 0);

        // Check number input rowsets
        sp->set_call_back("process_compaction_job::loop_input_done", [](void* c) {
            int& num_input_rowsets = *(int*)c;
            ASSERT_EQ(num_input_rowsets, 5);
        });
        // No tmp rowset key (output rowset)
        meta_service->finish_tablet_job(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                        &req, &res, nullptr);
        ASSERT_NE(res.status().msg().find("invalid txn_id"), std::string::npos);

        int64_t txn_id = dist(rng);
        compaction->add_txn_id(txn_id);

        meta_service->finish_tablet_job(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                        &req, &res, nullptr);
        ASSERT_NE(res.status().msg().find("failed to get tmp rowset key"), std::string::npos);

        // Provide invalid output rowset meta
        auto tmp_rowset_key = meta_rowset_tmp_key({instance_id, txn_id, tablet_id});
        doris::RowsetMetaPB tmp_rs_pb;
        tmp_rs_pb.set_rowset_id(0);
        auto tmp_rowset_val = tmp_rs_pb.SerializeAsString();
        ASSERT_EQ(meta_service->txn_kv_->create_txn(&txn), 0);
        txn->put(tmp_rowset_key, tmp_rowset_val);
        ASSERT_EQ(txn->commit(), 0);

        meta_service->finish_tablet_job(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                        &req, &res, nullptr);
        ASSERT_NE(res.status().msg().find("invalid txn_id in output tmp rowset meta"),
                  std::string::npos);

        // Provide txn_id in output rowset meta
        tmp_rs_pb.set_txn_id(10086);
        tmp_rowset_val = tmp_rs_pb.SerializeAsString();
        ASSERT_EQ(meta_service->txn_kv_->create_txn(&txn), 0);
        txn->put(tmp_rowset_key, tmp_rowset_val);
        ASSERT_EQ(txn->commit(), 0);

        meta_service->finish_tablet_job(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                        &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

        //=====================================================================
        // All branch tests done, we are done commit a compaction job
        //=====================================================================
        ASSERT_EQ(meta_service->txn_kv_->create_txn(&txn), 0);
        tablet_stats_val.clear();
        ASSERT_EQ(txn->get(tablet_stats_key, &tablet_stats_val), 0);
        TabletStatsPB stats;
        ASSERT_TRUE(stats.ParseFromString(tablet_stats_val));

        // clang-format off
        EXPECT_EQ(stats.base_compaction_cnt()      , tablet_stats_pb.base_compaction_cnt() + (req.job().compaction(0).type() == TabletCompactionJobPB::BASE));
        EXPECT_EQ(stats.cumulative_compaction_cnt(), tablet_stats_pb.cumulative_compaction_cnt() + (req.job().compaction(0).type() == TabletCompactionJobPB::CUMULATIVE));
        EXPECT_EQ(stats.cumulative_point()         , req.job().compaction(0).output_cumulative_point());
        EXPECT_EQ(stats.num_rows()                 , tablet_stats_pb.num_rows() + (req.job().compaction(0).num_output_rows() - req.job().compaction(0).num_input_rows()));
        EXPECT_EQ(stats.data_size()                , tablet_stats_pb.data_size() + (req.job().compaction(0).size_output_rowsets() - req.job().compaction(0).size_input_rowsets()));
        EXPECT_EQ(stats.num_rowsets()              , tablet_stats_pb.num_rowsets() + (req.job().compaction(0).num_output_rowsets() - req.job().compaction(0).num_input_rowsets()));
        EXPECT_EQ(stats.num_segments()             , tablet_stats_pb.num_segments() + (req.job().compaction(0).num_output_segments() - req.job().compaction(0).num_input_segments()));
        // EXPECT_EQ(stats.last_compaction_time     (), now);
        // clang-format on

        // Check job removed, tablet meta updated
        auto job_key = job_tablet_key({instance_id, table_id, index_id, partition_id, tablet_id});
        std::string job_val;
        ASSERT_EQ(txn->get(job_key, &job_val), 0);
        TabletJobInfoPB job_pb;
        ASSERT_TRUE(job_pb.ParseFromString(job_val));
        ASSERT_TRUE(job_pb.compaction().empty());
        tablet_meta_val.clear();
        ASSERT_EQ(txn->get(tablet_meta_key, &tablet_meta_val), 0);
        ASSERT_TRUE(tablet_meta_pb.ParseFromString(tablet_meta_val));
        LOG(INFO) << tablet_meta_pb.DebugString();
        ASSERT_EQ(tablet_meta_pb.cumulative_layer_point(),
                  req.job().compaction(0).output_cumulative_point());
        ASSERT_EQ(tablet_meta_pb.cumulative_layer_point(), stats.cumulative_point());

        // Check tmp rowset removed
        ASSERT_EQ(txn->get(tmp_rowset_key, &tmp_rowset_val), 1);
        // Check input rowsets removed, the largest version remains
        for (int i = 1; i < input_rowset_keys.size() - 2; ++i) {
            std::string val;
            EXPECT_EQ(txn->get(input_rowset_keys[i], &val), 1) << hex(input_rowset_keys[i]);
        }
        // Check recycle rowsets added
        for (int i = 1; i < input_rowset_vals.size() - 1; ++i) {
            doris::RowsetMetaPB rs;
            ASSERT_TRUE(rs.ParseFromString(*input_rowset_vals[i]));
            auto key = recycle_rowset_key({instance_id, tablet_id, rs.rowset_id_v2()});
            std::string val;
            EXPECT_EQ(txn->get(key, &val), 0) << hex(key);
        }
        // Check output rowset added
        auto rowset_key = meta_rowset_key({instance_id, tablet_id, input_version_end});
        std::string rowset_val;
        EXPECT_EQ(txn->get(rowset_key, &rowset_val), 0) << hex(rowset_key);
    }

    // Test abort compaction
    start_compaction_job();
    {
        FinishTabletJobRequest req;
        FinishTabletJobResponse res;
        std::string job_id = "job_id123";

        auto compaction = req.mutable_job()->add_compaction();
        compaction->set_id(job_id);
        compaction->set_initiator("ip:port");
        req.mutable_job()->mutable_idx()->set_tablet_id(tablet_id);
        req.set_action(FinishTabletJobRequest::ABORT);
        meta_service->finish_tablet_job(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                        &req, &res, nullptr);

        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv_->create_txn(&txn), 0);
        auto job_key = job_tablet_key({instance_id, table_id, index_id, partition_id, tablet_id});
        std::string job_val;
        ASSERT_EQ(txn->get(job_key, &job_val), 0);
        TabletJobInfoPB job_pb;
        ASSERT_TRUE(job_pb.ParseFromString(job_val));
        ASSERT_TRUE(job_pb.compaction().empty());
    }
}

TEST(MetaServiceTest, SchemaChangeJobTest) {
    auto meta_service = get_meta_service();
    meta_service->resource_mgr_.reset(); // Do not use resource manager

    std::string instance_id = "tablet_job_test_instance_id";

    [[maybe_unused]] auto sp = SyncPoint::get_instance();
    sp->set_call_back("get_instance_id::pred", [](void* p) { *((bool*)p) = true; });
    sp->set_call_back("get_instance_id", [&](void* p) { *((std::string*)p) = instance_id; });
    sp->enable_processing();

    brpc::Controller cntl;

    int64_t table_id = 1;
    int64_t index_id = 2;
    int64_t partition_id = 3;
    int64_t tablet_id = 4;
    int64_t new_tablet_id = 14;

    auto create_new_tablet = [&] {
        CreateTabletRequest req;
        MetaServiceGenericResponse res;
        auto tablet_meta_pb = req.mutable_tablet_meta();
        tablet_meta_pb->set_table_id(table_id);
        tablet_meta_pb->set_index_id(index_id);
        tablet_meta_pb->set_partition_id(partition_id);
        tablet_meta_pb->set_tablet_id(new_tablet_id);
        tablet_meta_pb->set_tablet_state(doris::TabletStatePB::PB_NOTREADY);
        auto rs_meta_pb = tablet_meta_pb->add_rs_metas();
        rs_meta_pb->set_tablet_id(new_tablet_id);
        rs_meta_pb->set_rowset_id(0);
        // rs_meta_pb->set_rowset_id_v2("xxx");
        rs_meta_pb->set_start_version(0);
        rs_meta_pb->set_end_version(1);
        meta_service->create_tablet(&cntl, &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    };

    auto start_schema_change_job = [&] {
        StartTabletJobRequest req;
        StartTabletJobResponse res;
        std::string job_id = "job_id123";

        req.mutable_job()->mutable_idx()->set_table_id(table_id);
        req.mutable_job()->mutable_idx()->set_index_id(index_id);
        req.mutable_job()->mutable_idx()->set_partition_id(partition_id);
        req.mutable_job()->mutable_idx()->set_tablet_id(tablet_id);
        auto sc = req.mutable_job()->mutable_schema_change();
        sc->set_id(job_id);
        sc->set_initiator("ip1:port");
        sc->mutable_new_tablet_idx()->set_table_id(table_id);
        sc->mutable_new_tablet_idx()->set_index_id(index_id);
        sc->mutable_new_tablet_idx()->set_partition_id(partition_id);
        sc->mutable_new_tablet_idx()->set_tablet_id(new_tablet_id);
        meta_service->start_tablet_job(&cntl, &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv_->create_txn(&txn), 0);
        auto job_key = job_tablet_key({instance_id, table_id, index_id, partition_id, tablet_id});
        std::string job_val;
        ASSERT_EQ(txn->get(job_key, &job_val), 0);
        TabletJobInfoPB job_pb;
        ASSERT_TRUE(job_pb.ParseFromString(job_val));
        ASSERT_TRUE(job_pb.has_schema_change());
    };

    // commit schema_change job with alter_version == 1
    {
        create_new_tablet();
        start_schema_change_job();

        FinishTabletJobRequest req;
        FinishTabletJobResponse res;
        std::string job_id = "job_id123";

        req.set_action(FinishTabletJobRequest::COMMIT);
        req.mutable_job()->mutable_idx()->set_table_id(table_id);
        req.mutable_job()->mutable_idx()->set_index_id(index_id);
        req.mutable_job()->mutable_idx()->set_partition_id(partition_id);
        req.mutable_job()->mutable_idx()->set_tablet_id(tablet_id);
        auto sc = req.mutable_job()->mutable_schema_change();
        sc->mutable_new_tablet_idx()->set_table_id(table_id);
        sc->mutable_new_tablet_idx()->set_index_id(index_id);
        sc->mutable_new_tablet_idx()->set_partition_id(partition_id);
        sc->mutable_new_tablet_idx()->set_tablet_id(new_tablet_id);
        sc->set_alter_version(1);

        sc->set_id("job_id1234");
        sc->set_initiator("ip1:port");
        meta_service->finish_tablet_job(&cntl, &req, &res, nullptr);
        ASSERT_NE(res.status().code(), MetaServiceCode::OK);
        ASSERT_NE(res.status().msg().find("unmatched job id or initiator"), std::string::npos);

        sc->set_id(job_id);
        sc->set_initiator("ip2:port");
        meta_service->finish_tablet_job(&cntl, &req, &res, nullptr);
        ASSERT_NE(res.status().code(), MetaServiceCode::OK);
        ASSERT_NE(res.status().msg().find("unmatched job id or initiator"), std::string::npos);

        sc->set_id(job_id);
        sc->set_initiator("ip1:port");
        meta_service->finish_tablet_job(&cntl, &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        meta_service->finish_tablet_job(&cntl, &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::JOB_ALREADY_SUCCESS);
    }

    // commit schema_change job with txn_ids
    {
        new_tablet_id = 24;
        create_new_tablet();
        start_schema_change_job();

        FinishTabletJobRequest req;
        FinishTabletJobResponse res;
        std::string job_id = "job_id123";

        req.set_action(FinishTabletJobRequest::COMMIT);
        req.mutable_job()->mutable_idx()->set_table_id(table_id);
        req.mutable_job()->mutable_idx()->set_index_id(index_id);
        req.mutable_job()->mutable_idx()->set_partition_id(partition_id);
        req.mutable_job()->mutable_idx()->set_tablet_id(tablet_id);
        auto sc = req.mutable_job()->mutable_schema_change();
        sc->mutable_new_tablet_idx()->set_table_id(table_id);
        sc->mutable_new_tablet_idx()->set_index_id(index_id);
        sc->mutable_new_tablet_idx()->set_partition_id(partition_id);
        sc->mutable_new_tablet_idx()->set_tablet_id(new_tablet_id);
        sc->set_id(job_id);
        sc->set_initiator("ip1:port");

        // provide converted tmp rowsets
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv_->create_txn(&txn), 0);
        std::vector<std::string> tmp_rs_keys;
        std::vector<std::string> tmp_rs_vals;
        for (int64_t v = 2; v <= 6; ++v) {
            auto tmp_rs_key = meta_rowset_tmp_key({instance_id, /* txn_id */ v, new_tablet_id});
            doris::RowsetMetaPB rs_pb;
            rs_pb.set_tablet_id(new_tablet_id);
            rs_pb.set_rowset_id(0);
            rs_pb.set_rowset_id_v2(hex(tmp_rs_key));
            rs_pb.set_start_version(v);
            rs_pb.set_end_version(v);
            auto tmp_rs_val = rs_pb.SerializeAsString();
            txn->put(tmp_rs_key, tmp_rs_val);
            tmp_rs_keys.push_back(std::move(tmp_rs_key));
            tmp_rs_vals.push_back(std::move(tmp_rs_val));

            sc->add_txn_ids(v);
            sc->add_output_versions(v);
        }
        ASSERT_EQ(txn->commit(), 0);

        sc->set_num_output_rows(1000);
        sc->set_num_output_rowsets(5);
        sc->set_num_output_segments(10);
        sc->set_size_output_rowsets(10000);
        sc->set_alter_version(6);

        meta_service->finish_tablet_job(&cntl, &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

        ASSERT_EQ(meta_service->txn_kv_->create_txn(&txn), 0);
        // check tablet state
        auto tablet_key =
                meta_tablet_key({instance_id, table_id, index_id, partition_id, new_tablet_id});
        std::string tablet_val;
        ASSERT_EQ(txn->get(tablet_key, &tablet_val), 0);
        doris::TabletMetaPB tablet_pb;
        ASSERT_TRUE(tablet_pb.ParseFromString(tablet_val));
        ASSERT_EQ(tablet_pb.tablet_state(), doris::TabletStatePB::PB_RUNNING);

        // check tablet stats
        auto tablet_stat_key =
                stats_tablet_key({instance_id, table_id, index_id, partition_id, new_tablet_id});
        std::string tablet_stat_val;
        ASSERT_EQ(txn->get(tablet_stat_key, &tablet_stat_val), 0);
        TabletStatsPB tablet_stat;
        ASSERT_TRUE(tablet_stat.ParseFromString(tablet_stat_val));
        ASSERT_EQ(tablet_stat.num_rows(), sc->num_output_rows());
        ASSERT_EQ(tablet_stat.num_rowsets(), sc->num_output_rowsets() + 1);
        ASSERT_EQ(tablet_stat.num_segments(), sc->num_output_segments());
        ASSERT_EQ(tablet_stat.data_size(), sc->size_output_rowsets());

        // check visible rowsets
        std::unique_ptr<RangeGetIterator> it;
        auto rs_start = meta_rowset_key({instance_id, new_tablet_id, 0});
        auto rs_end =
                meta_rowset_key({instance_id, new_tablet_id, std::numeric_limits<int64_t>::max()});
        ASSERT_EQ(txn->get(rs_start, rs_end, &it), 0);
        ASSERT_EQ(it->size(), 6);
        auto [k, v] = it->next();
        doris::RowsetMetaPB rs_pb;
        ASSERT_TRUE(rs_pb.ParseFromArray(v.data(), v.size()));
        ASSERT_EQ(rs_pb.start_version(), 0);
        ASSERT_EQ(rs_pb.end_version(), 1);
        for (int64_t ver = 2; ver <= 6; ++ver) {
            auto [k, v] = it->next();
            ASSERT_TRUE(rs_pb.ParseFromArray(v.data(), v.size()));
            ASSERT_EQ(rs_pb.start_version(), ver);
            ASSERT_EQ(rs_pb.end_version(), ver);
        }
    }

    // commit schema_change job with rowsets which overlapped with visible rowsets
    {
        new_tablet_id = 34;
        create_new_tablet();
        start_schema_change_job();

        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv_->create_txn(&txn), 0);

        // provide visible rowsets
        std::vector<std::string> rs_keys;
        std::vector<std::string> rs_vals;
        for (int64_t v = 11; v <= 15; ++v) {
            auto rs_key = meta_rowset_key({instance_id, new_tablet_id, v});
            doris::RowsetMetaPB rs_pb;
            rs_pb.set_tablet_id(new_tablet_id);
            rs_pb.set_rowset_id(0);
            rs_pb.set_rowset_id_v2(hex(rs_key));
            rs_pb.set_start_version(v);
            rs_pb.set_end_version(v);
            rs_pb.set_num_rows(100);
            rs_pb.set_data_disk_size(1000);
            rs_pb.set_num_segments(2);
            auto rs_val = rs_pb.SerializeAsString();
            txn->put(rs_key, rs_val);
            rs_keys.push_back(std::move(rs_key));
            rs_vals.push_back(std::move(rs_val));
        }
        // update tablet stat
        auto tablet_stat_key =
                stats_tablet_key({instance_id, table_id, index_id, partition_id, new_tablet_id});
        std::string tablet_stat_val;
        ASSERT_EQ(txn->get(tablet_stat_key, &tablet_stat_val), 0);
        TabletStatsPB tablet_stat;
        ASSERT_TRUE(tablet_stat.ParseFromString(tablet_stat_val));
        tablet_stat.set_num_rows(tablet_stat.num_rows() + 500);
        tablet_stat.set_data_size(tablet_stat.data_size() + 5000);
        tablet_stat.set_num_segments(tablet_stat.num_segments() + 10);
        tablet_stat.set_num_rowsets(tablet_stat.num_rowsets() + 5);
        ASSERT_TRUE(tablet_stat.SerializeToString(&tablet_stat_val));
        txn->put(tablet_stat_key, tablet_stat_val);

        ASSERT_EQ(txn->commit(), 0);

        FinishTabletJobRequest req;
        FinishTabletJobResponse res;
        std::string job_id = "job_id123";

        req.set_action(FinishTabletJobRequest::COMMIT);
        req.mutable_job()->mutable_idx()->set_table_id(table_id);
        req.mutable_job()->mutable_idx()->set_index_id(index_id);
        req.mutable_job()->mutable_idx()->set_partition_id(partition_id);
        req.mutable_job()->mutable_idx()->set_tablet_id(tablet_id);
        auto sc = req.mutable_job()->mutable_schema_change();
        sc->mutable_new_tablet_idx()->set_table_id(table_id);
        sc->mutable_new_tablet_idx()->set_index_id(index_id);
        sc->mutable_new_tablet_idx()->set_partition_id(partition_id);
        sc->mutable_new_tablet_idx()->set_tablet_id(new_tablet_id);
        sc->set_id(job_id);
        sc->set_initiator("ip1:port");

        // provide converted tmp rowsets
        ASSERT_EQ(meta_service->txn_kv_->create_txn(&txn), 0);
        std::vector<std::string> tmp_rs_keys;
        std::vector<std::string> tmp_rs_vals;
        std::vector<std::pair<int64_t, int64_t>> versions = {{2, 8}, {9, 12}, {13, 13}};
        for (auto [start_v, end_v] : versions) {
            auto tmp_rs_key = meta_rowset_tmp_key({instance_id, /* txn_id */ end_v, new_tablet_id});
            doris::RowsetMetaPB rs_pb;
            rs_pb.set_tablet_id(new_tablet_id);
            rs_pb.set_rowset_id(0);
            rs_pb.set_rowset_id_v2(hex(tmp_rs_key));
            rs_pb.set_start_version(start_v);
            rs_pb.set_end_version(end_v);
            auto tmp_rs_val = rs_pb.SerializeAsString();
            txn->put(tmp_rs_key, tmp_rs_val);
            tmp_rs_keys.push_back(std::move(tmp_rs_key));
            tmp_rs_vals.push_back(std::move(tmp_rs_val));

            sc->add_txn_ids(end_v);
            sc->add_output_versions(end_v);
        }
        ASSERT_EQ(txn->commit(), 0);

        sc->set_num_output_rows(1000);
        sc->set_num_output_rowsets(3);
        sc->set_num_output_segments(10);
        sc->set_size_output_rowsets(10000);
        sc->set_alter_version(13);

        meta_service->finish_tablet_job(&cntl, &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

        ASSERT_EQ(meta_service->txn_kv_->create_txn(&txn), 0);
        // check tablet state
        auto tablet_key =
                meta_tablet_key({instance_id, table_id, index_id, partition_id, new_tablet_id});
        std::string tablet_val;
        ASSERT_EQ(txn->get(tablet_key, &tablet_val), 0);
        doris::TabletMetaPB tablet_pb;
        ASSERT_TRUE(tablet_pb.ParseFromString(tablet_val));
        ASSERT_EQ(tablet_pb.tablet_state(), doris::TabletStatePB::PB_RUNNING);

        // check tablet stats
        ASSERT_EQ(txn->get(tablet_stat_key, &tablet_stat_val), 0);
        ASSERT_TRUE(tablet_stat.ParseFromString(tablet_stat_val));
        // overwrite [11-13], retain [14-15]
        EXPECT_EQ(tablet_stat.num_rows(), sc->num_output_rows() + 200);
        EXPECT_EQ(tablet_stat.num_rowsets(), sc->num_output_rowsets() + 1 + 2);
        EXPECT_EQ(tablet_stat.num_segments(), sc->num_output_segments() + 4);
        EXPECT_EQ(tablet_stat.data_size(), sc->size_output_rowsets() + 2000);

        // check visible rowsets
        std::unique_ptr<RangeGetIterator> it;
        auto rs_start = meta_rowset_key({instance_id, new_tablet_id, 0});
        auto rs_end =
                meta_rowset_key({instance_id, new_tablet_id, std::numeric_limits<int64_t>::max()});
        ASSERT_EQ(txn->get(rs_start, rs_end, &it), 0);
        // [0-1][2-8][9-12][13-13][14-14][15-15]
        EXPECT_EQ(it->size(), 6);
        auto [k, v] = it->next();
        doris::RowsetMetaPB rs_pb;
        ASSERT_TRUE(rs_pb.ParseFromArray(v.data(), v.size()));
        EXPECT_EQ(rs_pb.start_version(), 0);
        EXPECT_EQ(rs_pb.end_version(), 1);
        for (size_t i = 0; i < versions.size(); ++i) {
            auto [k, v] = it->next();
            ASSERT_TRUE(rs_pb.ParseFromArray(v.data(), v.size()));
            EXPECT_EQ(rs_pb.start_version(), versions[i].first);
            EXPECT_EQ(rs_pb.end_version(), versions[i].second);
            EXPECT_EQ(rs_pb.rowset_id_v2(), hex(tmp_rs_keys[i]));
        }
        for (int64_t ver = 14; ver <= 15; ++ver) {
            auto [k, v] = it->next();
            ASSERT_TRUE(rs_pb.ParseFromArray(v.data(), v.size()));
            EXPECT_EQ(rs_pb.start_version(), ver);
            EXPECT_EQ(rs_pb.end_version(), ver);
        }

        // check recycled rowsets
        auto recycl_rs_start = recycle_rowset_key({instance_id, new_tablet_id, ""});
        auto recycl_rs_end = recycle_rowset_key({instance_id, new_tablet_id, "\xff"});
        ASSERT_EQ(txn->get(recycl_rs_start, recycl_rs_end, &it), 0);
        // [11-11], [12-12], old[13-13]
        EXPECT_EQ(it->size(), 3);
    }
}

TEST(MetaServiceTest, RetrySchemaChangeJobTest) {
    auto meta_service = get_meta_service();
    meta_service->resource_mgr_.reset(); // Do not use resource manager

    std::string instance_id = "tablet_job_test_instance_id";

    [[maybe_unused]] auto sp = SyncPoint::get_instance();
    sp->set_call_back("get_instance_id::pred", [](void* p) { *((bool*)p) = true; });
    sp->set_call_back("get_instance_id", [&](void* p) { *((std::string*)p) = instance_id; });
    sp->enable_processing();

    brpc::Controller cntl;

    int64_t table_id = 1;
    int64_t index_id = 2;
    int64_t partition_id = 3;
    int64_t tablet_id = 4;
    int64_t new_tablet_id = 14;

    auto create_new_tablet = [&] {
        CreateTabletRequest req;
        MetaServiceGenericResponse res;
        auto tablet_meta_pb = req.mutable_tablet_meta();
        tablet_meta_pb->set_table_id(table_id);
        tablet_meta_pb->set_index_id(index_id);
        tablet_meta_pb->set_partition_id(partition_id);
        tablet_meta_pb->set_tablet_id(new_tablet_id);
        tablet_meta_pb->set_tablet_state(doris::TabletStatePB::PB_NOTREADY);
        auto rs_meta_pb = tablet_meta_pb->add_rs_metas();
        rs_meta_pb->set_tablet_id(new_tablet_id);
        rs_meta_pb->set_rowset_id(0);
        // rs_meta_pb->set_rowset_id_v2("xxx");
        rs_meta_pb->set_start_version(0);
        rs_meta_pb->set_end_version(1);
        meta_service->create_tablet(&cntl, &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    };

    std::string job_id = "job1";
    std::string initiator = "BE1";
    auto start_schema_change_job = [&]() {
        StartTabletJobRequest req;
        StartTabletJobResponse res;

        req.mutable_job()->mutable_idx()->set_table_id(table_id);
        req.mutable_job()->mutable_idx()->set_index_id(index_id);
        req.mutable_job()->mutable_idx()->set_partition_id(partition_id);
        req.mutable_job()->mutable_idx()->set_tablet_id(tablet_id);
        auto sc = req.mutable_job()->mutable_schema_change();
        sc->set_id(job_id);
        sc->set_initiator(initiator);
        sc->mutable_new_tablet_idx()->set_table_id(table_id);
        sc->mutable_new_tablet_idx()->set_index_id(index_id);
        sc->mutable_new_tablet_idx()->set_partition_id(partition_id);
        sc->mutable_new_tablet_idx()->set_tablet_id(new_tablet_id);
        meta_service->start_tablet_job(&cntl, &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv_->create_txn(&txn), 0);
        auto job_key = job_tablet_key({instance_id, table_id, index_id, partition_id, tablet_id});
        std::string job_val;
        ASSERT_EQ(txn->get(job_key, &job_val), 0);
        TabletJobInfoPB job_pb;
        ASSERT_TRUE(job_pb.ParseFromString(job_val));
        ASSERT_TRUE(job_pb.has_schema_change());
        ASSERT_EQ(job_pb.schema_change().id(), job_id);
    };

    // start "job1" on BE1
    create_new_tablet();
    start_schema_change_job();

    new_tablet_id = 24;
    create_new_tablet();

    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(meta_service->txn_kv_->create_txn(&txn), 0);

    // provide visible rowsets
    std::vector<std::string> rs_keys;
    std::vector<std::string> rs_vals;
    for (int64_t v = 11; v <= 15; ++v) {
        auto rs_key = meta_rowset_key({instance_id, new_tablet_id, v});
        doris::RowsetMetaPB rs_pb;
        rs_pb.set_tablet_id(new_tablet_id);
        rs_pb.set_rowset_id(0);
        rs_pb.set_rowset_id_v2(hex(rs_key));
        rs_pb.set_start_version(v);
        rs_pb.set_end_version(v);
        rs_pb.set_num_rows(100);
        rs_pb.set_data_disk_size(1000);
        rs_pb.set_num_segments(2);
        auto rs_val = rs_pb.SerializeAsString();
        txn->put(rs_key, rs_val);
        rs_keys.push_back(std::move(rs_key));
        rs_vals.push_back(std::move(rs_val));
    }
    // update tablet stat
    auto tablet_stat_key =
            stats_tablet_key({instance_id, table_id, index_id, partition_id, new_tablet_id});
    std::string tablet_stat_val;
    ASSERT_EQ(txn->get(tablet_stat_key, &tablet_stat_val), 0);
    TabletStatsPB tablet_stat;
    ASSERT_TRUE(tablet_stat.ParseFromString(tablet_stat_val));
    tablet_stat.set_num_rows(tablet_stat.num_rows() + 500);
    tablet_stat.set_data_size(tablet_stat.data_size() + 5000);
    tablet_stat.set_num_segments(tablet_stat.num_segments() + 10);
    tablet_stat.set_num_rowsets(tablet_stat.num_rowsets() + 5);
    ASSERT_TRUE(tablet_stat.SerializeToString(&tablet_stat_val));
    txn->put(tablet_stat_key, tablet_stat_val);

    ASSERT_EQ(txn->commit(), 0);

    // FE canceled "job1" and starts "job2" on BE1, should preempt previous "job1"
    job_id = "job2";
    start_schema_change_job();

    // retry "job2" on BE1
    start_schema_change_job();

    auto txn_id_gen = [](int64_t start_v, int64_t end_v) { return start_v << 32 | end_v; };

    // BE1 output_versions=[2-8][9-9][10-10][11-11]
    std::vector<std::pair<int64_t, int64_t>> be1_versions = {{2, 8}, {9, 9}, {10, 10}, {11, 11}};
    for (auto [start_v, end_v] : be1_versions) {
        CreateRowsetRequest req;
        CreateRowsetResponse res;
        req.mutable_rowset_meta()->set_rowset_id(0);
        int64_t txn_id = txn_id_gen(start_v, end_v);
        req.mutable_rowset_meta()->set_tablet_id(new_tablet_id);
        req.mutable_rowset_meta()->set_rowset_id_v2(fmt::format("BE1{}", txn_id));
        req.mutable_rowset_meta()->set_start_version(start_v);
        req.mutable_rowset_meta()->set_end_version(end_v);
        req.mutable_rowset_meta()->set_txn_id(txn_id);
        req.set_temporary(true);
        meta_service->prepare_rowset(&cntl, &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        meta_service->commit_rowset(&cntl, &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    // FE thinks BE1 is not alive and retries "job2" on BE2, should preempt "job2" created by BE1
    initiator = "BE2";
    start_schema_change_job();

    // BE2 output_versions=[2-8][9-12][13-13]
    std::vector<std::pair<int64_t, int64_t>> be2_versions = {{2, 8}, {9, 12}, {13, 13}};
    for (auto [start_v, end_v] : be2_versions) {
        CreateRowsetRequest req;
        CreateRowsetResponse res;
        req.mutable_rowset_meta()->set_tablet_id(new_tablet_id);
        req.mutable_rowset_meta()->set_rowset_id(0);
        int64_t txn_id = txn_id_gen(start_v, end_v);
        req.mutable_rowset_meta()->set_rowset_id_v2(fmt::format("BE2{}", txn_id));
        req.mutable_rowset_meta()->set_start_version(start_v);
        req.mutable_rowset_meta()->set_end_version(end_v);
        req.mutable_rowset_meta()->set_txn_id(txn_id);
        req.set_temporary(true);
        meta_service->prepare_rowset(&cntl, &req, &res, nullptr);
        if (start_v == 2) { // [2-8] has committed by BE1
            ASSERT_EQ(res.status().code(), MetaServiceCode::ALREADY_EXISTED);
        } else {
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            meta_service->commit_rowset(&cntl, &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        }
    }

    // BE1 commit job, but check initiator failed
    {
        FinishTabletJobRequest req;
        FinishTabletJobResponse res;

        req.set_action(FinishTabletJobRequest::COMMIT);
        req.mutable_job()->mutable_idx()->set_table_id(table_id);
        req.mutable_job()->mutable_idx()->set_index_id(index_id);
        req.mutable_job()->mutable_idx()->set_partition_id(partition_id);
        req.mutable_job()->mutable_idx()->set_tablet_id(tablet_id);
        auto sc = req.mutable_job()->mutable_schema_change();
        sc->mutable_new_tablet_idx()->set_table_id(table_id);
        sc->mutable_new_tablet_idx()->set_index_id(index_id);
        sc->mutable_new_tablet_idx()->set_partition_id(partition_id);
        sc->mutable_new_tablet_idx()->set_tablet_id(new_tablet_id);
        sc->set_id(job_id);
        sc->set_initiator("BE1");
        for (auto [start_v, end_v] : be1_versions) {
            sc->add_txn_ids(txn_id_gen(start_v, end_v));
            sc->add_output_versions(end_v);
        }
        sc->set_num_output_rows(1000);
        sc->set_num_output_rowsets(4);
        sc->set_num_output_segments(10);
        sc->set_size_output_rowsets(10000);
        sc->set_alter_version(11);
        meta_service->finish_tablet_job(&cntl, &req, &res, nullptr);
        ASSERT_NE(res.status().code(), MetaServiceCode::OK);
        ASSERT_NE(res.status().msg().find("unmatched job id or initiator"), std::string::npos);
    }

    // BE2 commit job
    {
        FinishTabletJobRequest req;
        FinishTabletJobResponse res;

        req.set_action(FinishTabletJobRequest::COMMIT);
        req.mutable_job()->mutable_idx()->set_table_id(table_id);
        req.mutable_job()->mutable_idx()->set_index_id(index_id);
        req.mutable_job()->mutable_idx()->set_partition_id(partition_id);
        req.mutable_job()->mutable_idx()->set_tablet_id(tablet_id);
        auto sc = req.mutable_job()->mutable_schema_change();
        sc->mutable_new_tablet_idx()->set_table_id(table_id);
        sc->mutable_new_tablet_idx()->set_index_id(index_id);
        sc->mutable_new_tablet_idx()->set_partition_id(partition_id);
        sc->mutable_new_tablet_idx()->set_tablet_id(new_tablet_id);
        sc->set_id(job_id);
        sc->set_initiator("BE2");
        for (auto [start_v, end_v] : be2_versions) {
            sc->add_txn_ids(txn_id_gen(start_v, end_v));
            sc->add_output_versions(end_v);
        }
        sc->set_num_output_rows(1000);
        sc->set_num_output_rowsets(3);
        sc->set_num_output_segments(10);
        sc->set_size_output_rowsets(10000);
        sc->set_alter_version(13);
        meta_service->finish_tablet_job(&cntl, &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

        ASSERT_EQ(meta_service->txn_kv_->create_txn(&txn), 0);
        // check tablet state
        auto tablet_key =
                meta_tablet_key({instance_id, table_id, index_id, partition_id, new_tablet_id});
        std::string tablet_val;
        ASSERT_EQ(txn->get(tablet_key, &tablet_val), 0);
        doris::TabletMetaPB tablet_pb;
        ASSERT_TRUE(tablet_pb.ParseFromString(tablet_val));
        ASSERT_EQ(tablet_pb.tablet_state(), doris::TabletStatePB::PB_RUNNING);

        // check tablet stats
        ASSERT_EQ(txn->get(tablet_stat_key, &tablet_stat_val), 0);
        ASSERT_TRUE(tablet_stat.ParseFromString(tablet_stat_val));
        // overwrite [11-13], retain [14-15]
        EXPECT_EQ(tablet_stat.num_rows(), sc->num_output_rows() + 200);
        EXPECT_EQ(tablet_stat.num_rowsets(), sc->num_output_rowsets() + 1 + 2);
        EXPECT_EQ(tablet_stat.num_segments(), sc->num_output_segments() + 4);
        EXPECT_EQ(tablet_stat.data_size(), sc->size_output_rowsets() + 2000);

        // check visible rowsets
        std::unique_ptr<RangeGetIterator> it;
        auto rs_start = meta_rowset_key({instance_id, new_tablet_id, 0});
        auto rs_end =
                meta_rowset_key({instance_id, new_tablet_id, std::numeric_limits<int64_t>::max()});
        ASSERT_EQ(txn->get(rs_start, rs_end, &it), 0);
        // [0-1][2-8][9-12][13-13][14-14][15-15]
        EXPECT_EQ(it->size(), 6);
        auto [k, v] = it->next();
        doris::RowsetMetaPB rs_pb;
        ASSERT_TRUE(rs_pb.ParseFromArray(v.data(), v.size()));
        EXPECT_EQ(rs_pb.start_version(), 0);
        EXPECT_EQ(rs_pb.end_version(), 1);
        for (auto [start_v, end_v] : be2_versions) {
            auto [k, v] = it->next();
            ASSERT_TRUE(rs_pb.ParseFromArray(v.data(), v.size()));
            EXPECT_EQ(rs_pb.start_version(), start_v);
            EXPECT_EQ(rs_pb.end_version(), end_v);
            if (start_v == 2) { // [2-8] was committed by BE1
                EXPECT_EQ(rs_pb.rowset_id_v2(), fmt::format("BE1{}", rs_pb.txn_id()));
            } else {
                EXPECT_EQ(rs_pb.rowset_id_v2(), fmt::format("BE2{}", rs_pb.txn_id()));
            }
        }
        for (int64_t ver = 14; ver <= 15; ++ver) {
            auto [k, v] = it->next();
            ASSERT_TRUE(rs_pb.ParseFromArray(v.data(), v.size()));
            EXPECT_EQ(rs_pb.start_version(), ver);
            EXPECT_EQ(rs_pb.end_version(), ver);
        }

        // check recycled rowsets
        auto recycl_rs_start = recycle_rowset_key({instance_id, new_tablet_id, ""});
        auto recycl_rs_end = recycle_rowset_key({instance_id, new_tablet_id, "\xff"});
        ASSERT_EQ(txn->get(recycl_rs_start, recycl_rs_end, &it), 0);
        // [11-11], [12-12], old[13-13]
        EXPECT_EQ(it->size(), 3);
    }
}

TEST(MetaServiceTest, ConflictCompactionTest) {
    auto meta_service = get_meta_service();
    meta_service->resource_mgr_.reset(); // Do not use resource manager

    std::string instance_id = "tablet_job_test_instance_id";

    [[maybe_unused]] auto sp = SyncPoint::get_instance();
    sp->set_call_back("get_instance_id::pred", [](void* p) { *((bool*)p) = true; });
    sp->set_call_back("get_instance_id", [&](void* p) { *((std::string*)p) = instance_id; });
    sp->enable_processing();

    brpc::Controller cntl;

    int64_t table_id = 1;
    int64_t index_id = 2;
    int64_t partition_id = 3;
    int64_t tablet_id = 4;

    // create tablet
    {
        CreateTabletRequest req;
        MetaServiceGenericResponse res;
        auto tablet_meta_pb = req.mutable_tablet_meta();
        tablet_meta_pb->set_table_id(table_id);
        tablet_meta_pb->set_index_id(index_id);
        tablet_meta_pb->set_partition_id(partition_id);
        tablet_meta_pb->set_tablet_id(tablet_id);
        tablet_meta_pb->set_tablet_state(doris::TabletStatePB::PB_RUNNING);
        auto rs_meta_pb = tablet_meta_pb->add_rs_metas();
        rs_meta_pb->set_tablet_id(tablet_id);
        rs_meta_pb->set_rowset_id(0);
        // rs_meta_pb->set_rowset_id_v2("xxx");
        rs_meta_pb->set_start_version(0);
        rs_meta_pb->set_end_version(1);
        meta_service->create_tablet(&cntl, &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    };

    {
        StartTabletJobRequest req;
        StartTabletJobResponse res;
        auto compaction = req.mutable_job()->add_compaction();
        compaction->set_id("job1");
        compaction->set_initiator("BE1");
        req.mutable_job()->mutable_idx()->set_tablet_id(tablet_id);
        compaction->set_base_compaction_cnt(0);
        compaction->set_cumulative_compaction_cnt(0);
        meta_service->start_tablet_job(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                       &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    {
        StartTabletJobRequest req;
        StartTabletJobResponse res;
        auto compaction = req.mutable_job()->add_compaction();
        compaction->set_id("job2");
        compaction->set_initiator("BE1");
        req.mutable_job()->mutable_idx()->set_tablet_id(tablet_id);
        compaction->set_base_compaction_cnt(0);
        compaction->set_cumulative_compaction_cnt(0);
        meta_service->start_tablet_job(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                       &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK); // preempt
    }

    {
        StartTabletJobRequest req;
        StartTabletJobResponse res;
        auto compaction = req.mutable_job()->add_compaction();
        compaction->set_id("job3");
        compaction->set_initiator("BE2");
        req.mutable_job()->mutable_idx()->set_tablet_id(tablet_id);
        compaction->set_base_compaction_cnt(0);
        compaction->set_cumulative_compaction_cnt(0);
        meta_service->start_tablet_job(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                       &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::JOB_TABLET_BUSY);
    }

    // check job kv
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(meta_service->txn_kv_->create_txn(&txn), 0);
    std::string job_key =
            job_tablet_key({instance_id, table_id, index_id, partition_id, tablet_id});
    std::string job_val;
    TabletJobInfoPB job_pb;
    ASSERT_EQ(txn->get(job_key, &job_val), 0);
    ASSERT_TRUE(job_pb.ParseFromString(job_val));
    LOG(INFO) << job_pb.DebugString();
    ASSERT_EQ(job_pb.compaction(0).id(), "job2");
    ASSERT_EQ(job_pb.compaction(0).initiator(), "BE1");
}
