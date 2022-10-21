#include "cloud/cloud_schema_change.h"

#include "cloud/utils.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/tablet.h"
#include "olap/tablet_meta.h"

namespace doris::cloud {

static constexpr int ALTER_TABLE_BATCH_SIZE = 4096;

static std::unique_ptr<SchemaChange> get_sc_procedure(const RowBlockChanger& rb_changer,
                                                      bool sc_sorting, bool sc_directly) {
    if (sc_sorting) {
        return std::make_unique<VSchemaChangeWithSorting>(
                rb_changer, config::memory_limitation_per_thread_for_schema_change_bytes);
    }
    return std::make_unique<VSchemaChangeDirectly>(rb_changer);
}

CloudSchemaChange::CloudSchemaChange(std::string job_id) : _job_id(std::move(job_id)) {}

CloudSchemaChange::~CloudSchemaChange() = default;

Status CloudSchemaChange::process_alter_tablet(const TAlterTabletReqV2& request) {
    LOG(INFO) << "Begin to alter tablet. base_tablet_id=" << request.base_tablet_id
              << ", new_tablet_id=" << request.new_tablet_id
              << ", alter_version=" << request.alter_version << ", job_id=" << _job_id;

    // new tablet has to exist
    TabletSharedPtr new_tablet;
    RETURN_IF_ERROR(cloud::tablet_mgr()->get_tablet(request.new_tablet_id, &new_tablet));
    if (new_tablet->tablet_state() == TABLET_RUNNING) {
        LOG(INFO) << "schema change job has already finished. base_tablet_id="
                  << request.base_tablet_id << ", new_tablet_id=" << request.new_tablet_id
                  << ", alter_version=" << request.alter_version << ", job_id=" << _job_id;
        return Status::OK();
    }

    TabletSharedPtr base_tablet;
    RETURN_IF_ERROR(cloud::tablet_mgr()->get_tablet(request.base_tablet_id, &base_tablet));
    std::unique_lock<std::mutex> schema_change_lock(base_tablet->get_schema_change_lock(),
                                                    std::try_to_lock);
    if (!schema_change_lock.owns_lock()) {
        LOG(WARNING) << "Failed to obtain schema change lock. base_tablet="
                     << request.base_tablet_id;
        return Status::OLAPInternalError(OLAP_ERR_TRY_LOCK_FAILED);
    }

    // MUST sync rowsets before capturing rowset readers and building DeleteHandler
    RETURN_IF_ERROR(base_tablet->cloud_sync_rowsets(request.alter_version));
    _output_cumulative_point = base_tablet->cumulative_layer_point();

    std::vector<RowsetReaderSharedPtr> rs_readers;
    if (request.alter_version > 1) {
        // [0-1] is a placeholder rowset, no need to convert
        RETURN_IF_ERROR(base_tablet->cloud_capture_rs_readers({2, base_tablet->local_max_version()},
                                                              &rs_readers));
    }
    // FIXME(cyx): Should trigger compaction on base_tablet if there are too many rowsets to convert.

    // Create a new tablet schema, should merge with dropped columns in light weight schema change
    TabletSchemaSPtr base_tablet_schema = std::make_shared<TabletSchema>();
    base_tablet_schema->copy_from(*base_tablet->tablet_schema());
    if (!request.columns.empty() && request.columns[0].col_unique_id >= 0) {
        base_tablet_schema->clear_columns();
        for (const auto& column : request.columns) {
            base_tablet_schema->append_column(TabletColumn(column));
        }
    }

    // delete handlers for new tablet
    DeleteHandler delete_handler;
    {
        std::shared_lock base_tablet_lock(base_tablet->get_header_lock());
        auto& all_del_preds = base_tablet->delete_predicates();
        for (auto& delete_pred : all_del_preds) {
            if (delete_pred->version().first > request.alter_version) {
                continue;
            }
            base_tablet_schema->merge_dropped_columns(
                    base_tablet->tablet_schema(delete_pred->version()));
        }
        RETURN_IF_ERROR(
                delete_handler.init(base_tablet_schema, all_del_preds, request.alter_version));
    }

    std::vector<ColumnId> return_columns;
    return_columns.resize(base_tablet_schema->num_columns());
    std::iota(return_columns.begin(), return_columns.end(), 0);

    // reader_context is stack variables, it's lifetime MUST keep the same with rs_readers
    RowsetReaderContext reader_context;
    reader_context.reader_type = READER_ALTER_TABLE;
    reader_context.tablet_schema = base_tablet_schema;
    reader_context.need_ordered_result = true;
    reader_context.delete_handler = &delete_handler;
    reader_context.return_columns = &return_columns;
    reader_context.sequence_id_idx = reader_context.tablet_schema->sequence_col_idx();
    reader_context.is_unique = base_tablet->keys_type() == UNIQUE_KEYS;
    reader_context.batch_size = ALTER_TABLE_BATCH_SIZE;
    reader_context.is_vec = config::enable_vectorized_alter_table;

    for (auto& rs_reader : rs_readers) {
        RETURN_IF_ERROR(rs_reader->init(&reader_context));
    }

    SchemaChangeParams sc_params;

    DescriptorTbl::create(&sc_params.pool, request.desc_tbl, &sc_params.desc_tbl);
    sc_params.base_tablet = base_tablet;
    sc_params.new_tablet = new_tablet;
    sc_params.ref_rowset_readers = rs_readers;
    sc_params.delete_handler = &delete_handler;
    sc_params.base_tablet_schema = base_tablet_schema;
    if (!request.__isset.materialized_view_params) {
        return _convert_historical_rowsets(sc_params);
    }
    for (auto& item : request.materialized_view_params) {
        AlterMaterializedViewParam mv_param;
        mv_param.column_name = item.column_name;

        if (item.__isset.origin_column_name) {
            mv_param.origin_column_name = item.origin_column_name;
        }

        if (item.__isset.mv_expr) {
            if (item.mv_expr.nodes[0].node_type == TExprNodeType::FUNCTION_CALL) {
                mv_param.mv_expr = item.mv_expr.nodes[0].fn.name.function_name;
            } else if (item.mv_expr.nodes[0].node_type == TExprNodeType::CASE_EXPR) {
                mv_param.mv_expr = "count_field";
            }
            mv_param.expr = std::make_shared<TExpr>(item.mv_expr);
        }
        sc_params.materialized_params_map.emplace(item.column_name, std::move(mv_param));
    }
    return _convert_historical_rowsets(sc_params);
}

Status CloudSchemaChange::_convert_historical_rowsets(const SchemaChangeParams& sc_params) {
    LOG(INFO) << "Begin to convert historical rowsets for new_tablet from base_tablet. base_tablet="
              << sc_params.base_tablet->tablet_id()
              << ", new_tablet=" << sc_params.new_tablet->tablet_id() << ", job_id=" << _job_id;

    auto& new_tablet = sc_params.new_tablet;

    // Add filter information in change, and filter column information will be set in _parse_request
    // And filter some data every time the row block changes
    RowBlockChanger rb_changer(new_tablet->tablet_schema(), sc_params.delete_handler,
                               *sc_params.desc_tbl);

    bool sc_sorting = false;
    bool sc_directly = false;

    // 1. Parse the Alter request and convert it into an internal representation
    RETURN_IF_ERROR(
            SchemaChangeHandler::_parse_request(sc_params, &rb_changer, &sc_sorting, &sc_directly));

    // 2. Generate historical data converter
    auto sc_procedure = get_sc_procedure(rb_changer, sc_sorting, sc_directly);

    selectdb::TabletJobInfoPB job;
    auto idx = job.mutable_idx();
    idx->set_tablet_id(sc_params.base_tablet->tablet_id());
    idx->set_table_id(sc_params.base_tablet->table_id());
    idx->set_index_id(sc_params.base_tablet->index_id());
    idx->set_partition_id(sc_params.base_tablet->partition_id());
    auto sc_job = job.mutable_schema_change();
    sc_job->set_id(_job_id);
    sc_job->set_initiator(BackendOptions::get_localhost() + ':' +
                          std::to_string(config::heartbeat_service_port));
    auto new_tablet_idx = sc_job->mutable_new_tablet_idx();
    new_tablet_idx->set_tablet_id(new_tablet->tablet_id());
    new_tablet_idx->set_table_id(new_tablet->table_id());
    new_tablet_idx->set_index_id(new_tablet->index_id());
    new_tablet_idx->set_partition_id(new_tablet->partition_id());
    auto st = cloud::meta_mgr()->prepare_tablet_job(job);
    if (!st.ok()) {
        if (st.precise_code() == JOB_ALREADY_SUCCESS) {
            st = new_tablet->cloud_sync_rowsets();
            if (!st.ok()) {
                LOG_WARNING("failed to sync new tablet")
                        .tag("tablet_id", new_tablet->tablet_id())
                        .error(st);
            }
            return Status::OK();
        }
        return st;
    }

    // 3. Convert historical data
    for (auto& rs_reader : sc_params.ref_rowset_readers) {
        VLOG_TRACE << "Begin to convert a history rowset. version=" << rs_reader->version();

        std::unique_ptr<RowsetWriter> rowset_writer;
        RowsetWriterContext context;
        context.txn_id = rs_reader->rowset()->txn_id();
        context.version = rs_reader->version();
        context.rowset_state = VISIBLE;
        context.segments_overlap = rs_reader->rowset()->rowset_meta()->segments_overlap();
        context.tablet_schema = new_tablet->tablet_schema();
        context.oldest_write_timestamp = rs_reader->oldest_write_timestamp();
        context.newest_write_timestamp = rs_reader->newest_write_timestamp();
        context.fs = cloud::latest_fs();
        RETURN_IF_ERROR(new_tablet->create_rowset_writer(&context, &rowset_writer));

        RowsetMetaSharedPtr existed_rs_meta;
        auto st = meta_mgr()->prepare_rowset(rowset_writer->rowset_meta(), true, &existed_rs_meta);
        if (!st.ok()) {
            if (st.is_already_exist()) {
                LOG(INFO) << "Rowset " << rs_reader->version() << " has already existed in tablet "
                          << new_tablet->tablet_id();
                // Add already committed rowset to _output_rowsets.
                DCHECK(existed_rs_meta != nullptr);
                RowsetSharedPtr rowset;
                // schema is nullptr implies using RowsetMeta.tablet_schema
                RowsetFactory::create_rowset(nullptr, sc_params.new_tablet->tablet_path(),
                                             std::move(existed_rs_meta), &rowset);
                _output_rowsets.push_back(std::move(rowset));
                continue;
            } else {
                return st;
            }
        }

        RETURN_IF_ERROR(sc_procedure->process(rs_reader, rowset_writer.get(), sc_params.new_tablet,
                                              sc_params.base_tablet, sc_params.base_tablet_schema));

        auto new_rowset = rowset_writer->build();
        if (!new_rowset) {
            return Status::InternalError("failed to build rowset, version=[{}-{}]",
                                         rs_reader->version().first, rs_reader->version().second);
        }

        st = meta_mgr()->commit_rowset(rowset_writer->rowset_meta(), true, &existed_rs_meta);
        if (!st.ok()) {
            if (st.is_already_exist()) {
                LOG(INFO) << "Rowset " << rs_reader->version() << " has already existed in tablet "
                          << new_tablet->tablet_id();
                // Add already committed rowset to _output_rowsets.
                DCHECK(existed_rs_meta != nullptr);
                RowsetSharedPtr rowset;
                // schema is nullptr implies using RowsetMeta.tablet_schema
                RowsetFactory::create_rowset(nullptr, sc_params.new_tablet->tablet_path(),
                                             std::move(existed_rs_meta), &rowset);
                _output_rowsets.push_back(std::move(rowset));
                continue;
            } else {
                return st;
            }
        }
        _output_rowsets.push_back(std::move(new_rowset));

        VLOG_TRACE << "Successfully convert a history version " << rs_reader->version();
    }

    if (sc_params.ref_rowset_readers.empty()) {
        sc_job->set_alter_version(1); // no rowset to convert implies alter_version == 1
    } else {
        int64_t num_output_rows = 0;
        int64_t size_output_rowsets = 0;
        int64_t num_output_segments = 0;
        for (auto& rs : _output_rowsets) {
            sc_job->add_txn_ids(rs->txn_id());
            sc_job->add_output_versions(rs->end_version());
            num_output_rows += rs->num_rows();
            size_output_rowsets += rs->data_disk_size();
            num_output_segments += rs->num_segments();
        }
        sc_job->set_num_output_rows(num_output_rows);
        sc_job->set_size_output_rowsets(size_output_rowsets);
        sc_job->set_num_output_segments(num_output_segments);
        sc_job->set_num_output_rowsets(_output_rowsets.size());
        sc_job->set_alter_version(_output_rowsets.back()->end_version());
    }
    _output_cumulative_point = std::min(_output_cumulative_point, sc_job->alter_version() + 1);
    sc_job->set_output_cumulative_point(_output_cumulative_point);

    selectdb::TabletStatsPB stats;
    st = cloud::meta_mgr()->commit_tablet_job(job, &stats);
    if (!st.ok()) {
        if (st.precise_code() == JOB_ALREADY_SUCCESS) {
            st = new_tablet->cloud_sync_rowsets();
            if (!st.ok()) {
                LOG_WARNING("failed to sync new tablet")
                        .tag("tablet_id", new_tablet->tablet_id())
                        .error(st);
            }
            return Status::OK();
        }
        return st;
    }

    {
        std::lock_guard wlock(new_tablet->get_header_lock());
        new_tablet->cloud_add_rowsets(std::move(_output_rowsets), true);
        new_tablet->set_cumulative_layer_point(_output_cumulative_point);
        new_tablet->reset_approximate_stats(stats.num_rowsets(), stats.num_segments(),
                                            stats.num_rows(), stats.data_size());
        new_tablet->set_tablet_state(TABLET_RUNNING);
    }
    return Status::OK();
}

} // namespace doris::cloud
