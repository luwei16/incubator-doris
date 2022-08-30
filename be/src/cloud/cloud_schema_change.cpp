#include "cloud/cloud_schema_change.h"

#include "cloud/utils.h"
#include "common/status.h"
#include "olap/olap_define.h"
#include "vec/olap/block_reader.h"

namespace doris::cloud {

static constexpr int ALTER_TABLE_BATCH_SIZE = 4096;

Status CloudSchemaChangeHandler::process_alter_tablet(const TAlterTabletReqV2& request) {
    LOG(INFO) << "Begin to alter tablet. base_tablet_id=" << request.base_tablet_id
              << ", new_tablet_id=" << request.new_tablet_id
              << ", alter_version=" << request.alter_version;

    auto base_tablet =
            StorageEngine::instance()->tablet_manager()->get_tablet(request.base_tablet_id);
    if (base_tablet == nullptr) {
        LOG(WARNING) << "Fail to find base tablet. base_tablet=" << request.base_tablet_id;
        return Status::OLAPInternalError(OLAP_ERR_TABLE_NOT_FOUND);
    }
    std::unique_lock<std::mutex> schema_change_lock(base_tablet->get_schema_change_lock(),
                                                    std::try_to_lock);
    if (!schema_change_lock.owns_lock()) {
        LOG(WARNING) << "Failed to obtain schema change lock. base_tablet="
                     << request.base_tablet_id;
        return Status::OLAPInternalError(OLAP_ERR_TRY_LOCK_FAILED);
    }

    // new tablet has to exist
    TabletSharedPtr new_tablet =
            StorageEngine::instance()->tablet_manager()->get_tablet(request.new_tablet_id);
    if (new_tablet == nullptr) {
        LOG(WARNING) << "Fail to find new tablet. new_tablet=" << request.new_tablet_id;
        return Status::OLAPInternalError(OLAP_ERR_TABLE_NOT_FOUND);
    }

    auto missed_versions = new_tablet->cloud_calc_missed_versions(request.alter_version);
    if (missed_versions.empty()) {
        LOG(INFO) << "The convert job has already finished. base_tablet_id="
                  << request.base_tablet_id << ", new_tablet_id=" << request.new_tablet_id
                  << ", alter_version=" << request.alter_version;
        return Status::OK();
    }

    std::vector<RowsetReaderSharedPtr> rs_readers;
    // delete handlers for new tablet
    DeleteHandler delete_handler;
    std::vector<ColumnId> return_columns;
    auto base_tablet_schema = std::make_shared<TabletSchema>();
    base_tablet_schema->copy_from(*base_tablet->tablet_schema());
    if (!request.columns.empty() && request.columns[0].col_unique_id >= 0) {
        base_tablet_schema->clear_columns();
        for (const auto& column : request.columns) {
            base_tablet_schema->append_column(TabletColumn(column));
        }
    }

    {
        size_t num_cols = base_tablet_schema->num_columns();
        return_columns.resize(num_cols);
        for (int i = 0; i < num_cols; ++i) {
            return_columns[i] = i;
        }

        // reader_context is stack variables, it's lifetime should keep the same
        // with rs_readers
        RowsetReaderContext reader_context;
        reader_context.reader_type = READER_ALTER_TABLE;
        reader_context.tablet_schema = base_tablet_schema;
        reader_context.need_ordered_result = true;
        reader_context.delete_handler = &delete_handler;
        reader_context.return_columns = &return_columns;
        // for schema change, seek_columns is the same to return_columns
        reader_context.return_columns = &return_columns;
        reader_context.sequence_id_idx = reader_context.tablet_schema->sequence_col_idx();
        reader_context.is_unique = base_tablet->keys_type() == UNIQUE_KEYS;
        reader_context.batch_size = ALTER_TABLE_BATCH_SIZE;
        reader_context.is_vec = config::enable_vectorized_alter_table;

        std::shared_lock base_tablet_rlock(base_tablet->get_header_lock());
        RETURN_IF_ERROR(base_tablet->capture_rs_readers(missed_versions, &rs_readers));

        vectorized::BlockReader reader;
        TabletReader::ReaderParams reader_params;
        reader_params.tablet = base_tablet;
        reader_params.reader_type = READER_ALTER_TABLE;
        reader_params.rs_readers = rs_readers;
        reader_params.tablet_schema = base_tablet_schema;
        reader_params.return_columns.resize(base_tablet_schema->num_columns());
        std::iota(reader_params.return_columns.begin(), reader_params.return_columns.end(), 0);
        reader_params.origin_return_columns = &reader_params.return_columns;
        reader_params.version = {0, request.alter_version};
        // BlockReader::init will call base_tablet->get_header_lock(), but this lock we already get at outer layer, so we just call TabletReader::init
        RETURN_IF_ERROR(reader.init(reader_params));

        RETURN_IF_ERROR(delete_handler.init(base_tablet_schema, base_tablet->delete_predicates(),
                                            request.alter_version, &reader));

        for (auto& rs_reader : rs_readers) {
            RETURN_IF_ERROR(rs_reader->init(&reader_context));
        }
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

Status CloudSchemaChangeHandler::_convert_historical_rowsets(const SchemaChangeParams& sc_params) {
    LOG(INFO) << "Begin to convert historical rowsets for new_tablet from base_tablet. base_tablet="
              << sc_params.base_tablet->tablet_id()
              << ", new_tablet=" << sc_params.new_tablet->tablet_id();

    // Add filter information in change, and filter column information will be set in _parse_request
    // And filter some data every time the row block changes
    RowBlockChanger rb_changer(sc_params.new_tablet->tablet_schema(), sc_params.delete_handler,
                               *sc_params.desc_tbl);

    bool sc_sorting = false;
    bool sc_directly = false;

    // 1. Parse the Alter request and convert it into an internal representation
    RETURN_IF_ERROR(SchemaChangeHandler::_parse_request(
            sc_params.base_tablet, sc_params.new_tablet, &rb_changer, &sc_sorting, &sc_directly,
            sc_params.materialized_params_map, *sc_params.desc_tbl, sc_params.base_tablet_schema));

    // 2. Generate historical data converter
    auto sc_procedure = SchemaChangeHandler::get_sc_procedure(rb_changer, sc_sorting, sc_directly);

    // 3. Convert historical data
    for (auto& rs_reader : sc_params.ref_rowset_readers) {
        VLOG_TRACE << "Begin to convert a history rowset. version=" << rs_reader->version();

        auto& new_tablet = sc_params.new_tablet;

        std::unique_ptr<RowsetWriter> rowset_writer;
        RowsetWriterContext context;
        context.version = rs_reader->version();
        context.rowset_state = VISIBLE;
        context.segments_overlap = rs_reader->rowset()->rowset_meta()->segments_overlap();
        context.tablet_schema = new_tablet->tablet_schema();
        context.oldest_write_timestamp = rs_reader->oldest_write_timestamp();
        context.newest_write_timestamp = rs_reader->newest_write_timestamp();
        context.fs = new_tablet->data_dir()->fs();
        RETURN_IF_ERROR(new_tablet->create_rowset_writer(&context, &rowset_writer));

        auto st = meta_mgr()->prepare_rowset(rowset_writer->rowset_meta(), false);
        if (!st.ok()) {
            if (st.is_already_exist()) {
                // This should only occur when:
                // 1. BE restarts during `_convert_historical_rowsets`,
                //    and the last `commit_rowset` request is not completed before calling `get_tablet`.
                // 2. `commit_rowset` timeout but succeeded, and FE retries alter task.
                LOG(INFO) << "Rowset " << rs_reader->version() << " has already existed in tablet "
                          << sc_params.new_tablet->tablet_id();
                // TODO(cyx): Add already committed rowset to tablet.
                continue;
            } else {
                return st;
            }
        }

        RETURN_IF_ERROR(sc_procedure->process(rs_reader, rowset_writer.get(), sc_params.new_tablet,
                                              sc_params.base_tablet));

        auto new_rowset = rowset_writer->build();
        if (!new_rowset) {
            return Status::InternalError("failed to build rowset, version=[{}-{}]",
                                         rs_reader->version().first, rs_reader->version().second);
        }

        st = meta_mgr()->commit_rowset(rowset_writer->rowset_meta(), false);
        if (st.ok()) {
            sc_params.new_tablet->add_new_rowset(new_rowset);
        } else {
            if (st.is_already_exist()) {
                // This should only occur when:
                // 1. BE restarts during `_convert_historical_rowsets`,
                //    and the last `commit_rowset` request is not completed before calling `get_tablet`.
                // 2. `commit_rowset` timeout but succeeded, and FE retries alter task.
                LOG(INFO) << "Rowset " << rs_reader->version() << " has already existed in tablet "
                          << sc_params.new_tablet->tablet_id();
                // TODO(cyx): Add already committed rowset to tablet.
            } else {
                return st;
            }
        }

        VLOG_TRACE << "Successfully convert a history version " << rs_reader->version();
    }
    return Status::OK();
}

} // namespace doris::cloud
