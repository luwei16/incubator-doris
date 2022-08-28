#pragma once

#include <functional>
#include <memory>
#include <string_view>

#include "meta-service/txn_kv.h"
#include "recycler/s3_accessor.h"

namespace selectdb {

class RecyclerImpl;

class Recycler {
public:
    Recycler();
    ~Recycler() = default;

    // returns 0 for success otherwise error
    int start();

private:
    std::vector<std::string> get_instance_ids();

private:
    std::shared_ptr<TxnKv> txn_kv_;
    std::unique_ptr<RecyclerImpl> impl_;
};

class RecyclerImpl {
public:
    RecyclerImpl(std::shared_ptr<TxnKv> txn_kv, std::shared_ptr<S3Accessor> accessor);
    ~RecyclerImpl() = default;

    // scan and recycle expired indexes
    void recycle_indexes(const std::string& instance_id);

    // scan and recycle expired partitions
    void recycle_partitions(const std::string& instance_id);

    // scan and recycle expired prepared rowsets
    void recycle_rowsets(const std::string& instance_id);

    // scan and recycle expired tmp rowsets
    void recycle_tmp_rowsets(const std::string& instance_id);

    /**
     * recycle all tablets belonging to the index specified by `index_id`
     *
     * @param partition_id if positive, only recycle tablets in this partition belonging to the specified index
     * @return 0 for success otherwise error
     */
    int recycle_tablets(const std::string& instance_id, int64_t table_id, int64_t index_id,
                        int64_t partition_id = -1);

    /**
     * recycle all rowsets belonging to the tablet specified by `tablet_id`
     *
     * @return 0 for success otherwise error
     */
    int recycle_tablet(const std::string& instance_id, int64_t tablet_id);

private:
    /**
     * Scan key-value pairs between [`begin`, `end`), and perform `recycle_func` on each key-value pair.
     * This function will also remove successfully recycled key-value pairs from kv store.
     *
     * @param recycle_func defines how to recycle resources corresponding to a key-value pair. Returns true if the recycling is successful.
     * @param try_range_remove_kv if true, try to remove key-value pairs from kv store by range which is more efficient
     * @return 0 if all corresponding resources(including key-value pairs) are recycled successfully, otherwise non-zero
     */
    int scan_and_recycle(std::string begin, std::string_view end,
                         std::function<bool(std::string_view k, std::string_view v)> recycle_func,
                         bool try_range_remove_kv = false);

private:
    std::shared_ptr<TxnKv> txn_kv_;
    // TODO(cyx): support multi s3 endpoint
    std::shared_ptr<S3Accessor> accessor_;
};

} // namespace selectdb
