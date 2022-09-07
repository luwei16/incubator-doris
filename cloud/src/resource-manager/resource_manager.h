
#pragma once
// clang-format off

#include "meta-service/txn_kv.h"
#include "gen_cpp/selectdb_cloud.pb.h"

#include <map>
#include <string>
#include <shared_mutex>
// clang-format on

namespace selectdb {

enum class Role : int {
    UNDEFINED,
    SQL_SERVER,
    COMPUTE_NODE,
};

struct NodeInfo {
    Role role;
    std::string instance_id;
    std::string cluster_name;
    std::string cluster_id;
    NodeInfoPB node_info;
};

struct ClusterInfo {
    ClusterPB cluster;
};

/**
 * This class manages resources referenced by selectdb cloud.
 * It manage a in-memory
 */
class ResourceManager {
public:
    ResourceManager(std::shared_ptr<TxnKv> txn_kv) : txn_kv_(txn_kv) {};

    /**
     * Loads all instance into memory and build an index
     *
     * @return 0 for success, non-zero for failure
     */
    int init();

    /**
     * Gets nodes with given cloud unique id
     *
     * @param cloud_unique_id the cloud_unique_id attached to the node when it was added to the
     *                        instance
     * @param node output param
     * @return empty string for success, otherwise failure reason returned
     */
    std::string get_node(const std::string& cloud_unique_id, std::vector<NodeInfo>* nodes);

    std::string add_cluster(const std::string& instance_id, const ClusterInfo& cluster);

    /**
     * Drops a cluster
     *
     * @param clsuter cluster to drop, only cluster name and clsuter id are concered
     * @return empty string for success, otherwise failure reason returned
     */
    std::string drop_cluster(const std::string& instance_id, const ClusterInfo& cluster);

    /**
     * Update a cluster
     *
     * @param clsuter cluster to update, only cluster name and clsuter id are concered
     * @param action update operation code snippet
     * @filter filter condition
     * @return empty string for success, otherwise failure reason returned
     */
    std::string update_cluster(const std::string& instance_id, const ClusterInfo& cluster,
                               std::function<std::string(::selectdb::ClusterPB&)> action,
                               std::function<bool(const ::selectdb::ClusterPB&)> filter);

    /**
     * Get instance from underlying storage with given transaction.
     *
     * @param txn if txn is not given, get with a new txn inside this function
     *
     * @return a <code, msg> pair, code == 0 for success, otherwise error
     */
    std::pair<int, std::string> get_instance(std::shared_ptr<Transaction> txn,
                                             const std::string& instance_id,
                                             InstanceInfoPB* inst_pb);
    // return err msg
    std::string modify_nodes(const std::string& instance_id, const std::vector<NodeInfo>& to_add,
                             const std::vector<NodeInfo>& to_del);

    bool check_cluster_params_valid(const ClusterPB& cluster, std::string* err,
                                    bool check_master_num);

private:
    void add_cluster_to_index(const std::string& instance_id, const ClusterPB& cluster);

    void remove_cluster_from_index(const std::string& instance_id, const ClusterPB& cluster);

    void update_cluster_to_index(const std::string& instance_id, const ClusterPB& original,
                                 const ClusterPB& now);

    void remove_cluster_from_index_no_lock(const std::string& instance_id,
                                           const ClusterPB& cluster);

    void add_cluster_to_index_no_lock(const std::string& instance_id, const ClusterPB& cluster);

private:
    std::shared_mutex mtx_;
    // cloud_unique_id -> NodeInfo
    std::multimap<std::string, NodeInfo> node_info_;

    std::shared_ptr<TxnKv> txn_kv_;
};

} // namespace selectdb
// vim: et tw=100 ts=4 sw=4 cc=80:
