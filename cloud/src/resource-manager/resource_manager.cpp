
// clang-format off
#include "resource_manager.h"
#include "meta-service/keys.h"
#include "common/logging.h"
#include "common/util.h"


#include <sstream>
// clang-format on

namespace selectdb {

int ResourceManager::init() {
    // Scan all instances
    std::unique_ptr<Transaction> txn;
    int ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        LOG(INFO) << "failed to init txn, ret=" << ret;
        return -1;
    }

    InstanceKeyInfo key0_info {""};
    InstanceKeyInfo key1_info {"\xff"}; // instance id are human readable strings
    std::string key0;
    std::string key1;
    instance_key(key0_info, &key0);
    instance_key(key1_info, &key1);

    std::unique_ptr<RangeGetIterator> it;

    int num_instances = 0;
    std::unique_ptr<int, std::function<void(int*)>> defer_log_range(
            (int*)0x01, [key0, key1, &num_instances](int*) {
                LOG(INFO) << "get instances, num_instances=" << num_instances << " range=["
                          << hex(key0) << "," << hex(key1) << "]";
            });

    //                     instance_id  instance
    std::vector<std::tuple<std::string, InstanceInfoPB>> instances;
    do {
        ret = txn->get(key0, key1, &it);
        if (ret != 0) {
            LOG(WARNING) << "internal error, failed to get instance, ret=" << ret;
            return -1;
        }

        while (it->has_next()) {
            auto [k, v] = it->next();
            LOG(INFO) << "range get instance_key=" << hex(k);
            instances.emplace_back("", InstanceInfoPB {});
            auto& [instance_id, inst] = instances.back();

            if (!inst.ParseFromArray(v.data(), v.size())) {
                LOG(WARNING) << "malformed instance, unable to deserialize, key=" << hex(k);
                return -1;
            }
            // 0x01 "instance" ${instance_id} -> InstanceInfoPB
            k.remove_prefix(1); // Remove key space
            std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>> out;
            ret = decode_key(&k, &out);
            if (ret != 0) {
                LOG(WARNING) << "failed to decode key, ret=" << ret;
                return -2;
            }
            if (out.size() != 2) {
                LOG(WARNING) << "decoded size no match, expect 2, given=" << out.size();
            }
            instance_id = std::get<std::string>(std::get<0>(out[1]));

            LOG(INFO) << "get an instance, instance_id=" << instance_id
                      << " instance json=" << proto_to_json(inst);

            ++num_instances;
            if (!it->has_next()) key0 = k;
        }
        key0.push_back('\x00'); // Update to next smallest key for iteration
    } while (it->more());

    for (auto& [inst_id, inst] : instances) {
        for (auto& c : inst.clusters()) {
            add_cluster_to_index(inst_id, c);
        }
    }

    return 0;
}

std::string ResourceManager::get_node(const std::string& cloud_unique_id,
                                      std::vector<NodeInfo>* nodes) {
    std::shared_lock l(mtx_);
    auto [s, e] = node_info_.equal_range(cloud_unique_id);
    if (s == node_info_.end() || s->first != cloud_unique_id) {
        LOG(INFO) << "cloud unique id not found cloud_unique_id=" << cloud_unique_id;
        return "cloud_unique_id not found";
    }
    nodes->reserve(nodes->size() + node_info_.count(cloud_unique_id));
    for (auto i = s; i != e; ++i) {
        nodes->emplace_back(i->second); // Just copy, it's cheap
    }
    return "";
}

std::string ResourceManager::add_node(const std::string& instance_id, const NodeInfo& node) {
    return "";
}

std::string ResourceManager::add_cluster(const std::string& instance_id,
                                         const ClusterInfo& cluster) {
    std::string err;
    std::stringstream ss;

    std::unique_ptr<int, std::function<void(int*)>> defer(
            (int*)0x01, [&err](int*) { LOG(INFO) << "add_cluster err=" << err; });

    // FIXME(gavin): ensure atomicity of the entire process of adding cluster.
    //               Inserting a placeholer to node_info_ before persistence
    //               and updating it after persistence for the cloud_unique_id
    //               to add is a fairly fine solution.
    {
        std::shared_lock l(mtx_);
        // Check uniqueness of cloud_unique_id to add, cloud unique ids are not
        // shared between instances
        for (auto& i : cluster.cluster.nodes()) {
            auto it = node_info_.find(i.cloud_unique_id());
            if (it == node_info_.end()) continue;

            ss << "cloud_unique_id is already occupied by an instance,"
               << " instance_id=" << it->second.instance_id
               << " cluster_name=" << it->second.cluster_name
               << " cluster_id=" << it->second.cluster_id
               << " cloud_unique_id=" << i.cloud_unique_id();
            err = ss.str();
            LOG(INFO) << err;
            return err;
        }
    }

    std::unique_ptr<Transaction> txn0;
    int ret = txn_kv_->create_txn(&txn0);
    if (ret != 0) {
        err = "failed to create txn";
        LOG(WARNING) << err << " ret=" << ret;
        return err;
    }

    std::shared_ptr<Transaction> txn(txn0.release());
    InstanceInfoPB instance;
    auto [c0, m0] = get_instance(txn, instance_id, &instance);
    if (c0 != 0) {
        err = m0;
        return err;
    }

    LOG(INFO) << "cluster to add json=" << proto_to_json(cluster.cluster);
    LOG(INFO) << "json=" << proto_to_json(instance);

    // Check id and name, they need to be unique
    // One cluster id per name, name is alias of cluster id
    for (auto& i : instance.clusters()) {
        if (i.cluster_id() == cluster.cluster.cluster_id() ||
            i.cluster_name() == cluster.cluster.cluster_name()) {
            ss << "try to add a existing cluster,"
               << " existing_cluster_id=" << i.cluster_id()
               << " existing_cluster_name=" << i.cluster_name()
               << " new_cluster_id=" << cluster.cluster.cluster_id()
               << " new_cluster_name=" << cluster.cluster.cluster_name();
            err = ss.str();
            return err;
        }
    }

    // TODO(gavin): Check duplicated nodes, one node cannot deploy on multiple clusters

    instance.add_clusters()->CopyFrom(cluster.cluster);
    LOG(INFO) << "instance " << instance_id << " has " << instance.clusters().size() << " clusters";

    InstanceKeyInfo key_info {instance_id};
    std::string key;
    std::string val;
    instance_key(key_info, &key);

    val = instance.SerializeAsString();
    if (val.empty()) {
        err = "failed to serialize";
        return err;
    }

    txn->put(key, val);
    LOG(INFO) << "put instnace_key=" << hex(key);
    ret = txn->commit();
    if (ret != 0) {
        err = "failed to commit kv txn";
        LOG(WARNING) << err << " ret=" << ret;
        return err;
    }

    add_cluster_to_index(instance_id, cluster.cluster);

    return err;
}

std::string ResourceManager::drop_cluster(const std::string& instance_id,
                                          const ClusterInfo& cluster) {
    std::stringstream ss;
    std::string err;

    std::string cluster_id = cluster.cluster.has_cluster_id() ? cluster.cluster.cluster_id() : "";
    std::string cluster_name =
            cluster.cluster.has_cluster_name() ? cluster.cluster.cluster_name() : "";
    if (cluster_id.empty() || cluster_name.empty()) {
        ss << "missing cluster_id or cluster_name, cluster_id=" << cluster_id
           << " cluster_name=" << cluster_name;
        err = ss.str();
        LOG(INFO) << err;
        return err;
    }

    std::unique_ptr<Transaction> txn0;
    int ret = txn_kv_->create_txn(&txn0);
    if (ret != 0) {
        err = "failed to create txn";
        LOG(WARNING) << err << " ret=" << ret;
        return err;
    }

    std::shared_ptr<Transaction> txn(txn0.release());
    InstanceInfoPB instance;
    auto [c0, m0] = get_instance(txn, instance_id, &instance);
    if (c0 != 0) {
        err = m0;
        return err;
    }

    bool found = false;
    int idx = -1;
    ClusterPB to_del;
    // Check id and name, they need to be unique
    // One cluster id per name, name is alias of cluster id
    for (auto& i : instance.clusters()) {
        ++idx;
        if (i.cluster_id() == cluster.cluster.cluster_id() &&
            i.cluster_name() == cluster.cluster.cluster_name()) {
            to_del.CopyFrom(i);
            LOG(INFO) << "found a cluster to drop,"
                      << " instance_id=" << instance_id << " cluster_id=" << i.cluster_id()
                      << " cluster_name=" << i.cluster_name() << " cluster=" << proto_to_json(i);
            found = true;
            break;
        }
    }

    if (!found) {
        ss << "failed to find cluster to drop,"
           << " instance_id=" << instance_id << " cluster_id=" << cluster.cluster.cluster_id()
           << " cluster_name=" << cluster.cluster.cluster_name();
        err = ss.str();
        return err;
    }

    auto& clusters = const_cast<std::decay_t<decltype(instance.clusters())>&>(instance.clusters());
    clusters.DeleteSubrange(idx, 1); // Remove it

    InstanceKeyInfo key_info {instance_id};
    std::string key;
    std::string val;
    instance_key(key_info, &key);

    val = instance.SerializeAsString();
    if (val.empty()) {
        err = "failed to serialize";
        return err;
    }

    txn->put(key, val);
    LOG(INFO) << "put instnace_key=" << hex(key);
    ret = txn->commit();
    if (ret != 0) {
        err = "failed to commit kv txn";
        LOG(WARNING) << err << " ret=" << ret;
        return err;
    }

    remove_cluster_from_index(instance_id, to_del);

    return err;
}

std::string ResourceManager::rename_cluster(const std::string& instance_id,
                                            const ClusterInfo& cluster) {
    std::stringstream ss;
    std::string err;

    std::string cluster_id = cluster.cluster.has_cluster_id() ? cluster.cluster.cluster_id() : "";
    std::string cluster_name =
            cluster.cluster.has_cluster_name() ? cluster.cluster.cluster_name() : "";
    if (cluster_id.empty() || cluster_name.empty()) {
        ss << "missing cluster_id or cluster_name, cluster_id=" << cluster_id
           << " cluster_name=" << cluster_name;
        err = ss.str();
        LOG(INFO) << err;
        return err;
    }

    // just rename clusters name, find by cluster id

    std::unique_ptr<Transaction> txn0;
    int ret = txn_kv_->create_txn(&txn0);
    if (ret != 0) {
        err = "failed to create txn";
        LOG(WARNING) << err << " ret=" << ret;
        return err;
    }

    std::shared_ptr<Transaction> txn(txn0.release());
    InstanceInfoPB instance;
    auto [c0, m0] = get_instance(txn, instance_id, &instance);
    if (c0 != 0) {
        err = m0;
        return err;
    }

    bool found = false;
    ClusterPB original;
    ClusterPB now;
    // just rename clusters name, find cluster by cluster id
    for (auto& i : instance.clusters()) {
        if (i.cluster_id() != cluster.cluster.cluster_id()) {
            continue;
        }
        if (i.cluster_name() == cluster.cluster.cluster_name()) {
            ss << "failed to rename cluster, name eq original name, original cluster is "
               << proto_to_json(i);
            err = ss.str();
            return err;
        }
        original.CopyFrom(i);
        LOG(INFO) << "found a cluster to rename,"
                  << " instance_id=" << instance_id << " cluster_id=" << i.cluster_id()
                  << " original cluster_name=" << i.cluster_name() << " rename to "
                  << cluster.cluster.cluster_name();
        auto& cluster_to_rename = const_cast<std::decay_t<decltype(i)>&>(i);
        cluster_to_rename.set_cluster_name(cluster.cluster.cluster_name());
        now.CopyFrom(i);
        found = true;
    }

    if (!found) {
        ss << "failed to find cluster to rename,"
           << " instance_id=" << instance_id << " cluster_id=" << cluster.cluster.cluster_id()
           << " cluster_name=" << cluster.cluster.cluster_name();
        err = ss.str();
        return err;
    }

    InstanceKeyInfo key_info {instance_id};
    std::string key;
    std::string val;
    instance_key(key_info, &key);

    val = instance.SerializeAsString();
    if (val.empty()) {
        err = "failed to serialize";
        return err;
    }

    txn->put(key, val);
    LOG(INFO) << "put instnace_key=" << hex(key);
    ret = txn->commit();
    if (ret != 0) {
        err = "failed to commit kv txn";
        LOG(WARNING) << err << " ret=" << ret;
        return err;
    }

    update_cluster_to_index(instance_id, original, now);

    return err;
}

void ResourceManager::update_cluster_to_index(const std::string& instance_id,
                                              const ClusterPB& original, const ClusterPB& now) {
    std::lock_guard l(mtx_);
    remove_cluster_from_index_no_lock(instance_id, original);
    add_cluster_to_index_no_lock(instance_id, now);
}

void ResourceManager::add_cluster_to_index_no_lock(const std::string& instance_id,
                                                   const ClusterPB& c) {
    auto type = c.has_type() ? c.type() : -1;
    Role role = (type == ClusterPB::SQL
                         ? Role::SQL_SERVER
                         : (type == ClusterPB::COMPUTE ? Role::COMPUTE_NODE : Role::UNDEFINED));
    LOG(INFO) << "add cluster to index, instance_id=" << instance_id << " cluster_type=" << type
              << " cluster_name=" << c.cluster_name() << " cluster_id=" << c.cluster_id();

    for (auto& i : c.nodes()) {
        bool existed = node_info_.count(i.cloud_unique_id());
        NodeInfo n {.role = role,
                    .instance_id = instance_id,
                    .cluster_name = c.cluster_name(),
                    .cluster_id = c.cluster_id(),
                    .node_info = i};
        LOG(WARNING) << (existed ? "duplicated cloud_unique_id " : "")
                     << "instance_id=" << instance_id << " cloud_unique_id=" << i.cloud_unique_id()
                     << " node_info=" << proto_to_json(i);
        node_info_.insert({i.cloud_unique_id(), std::move(n)});
    }
}

void ResourceManager::add_cluster_to_index(const std::string& instance_id, const ClusterPB& c) {
    std::lock_guard l(mtx_);
    add_cluster_to_index_no_lock(instance_id, c);
}

void ResourceManager::remove_cluster_from_index_no_lock(const std::string& instance_id,
                                                        const ClusterPB& c) {
    std::string cluster_name = c.cluster_name();
    std::string cluster_id = c.cluster_id();
    int cnt = 0;
    for (auto it = node_info_.begin(); it != node_info_.end();) {
        auto& [_, n] = *it;
        if (n.cluster_id != cluster_id || n.cluster_name != cluster_name) {
            ++it;
            continue;
        }
        ++cnt;
        LOG(INFO) << "remove node from index,"
                  << " role=" << static_cast<int>(n.role) << " cluster_name=" << n.cluster_name
                  << " cluster_id=" << n.cluster_id << " node_info=" << proto_to_json(n.node_info);
        it = node_info_.erase(it);
    }
    LOG(INFO) << cnt << " nodes removed from index, cluster_id=" << cluster_id
              << " cluster_name=" << cluster_name;
}

void ResourceManager::remove_cluster_from_index(const std::string& instance_id,
                                                const ClusterPB& c) {
    std::lock_guard l(mtx_);
    remove_cluster_from_index_no_lock(instance_id, c);
}

std::pair<int, std::string> ResourceManager::get_instance(std::shared_ptr<Transaction> txn,
                                                          const std::string& instance_id,
                                                          InstanceInfoPB* inst_pb) {
    std::pair<int, std::string> ec {0, ""};
    [[maybe_unused]] auto& [code, msg] = ec;
    std::stringstream ss;

    InstanceKeyInfo key_info {instance_id};
    std::string key;
    std::string val;
    instance_key(key_info, &key);

    int ret = 0;
    if (txn == nullptr) {
        std::unique_ptr<Transaction> txn0;
        ret = txn_kv_->create_txn(&txn0);
        if (ret != 0) {
            code = -1;
            msg = "failed to create txn";
            LOG(WARNING) << msg << " ret=" << ret;
            return ec;
        }
        txn.reset(txn0.release());
    }

    ret = txn->get(key, &val);
    LOG(INFO) << "get instnace_key=" << hex(key);

    if (ret != 0) {
        code = -2;
        ss << "failed to get intancece, instance_id=" << instance_id << " ret=" << ret;
        msg = ss.str();
        return ec;
    }

    if (!inst_pb->ParseFromString(val)) {
        code = -3;
        msg = "failed to parse InstanceInfoPB";
        return ec;
    }

    return ec;
}

} // namespace selectdb
// vim: et tw=100 ts=4 sw=4 cc=80:
