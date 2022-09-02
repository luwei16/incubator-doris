#pragma once

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <mutex>
#include <unordered_map>

#include "olap/olap_common.h"
namespace doris {
namespace io {

struct AtomicStatistics {
    std::atomic<int64_t> num_io_total = 0;
    std::atomic<int64_t> num_io_hit_cache = 0;
    std::atomic<int64_t> num_io_bytes_read_total = 0;
    std::atomic<int64_t> num_io_bytes_read_from_file_cache = 0;
    std::atomic<int64_t> num_io_bytes_read_from_write_cache = 0;
    std::atomic<int64_t> num_io_written_in_file_cache = 0;
    std::atomic<int64_t> num_io_bytes_written_in_file_cache = 0;
};

struct FileCacheProfile {
    static FileCacheProfile& instance() {
        static FileCacheProfile s_profile;
        return s_profile;
    }

    // avoid performance impact, use https to control
    inline static bool enable_profile = true;

    static void set_enable_profile(bool flag) {
        // if enable_profile = false originally, set true, it will clear the count
        if (!enable_profile && flag) {
            std::lock_guard lock(instance().mtx);
            instance().profile.clear();
        }
        enable_profile = flag;
    }

    void update(int64_t table_id, int64_t partition_id, OlapReaderStatistics* stats) {
        if (!enable_profile) {
            return;
        }
        std::shared_ptr<AtomicStatistics> count;
        std::lock_guard lock(mtx);
        {
            if (profile.count(table_id) < 1 || profile[table_id].count(partition_id) < 1) {
                profile[table_id][partition_id] = std::make_shared<AtomicStatistics>();
            }
            count = profile[table_id][partition_id];
        }
        count->num_io_total.fetch_add(stats->file_cache_stats.num_io_total);
        count->num_io_hit_cache.fetch_add(stats->file_cache_stats.num_io_hit_cache);
        count->num_io_bytes_read_total.fetch_add(stats->file_cache_stats.num_io_bytes_read_total);
        count->num_io_bytes_read_from_file_cache.fetch_add(
                stats->file_cache_stats.num_io_bytes_read_from_file_cache);
        count->num_io_bytes_read_from_write_cache.fetch_add(
                stats->file_cache_stats.num_io_bytes_read_from_write_cache);
        count->num_io_written_in_file_cache.fetch_add(
                stats->file_cache_stats.num_io_written_in_file_cache);
        count->num_io_bytes_written_in_file_cache.fetch_add(
                stats->file_cache_stats.num_io_bytes_written_in_file_cache);
    }
    std::mutex mtx;
    // use shared_ptr for concurrent
    std::unordered_map<int64_t, std::unordered_map<int64_t, std::shared_ptr<AtomicStatistics>>>
            profile;

    FileCacheStatistics report(int64_t table_id) {
        FileCacheStatistics stats;
        if (profile.count(table_id) == 1) {
            std::lock_guard lock(mtx);
            auto& partition_map = profile[table_id];
            for (auto& [partition_id, atomic_stats] : partition_map) {
                stats.num_io_total += atomic_stats->num_io_total;
                stats.num_io_hit_cache += atomic_stats->num_io_hit_cache;
                stats.num_io_bytes_read_total += atomic_stats->num_io_bytes_read_total;
                stats.num_io_bytes_read_from_file_cache +=
                        atomic_stats->num_io_bytes_read_from_file_cache;
                stats.num_io_bytes_read_from_write_cache +=
                        atomic_stats->num_io_bytes_read_from_write_cache;
                stats.num_io_written_in_file_cache += atomic_stats->num_io_written_in_file_cache;
                stats.num_io_bytes_written_in_file_cache +=
                        atomic_stats->num_io_bytes_written_in_file_cache;
            }
        }
        return stats;
    }

    FileCacheStatistics report(int64_t table_id, int64_t partition_id) {
        FileCacheStatistics stats;
        if (profile.count(table_id) == 1 && profile[table_id].count(partition_id) == 1) {
            std::shared_ptr<AtomicStatistics> count;
            {
                std::lock_guard lock(mtx);
                count = profile[table_id][partition_id];
            }
            stats.num_io_total = count->num_io_total;
            stats.num_io_hit_cache = count->num_io_hit_cache;
            stats.num_io_bytes_read_total = count->num_io_bytes_read_total;
            stats.num_io_bytes_read_from_file_cache = count->num_io_bytes_read_from_file_cache;
            stats.num_io_bytes_read_from_write_cache = count->num_io_bytes_read_from_write_cache;
            stats.num_io_written_in_file_cache = count->num_io_written_in_file_cache;
            stats.num_io_bytes_written_in_file_cache = count->num_io_bytes_written_in_file_cache;
        }
        return stats;
    }
};

} // namespace io
} // namespace doris