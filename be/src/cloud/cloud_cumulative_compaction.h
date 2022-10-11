#pragma once

#include "olap/cumulative_compaction.h"

namespace doris {

class CloudCumulativeCompaction : public CumulativeCompaction {
public:
    CloudCumulativeCompaction(TabletSharedPtr tablet);
    ~CloudCumulativeCompaction() override;

    Status prepare_compact() override;
    Status execute_compact_impl() override;

protected:
    Status pick_rowsets_to_compact() override;

    std::string compaction_name() const override { return "CloudCumulativeCompaction"; }

    Status update_tablet_meta() override;
    void garbage_collection() override;

private:
    void update_cumulative_point(int64_t base_compaction_cnt, int64_t cumulative_compaction_cnt);

private:
    std::string _uuid;
};

} // namespace doris
