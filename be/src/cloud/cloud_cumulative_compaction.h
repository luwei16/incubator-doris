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
    std::string compaction_name() const override { return "CloudCumulativeCompaction"; }

    Status update_tablet_meta() override;
    void garbage_collection() override;

private:
    std::string _uuid;
};

} // namespace doris
