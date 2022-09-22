#pragma once

#include "olap/base_compaction.h"

namespace doris {

class CloudBaseCompaction : public BaseCompaction {
public:
    CloudBaseCompaction(TabletSharedPtr tablet);
    ~CloudBaseCompaction() override;

    Status prepare_compact() override;
    Status execute_compact_impl() override;

protected:
    std::string compaction_name() const override { return "CloudBaseCompaction"; }

    Status update_tablet_meta() override;
    void garbage_collection() override;

private:
    std::string _uuid;
};

} // namespace doris
