#pragma once

#include <memory>

#include "olap/base_compaction.h"

namespace doris {

class CloudBaseCompaction : public BaseCompaction,
                            std::enable_shared_from_this<CloudBaseCompaction> {
public:
    CloudBaseCompaction(TabletSharedPtr tablet);
    ~CloudBaseCompaction() override;

    Status prepare_compact() override;
    Status execute_compact_impl() override;

    void do_lease();

protected:
    std::string compaction_name() const override { return "CloudBaseCompaction"; }

    Status update_tablet_meta() override;
    void garbage_collection() override;

private:
    std::string _uuid;
};

std::vector<std::shared_ptr<CloudBaseCompaction>> get_base_compactions();
void push_base_compaction(std::shared_ptr<CloudBaseCompaction> compaction);
void pop_base_compaction(CloudBaseCompaction* compaction);

} // namespace doris
