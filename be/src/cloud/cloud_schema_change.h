#pragma once

#include "olap/rowset/rowset.h"
#include "olap/schema_change.h"

namespace doris::cloud {

class CloudSchemaChange {
public:
    CloudSchemaChange(std::string job_id, int64_t expiration);
    ~CloudSchemaChange(); 

    // This method is idempotent for a same request.
    Status process_alter_tablet(const TAlterTabletReqV2& request);

private:
    Status _convert_historical_rowsets(const SchemaChangeParams& sc_params);

private:
    std::string _job_id;
    std::vector<RowsetSharedPtr> _output_rowsets;
    int64_t _output_cumulative_point;
    int64_t _expiration;
};

} // namespace doris::cloud
