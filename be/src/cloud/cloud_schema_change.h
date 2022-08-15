#pragma once

#include "olap/schema_change.h"

namespace doris::cloud {

class CloudSchemaChangeHandler {
public:
    // This method is idempotent for a same request.
    static Status process_alter_tablet(const TAlterTabletReqV2& request);

private:
    static Status _convert_historical_rowsets(const SchemaChangeParams& sc_params);
};

} // namespace doris::cloud
