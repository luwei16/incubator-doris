#include "http/http_handler.h"

namespace doris {

class PadSegmentAction : public HttpHandler {
public:
    PadSegmentAction() = default;

    ~PadSegmentAction() override = default;

    void handle(HttpRequest* req) override;
};

} // namespace doris
