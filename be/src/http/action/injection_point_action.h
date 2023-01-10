#include "http/http_handler.h"

namespace doris {

class InjectionPointAction : public HttpHandler {
public:
    InjectionPointAction() = default;

    ~InjectionPointAction() override = default;

    void handle(HttpRequest* req) override;

    static void register_suites();
};

} // namespace doris
