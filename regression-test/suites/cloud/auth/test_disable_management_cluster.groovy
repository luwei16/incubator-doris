suite("test_disable_management_cluster", "cloud_auth") {
    def host = "127.0.0.1"
    def heart_port = 10086
    def edit_log_port = 10000

    def asab = try_sql """
        ALTER SYSTEM ADD BACKEND "${host}:${heart_port}";
    """
    // failed: ERROR 1105 (HY000): errCode = 2, detailMessage = Unsupported operation.
    assertEquals(asab, null)

    def asaf = try_sql """
        ALTER SYSTEM ADD FOLLOWER "${host}:${edit_log_port}"
    """
    // failed: ERROR 1105 (HY000): errCode = 2, detailMessage = Unsupported operation.
    assertEquals(asaf, null)

    def asdb = try_sql """
        ALTER SYSTEM DROP BACKEND "${host}:${heart_port}"
    """
    // failed: ERROR 1105 (HY000): errCode = 2, detailMessage = Unsupported operation.
    assertEquals(asdb, null)

    def asdf = try_sql """
        ALTER SYSTEM DROP FOLLOWER "${host}:${edit_log_port}"
    """
    // failed: ERROR 1105 (HY000): errCode = 2, detailMessage = Unsupported operation.
    assertEquals(asdf, null)
}