import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_compaction_with_delete") {
    def tableName = "test_compaction_with_delete"

    try {
        //BackendId,Cluster,IP,HeartbeatPort,BePort,HttpPort,BrpcPort,LastStartTime,LastHeartbeat,Alive,SystemDecommissioned,ClusterDecommissioned,TabletNum,DataUsedCapacity,AvailCapacity,TotalCapacity,UsedPct,MaxDiskUsedPct,RemoteUsedCapacity,Tag,ErrMsg,Version,Status
        String[][] backends = sql """ show backends; """
        assertTrue(backends.size() > 0)
        def backendId_to_backendIP = [:]
        def backendId_to_backendHttpPort = [:]
        def cluster_to_backendId = [:]
        for (String[] backend in backends) {
            backendId_to_backendIP.put(backend[0], backend[2])
            backendId_to_backendHttpPort.put(backend[0], backend[5])
            def tagJson = parseJson(backend[19])
            if (!cluster_to_backendId.containsKey(tagJson.cloud_cluster_name)) {
                cluster_to_backendId.put(tagJson.cloud_cluster_name, backend[0])
            }
        }
        assertTrue(cluster_to_backendId.size() >= 2)
        def cluster0 = cluster_to_backendId.keySet()[0]
        def cluster1 = cluster_to_backendId.keySet()[1]
        def backend_id0 = cluster_to_backendId.get(cluster0)
        def backend_id1 = cluster_to_backendId.get(cluster1)

        sql """ DROP TABLE IF EXISTS ${tableName}; """
        sql """
            CREATE TABLE ${tableName} (
                `id` int(11) NULL,
                `name` varchar(255) NULL,
                `score` int(11) SUM NULL
            ) ENGINE=OLAP
            AGGREGATE KEY(`id`, `name`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "in_memory" = "false",
                "persistent" = "false",
                "storage_format" = "V2",
                "light_schema_change" = "true",
                "disable_auto_compaction" = "true"
            );
        """

        //TabletId,ReplicaId,BackendId,SchemaHash,Version,LstSuccessVersion,LstFailedVersion,LstFailedTime,LocalDataSize,RemoteDataSize,RowCount,State,LstConsistencyCheckTime,CheckVersion,VersionCount,PathHash,MetaUrl,CompactionStatus
        String[][] tablets = sql """ show tablets from ${tableName}; """

        def doCompaction = { backend_id, compact_type, should_success ->
            // trigger compactions for all tablets in ${tableName}
            for (String[] tablet in tablets) {
                String tablet_id = tablet[0]
                StringBuilder sb = new StringBuilder();
                sb.append("curl -X POST http://")
                sb.append(backendId_to_backendIP.get(backend_id))
                sb.append(":")
                sb.append(backendId_to_backendHttpPort.get(backend_id))
                sb.append("/api/compaction/run?tablet_id=")
                sb.append(tablet_id)
                sb.append("&compact_type=${compact_type}")

                String command = sb.toString()
                process = command.execute()
                code = process.waitFor()
                err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
                out = process.getText()
                logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)
                assertEquals(code, 0)
                def compactJson = parseJson(out.trim())
                if (should_success) {
                    assertEquals("success", compactJson.status.toLowerCase())
                }
            }
            if (!should_success) {
                return
            }

            // wait for all compactions done
            for (String[] tablet in tablets) {
                boolean running = true
                do {
                    Thread.sleep(1000)
                    String tablet_id = tablet[0]
                    StringBuilder sb = new StringBuilder();
                    sb.append("curl -X GET http://")
                    sb.append(backendId_to_backendIP.get(backend_id))
                    sb.append(":")
                    sb.append(backendId_to_backendHttpPort.get(backend_id))
                    sb.append("/api/compaction/run_status?tablet_id=")
                    sb.append(tablet_id)

                    String command = sb.toString()
                    logger.info(command)
                    process = command.execute()
                    code = process.waitFor()
                    err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
                    out = process.getText()
                    logger.info("Get compaction status: code=" + code + ", out=" + out + ", err=" + err)
                    assertEquals(code, 0)
                    def compactionStatus = parseJson(out.trim())
                    assertEquals("success", compactionStatus.status.toLowerCase())
                    running = compactionStatus.run_status
                } while (running)
            }

            int rowCount = 0
            for (String[] tablet in tablets) {
                String tablet_id = tablet[0]
                StringBuilder sb = new StringBuilder();
                def compactionStatusUrlIndex = 17
                sb.append("curl -X GET ")
                sb.append(tablet[compactionStatusUrlIndex])
                String command = sb.toString()
                // wait for cleaning stale_rowsets
                process = command.execute()
                code = process.waitFor()
                err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
                out = process.getText()
                logger.info("Show tablets status: code=" + code + ", out=" + out + ", err=" + err)
                assertEquals(code, 0)
                def tabletJson = parseJson(out.trim())
                assert tabletJson.rowsets instanceof List
                for (String rowset in (List<String>) tabletJson.rowsets) {
                    rowCount += Integer.parseInt(rowset.split(" ")[1])
                }
            }
            assert (rowCount < 8)
        }

        sql """ use @${cluster0}; """
        sql """ INSERT INTO ${tableName} VALUES (1, "a", 100); """
        sql """ INSERT INTO ${tableName} VALUES (1, "a", 100); """
        sql """ INSERT INTO ${tableName} VALUES (1, "a", 100); """
        sql """ INSERT INTO ${tableName} VALUES (1, "a", 100); """
        sql """ INSERT INTO ${tableName} VALUES (1, "a", 100); """
        sql """ INSERT INTO ${tableName} VALUES (1, "a", 100); """
        doCompaction.call(backend_id0, "cumulative", true)
        sql """ INSERT INTO ${tableName} VALUES (2, "a", 100); """
        sql """ DELETE FROM ${tableName} WHERE id = 1; """
        // no suitable version but promote cumulative point to delete version + 1
        doCompaction.call(backend_id0, "cumulative", false)
        // TODO(cyx): check cumulative point
        sql """ INSERT INTO ${tableName} VALUES (3, "a", 100); """
        sql """ INSERT INTO ${tableName} VALUES (3, "a", 100); """
        sql """ INSERT INTO ${tableName} VALUES (3, "a", 100); """
        sql """ INSERT INTO ${tableName} VALUES (3, "a", 100); """
        sql """ INSERT INTO ${tableName} VALUES (3, "a", 100); """
        sql """ INSERT INTO ${tableName} VALUES (3, "a", 100); """
        doCompaction.call(backend_id0, "cumulative", true)
        qt_select_default """ SELECT * FROM ${tableName}; """

    } finally {
        try_sql("DROP TABLE IF EXISTS ${tableName}")
    }    
}
