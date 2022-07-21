// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.catalog;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

public class CloudReplica extends Replica {
    private static final Logger LOG = LogManager.getLogger(CloudReplica.class);

    public CloudReplica(long replicaId, List<Long> backendIds, long version, int schemaHash,
                        long dataSize, long remoteDataSize, long rowCount, ReplicaState state,
                        long lastFailedVersion,
                        long lastSuccessVersion) {
        super(replicaId, -1, version, schemaHash, dataSize, remoteDataSize,
                rowCount, state, lastFailedVersion, lastSuccessVersion);
    }

    public CloudReplica(long replicaId, List<Long> backendId, ReplicaState state, long version, int schemaHash) {
        super(replicaId, -1, state, version, schemaHash);
    }

    // In the future, a replica may be mapped to multiple BEs in a cluster,
    // so this value is be list
    private Map<List<Long>, List<Long>> clusterToBackends;
}
