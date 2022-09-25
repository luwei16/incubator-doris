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

package org.apache.doris.analysis;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.PrintableMap;

import com.google.common.collect.ImmutableSet;
import com.selectdb.cloud.proto.SelectdbCloud.ObjectStoreInfoPB;
import com.selectdb.cloud.proto.SelectdbCloud.StagePB;
import lombok.Getter;

import java.util.Map;
import java.util.Map.Entry;

public class StageParam {
    public static final String ENDPOINT = "endpoint";
    public static final String REGION = "region";
    public static final String BUCKET = "bucket";
    public static final String PREFIX = "prefix";
    public static final String AK = "ak";
    public static final String SK = "sk";
    private static final ImmutableSet<String> CONFIGURABLE_PROPERTIES_SET = new ImmutableSet.Builder<String>().add(
            ENDPOINT).add(REGION).add(BUCKET).add(PREFIX).add(AK).add(SK).build();

    @Getter
    protected StagePB.StageType type;
    private Map<String, String> properties;

    public StageParam(StagePB.StageType type, Map<String, String> properties) {
        this.type = type;
        this.properties = properties;
    }

    public void analyze() throws AnalysisException {
        for (Entry<String, String> entry : properties.entrySet()) {
            if (!CONFIGURABLE_PROPERTIES_SET.contains(entry.getKey())) {
                throw new AnalysisException("Property '" + entry.getKey() + "' is invalid for ExternalStage");
            }
        }
        if (properties.size() != CONFIGURABLE_PROPERTIES_SET.size()) {
            for (String prop : CONFIGURABLE_PROPERTIES_SET) {
                if (!properties.containsKey(prop)) {
                    throw new AnalysisException("Property '" + prop + "' is required for ExternalStage");
                }
            }
        }
    }

    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("(").append(new PrintableMap<>(properties, "=", true, false)).append(") ");
        return sb.toString();
    }

    public ObjectStoreInfoPB toProto() {
        return ObjectStoreInfoPB.newBuilder().setEndpoint(properties.get(ENDPOINT)).setRegion(properties.get(REGION))
                .setBucket(properties.get(BUCKET)).setPrefix(properties.get(PREFIX)).setAk(properties.get(AK))
                .setSk(properties.get(SK)).build();
    }
}
