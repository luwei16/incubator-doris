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
import lombok.Getter;

import java.util.Map;
import java.util.Map.Entry;

public class FileFormat {
    public static final String TYPE = "type";
    private static final ImmutableSet<String> DATA_DESC_PROPERTIES_SET = new ImmutableSet.Builder<String>()
            .add(LoadStmt.KEY_IN_PARAM_LINE_DELIMITER)
            .add(LoadStmt.KEY_IN_PARAM_STRIP_OUTER_ARRAY)
            .add(LoadStmt.KEY_IN_PARAM_FUZZY_PARSE)
            .add(LoadStmt.KEY_IN_PARAM_NUM_AS_STRING)
            .add(LoadStmt.KEY_IN_PARAM_JSONPATHS)
            .add(LoadStmt.KEY_IN_PARAM_JSONROOT)
            .build();
    private static final ImmutableSet<String> CONFIGURABLE_PROPERTIES_SET = new ImmutableSet.Builder<String>().addAll(
            DATA_DESC_PROPERTIES_SET).add(TYPE).add(LoadStmt.KEY_IN_PARAM_COLUMN_SEPARATOR).build();

    @Getter
    private Map<String, String> properties;

    public FileFormat(Map<String, String> properties) {
        this.properties = properties;
    }

    public void analyze() throws AnalysisException {
        if (!properties.containsKey(TYPE)) {
            throw new AnalysisException("Property '" + TYPE + "' is required in FileFormat");
        }
        for (Entry<String, String> entry : properties.entrySet()) {
            String key = entry.getKey();
            if (!CONFIGURABLE_PROPERTIES_SET.contains(key)) {
                throw new AnalysisException("Property '" + key + "' is invalid in FileFormat");
            }
            if (!DATA_DESC_PROPERTIES_SET.contains(TYPE)) {
                continue;
            }
            try {
                LoadStmt.PROPERTIES_MAP.get(key).apply(entry.getValue());
            } catch (Exception e) {
                throw new AnalysisException("Failed to parse property " + key + ". Error: " + e.getMessage());
            }
        }
    }

    public String toSql() {
        if (properties.size() > 0) {
            StringBuilder sb = new StringBuilder();
            sb.append("file_format = (").append(new PrintableMap<>(properties, "=", true, false)).append(") ");
            return sb.toString();
        }
        return "";
    }

    public void mergeProperties(Map<String, String> properties) {
        for (Entry<String, String> entry : properties.entrySet()) {
            if (!this.properties.containsKey(entry.getKey())) {
                this.properties.put(entry.getKey(), entry.getValue());
            }
        }
    }

    public void toDataDescriptionProperties(Map<String, String> dataDescProperties) {
        for (Entry<String, String> entry : properties.entrySet()) {
            if (DATA_DESC_PROPERTIES_SET.contains(entry.getKey())) {
                dataDescProperties.put(entry.getKey(), entry.getValue());
            }
        }
    }

    public String getFormat() {
        return properties.get(TYPE);
    }

    public String getColumnSeparator() {
        return properties.get(LoadStmt.KEY_IN_PARAM_COLUMN_SEPARATOR);
    }
}
