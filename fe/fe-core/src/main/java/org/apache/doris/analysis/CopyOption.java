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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import lombok.Getter;

import java.util.Map;
import java.util.Map.Entry;

public class CopyOption {
    public static final String SIZE_LIMIT = "size_limit";
    public static final String ON_ERROR = "on_error";
    public static final String ON_ERROR_CONTINUE = "continue";
    public static final String ON_ERROR_ABORT_STATEMENT = "abort_statement";
    public static final String ON_ERROR_MAX_FILTER_RATIO = LoadStmt.MAX_FILTER_RATIO_PROPERTY + "_";
    private static final ImmutableSet<String> CONFIGURABLE_PROPERTIES_SET = new ImmutableSet.Builder<String>()
            .add(SIZE_LIMIT)
            .add(ON_ERROR)
            .build();

    @Getter
    private Map<String, String> properties;

    public CopyOption(Map<String, String> properties) {
        this.properties = properties;
    }

    public void analyze() throws AnalysisException {
        for (Entry<String, String> entry : properties.entrySet()) {
            String key = entry.getKey();
            if (!CONFIGURABLE_PROPERTIES_SET.contains(key)) {
                throw new AnalysisException("Property '" + key + "' is invalid in CopyOption");
            }
            if (key.equals(SIZE_LIMIT)) {
                analyzeSizeLimit(key, entry.getValue());
            } else if (key.equals(ON_ERROR)) {
                analyzeOnError(key, entry.getValue());
            }
        }
    }

    public void analyzeProperty(Map<String, String> properties) {
        properties.put(LoadStmt.MAX_FILTER_RATIO_PROPERTY, String.valueOf(getMaxFilterRatio()));
    }

    public void mergeProperties(Map<String, String> properties) {
        for (Entry<String, String> entry : properties.entrySet()) {
            if (!this.properties.containsKey(entry.getKey())) {
                this.properties.put(entry.getKey(), entry.getValue());
            }
        }
    }

    public String toSql() {
        if (properties.size() > 0) {
            StringBuilder sb = new StringBuilder();
            sb.append("copy_option = (").append(new PrintableMap<>(properties, "=", true, false)).append(") ");
            return sb.toString();
        }
        return "";
    }

    private void analyzeSizeLimit(String key, String value) throws AnalysisException {
        try {
            Long.parseLong(value);
        } catch (Exception e) {
            throw new AnalysisException("Failed to parse property " + key + ". Error: " + e.getMessage());
        }
    }

    /**
     * @return the size limit, note that 0 means no limit
     */
    public long getSizeLimit() {
        if (properties.containsKey(SIZE_LIMIT)) {
            return Long.parseLong(this.properties.get(SIZE_LIMIT));
        }
        return 0;
    }

    @VisibleForTesting
    protected double getMaxFilterRatio() {
        if (properties.containsKey(ON_ERROR)) {
            String value = this.properties.get(ON_ERROR);
            if (value.startsWith(ON_ERROR_MAX_FILTER_RATIO)) {
                return Double.parseDouble(value.substring(ON_ERROR_MAX_FILTER_RATIO.length()));
            } else if (value.equalsIgnoreCase(ON_ERROR_CONTINUE)) {
                return 1;
            } else {
                return 0;
            }
        }
        return 0;
    }

    private void analyzeOnError(String key, String value) throws AnalysisException {
        try {
            if (value.startsWith(ON_ERROR_MAX_FILTER_RATIO)) {
                double maxFilterRatio = getMaxFilterRatio();
                if (maxFilterRatio < 0 || maxFilterRatio > 1) {
                    throw new AnalysisException("max_filter_ratio must in [0, 1]");
                }
            } else if (!value.equalsIgnoreCase(ON_ERROR_CONTINUE) && !value.equalsIgnoreCase(
                    ON_ERROR_ABORT_STATEMENT)) {
                throw new AnalysisException("Failed to parse property " + key + " with value: " + value);
            }
        } catch (Exception e) {
            throw new AnalysisException("Failed to parse property " + key + ". Error: " + e.getMessage());
        }
    }
}
