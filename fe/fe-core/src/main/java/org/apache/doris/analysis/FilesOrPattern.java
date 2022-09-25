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

import com.google.common.base.Joiner;
import lombok.Getter;

import java.util.List;

public class FilesOrPattern {

    @Getter
    private List<String> files;
    @Getter
    private String pattern;

    public FilesOrPattern(List<String> files, String pattern) {
        this.files = files;
        this.pattern = pattern;
    }

    public String toSql() {
        StringBuilder sb = new StringBuilder();
        if (files != null) {
            sb.append("FILES = (");
            Joiner.on(", ").appendTo(sb, files).append(")");
        }
        if (pattern != null) {
            sb.append("PATTERN = '").append(pattern).append("'");
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return "FilesOrPattern{" + "files=" + files + ", pattern='" + pattern + '\'' + '}';
    }
}
