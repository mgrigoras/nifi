/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.serialization;

import java.util.Collections;
import java.util.Map;

public interface WriteResult {

    /**
     * @return the number of records written
     */
    int getRecordCount();

    /**
     * @return values that should be added to the FLowFile as attributes
     */
    Map<String, String> getAttributes();

    public static WriteResult of(final int recordCount, final Map<String, String> attributes) {
        return new WriteResult() {
            @Override
            public int getRecordCount() {
                return recordCount;
            }

            @Override
            public Map<String, String> getAttributes() {
                return attributes;
            }
        };
    }

    public static final WriteResult EMPTY = of(0, Collections.emptyMap());
}
