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

package org.apache.nifi.serialization.record;

import java.util.Date;

public interface Record {

    RecordSchema getSchema();

    /**
     * <p>
     * Returns a view of the the values of the fields in this Record.
     * </p>
     *
     * <b>NOTE:</b> The array that is returned may be an underlying array that is backing
     * the contents of the Record. As such, modifying the array in any way may result in
     * modifying the record.
     *
     * @return a view of the values of the fields in this Record
     */
    Object[] getValues();

    Object getValue(int index);

    Object getValue(String fieldName);

    String getAsString(int index);

    String getAsString(String fieldName);

    String getAsString(int index, String format);

    String getAsString(String fieldName, String format);

    Long getAsLong(int index);

    Long getAsLong(String fieldName);

    Integer getAsInt(int index);

    Integer getAsInt(String fieldName);

    Double getAsDouble(int index);

    Double getAsDouble(String fieldName);

    Float getAsFloat(int index);

    Float getAsFloat(String fieldName);

    Record getAsRecord(int index);

    Record getAsRecord(String fieldName);

    Boolean getAsBoolean(int index);

    Boolean getAsBoolean(String fieldName);

    Date getAsDate(int index);

    Date getAsDate(String fieldName);

    Date getAsDate(int index, String format);

    Date getAsDate(String fieldName, String format);

    Object[] getAsArray(int index);

    Object[] getAsArray(String fieldName);
}
