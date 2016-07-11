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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SimpleRecordSchema implements RecordSchema {
    private final List<RecordField> fields;
    private final Map<String, DataType> types;

    public SimpleRecordSchema(final List<RecordField> fields) {
        this.fields = Collections.unmodifiableList(new ArrayList<>(fields));
        types = fields.stream().collect(Collectors.toMap(field -> field.getFieldName(), field -> field.getDataType()));
    }

    @Override
    public List<RecordField> getFields() {
        return fields;
    }

    @Override
    public int getFieldCount() {
        return fields.size();
    }

    @Override
    public RecordField getField(final int index) {
        return fields.get(index);
    }

    @Override
    public List<DataType> getDataTypes() {
        return getFields().stream().map(recordField -> recordField.getDataType())
            .collect(Collectors.toList());
    }

    @Override
    public List<String> getFieldNames() {
        return getFields().stream().map(recordField -> recordField.getFieldName())
            .collect(Collectors.toList());
    }

    @Override
    public DataType getDataType(final String fieldName) {
        return types.get(fieldName);
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof RecordSchema)) {
            return false;
        }

        final RecordSchema other = (RecordSchema) obj;
        return fields.equals(other.getFields());
    }

    @Override
    public int hashCode() {
        return 143 + 3 * fields.hashCode();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("[");

        for (int i = 0; i < fields.size(); i++) {
            final RecordField field = fields.get(i);

            sb.append("\"");
            sb.append(field.getFieldName());
            sb.append("\" : \"");
            sb.append(field.getDataType());
            sb.append("\"");

            if (i < fields.size() - 1) {
                sb.append(", ");
            }
        }
        sb.append("]");
        return sb.toString();
    }
}
