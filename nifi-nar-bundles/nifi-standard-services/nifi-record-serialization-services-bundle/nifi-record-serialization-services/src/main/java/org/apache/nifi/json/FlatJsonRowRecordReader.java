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

package org.apache.nifi.json;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.DataType;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordField;
import org.apache.nifi.serialization.RecordFieldType;
import org.apache.nifi.serialization.RecordSchema;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.codehaus.jackson.JsonNode;


public class FlatJsonRowRecordReader extends AbstractJsonRowRecordReader {
    private final Map<String, DataType> fieldTypeOverrides;


    public FlatJsonRowRecordReader(final InputStream in, final ComponentLog logger, final Map<String, DataType> fieldTypeOverrides) throws IOException, MalformedRecordException {
        super(in, logger);
        this.fieldTypeOverrides = fieldTypeOverrides;
    }

    @Override
    protected Object[] convertJsonNodeToObjectArray(final JsonNode jsonNode, final RecordSchema schema) {
        if (jsonNode == null) {
            return null;
        }

        final Object[] values = new Object[schema.getFieldCount()];
        for (int i = 0; i < schema.getFieldCount(); i++) {
            final RecordField field = schema.getField(i);
            final String fieldName = field.getFieldName();
            final JsonNode fieldNode = jsonNode.get(fieldName);

            final DataType desiredType = field.getDataType();
            values[i] = convertField(fieldNode, fieldName, desiredType);
        }

        return values;
    }


    @Override
    public RecordSchema getSchema() {
        final List<RecordField> recordFields = new ArrayList<>();
        final Optional<JsonNode> firstNodeOption = getFirstJsonNode();

        if (firstNodeOption.isPresent()) {
            final Iterator<Map.Entry<String, JsonNode>> itr = firstNodeOption.get().getFields();
            while (itr.hasNext()) {
                final Map.Entry<String, JsonNode> entry = itr.next();
                final String elementName = entry.getKey();
                final JsonNode node = entry.getValue();

                DataType dataType;
                final DataType overriddenDataType = fieldTypeOverrides.get(elementName);
                if (overriddenDataType == null) {
                    final RecordFieldType fieldType = determineFieldType(node);
                    dataType = fieldType.getDataType();
                } else {
                    dataType = overriddenDataType;
                }

                recordFields.add(new RecordField(elementName, dataType));
            }
        }

        // If there are any overridden field types that we didn't find, add as the last fields.
        final Set<String> knownFieldNames = recordFields.stream()
            .map(f -> f.getFieldName())
            .collect(Collectors.toSet());

        for (final Map.Entry<String, DataType> entry : fieldTypeOverrides.entrySet()) {
            if (!knownFieldNames.contains(entry.getKey())) {
                recordFields.add(new RecordField(entry.getKey(), entry.getValue()));
            }
        }

        return new SimpleRecordSchema(recordFields);
    }

}
