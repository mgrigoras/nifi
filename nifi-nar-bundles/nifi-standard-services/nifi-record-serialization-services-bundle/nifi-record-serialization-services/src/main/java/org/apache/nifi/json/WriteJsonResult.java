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

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigInteger;
import java.sql.Array;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonGenerator;

public class WriteJsonResult implements RecordSetWriter {
    private final boolean prettyPrint;

    public WriteJsonResult(final boolean prettyPrint) {
        this.prettyPrint = prettyPrint;
    }

    @Override
    public WriteResult write(final RecordSet rs, final OutputStream rawOut) throws IOException {
        int count = 0;

        final JsonFactory factory = new JsonFactory();
        final DateFormat dateFormat = new SimpleDateFormat("MM-dd-yyyy");
        final DateFormat timeFormat = new SimpleDateFormat("HH:mm:ss.SSS");
        final DateFormat timestampFormat = new SimpleDateFormat("MM-dd-yyyy HH:mm:ss.SSS");

        try (final OutputStream out = new BufferedOutputStream(rawOut);
            final JsonGenerator generator = factory.createJsonGenerator(out)) {

            if (prettyPrint) {
                generator.useDefaultPrettyPrinter();
            }

            generator.writeStartArray();

            Record record;
            while ((record = rs.next()) != null) {
                count++;
                writeRecord(record, generator, dateFormat, timeFormat, timestampFormat,
                    g -> g.writeStartObject(), g -> g.writeEndObject());
            }

            generator.writeEndArray();
        } catch (final SQLException e) {
            throw new IOException("Failed to serialize Result Set to stream", e);
        }

        return WriteResult.of(count, Collections.emptyMap());
    }

    private void writeRecord(final Record record, final JsonGenerator generator, final DateFormat dateFormat, final DateFormat timeFormat, final DateFormat timestampFormat,
        final GeneratorTask startTask, final GeneratorTask endTask)
        throws JsonGenerationException, IOException, SQLException {

        final RecordSchema schema = record.getSchema();
        startTask.apply(generator);
        for (int i = 0; i < schema.getFieldCount(); i++) {
            final String fieldName = schema.getField(i).getFieldName();
            final Object value = record.getValue(i);
            if (value == null) {
                generator.writeNullField(fieldName);
                continue;
            }

            generator.writeFieldName(fieldName);
            final DataType dataType = schema.getDataType(fieldName).get();
            writeValue(generator, value, dataType, i < schema.getFieldCount() - 1, dateFormat, timeFormat, timestampFormat);
        }

        endTask.apply(generator);
    }


    private void writeValue(final JsonGenerator generator, final Object value, final DataType dataType, final boolean moreCols,
        final DateFormat dateFormat, final DateFormat timeFormat, final DateFormat timestampFormat) throws JsonGenerationException, IOException, SQLException {

        if (value == null) {
            generator.writeNull();
            return;
        }

        switch (dataType.getFieldType()) {
            case DATE:
                generator.writeString(dateFormat.format(value.toString()));
                break;
            case TIME:
                generator.writeString(timeFormat.format(value.toString()));
                break;
            case TIMESTAMP:
                generator.writeString(timestampFormat.format(value.toString()));
                break;
            case DOUBLE:
                generator.writeNumber((Double) value);
                break;
            case FLOAT:
                generator.writeNumber((Float) value);
                break;
            case INT:
                generator.writeNumber((Integer) value);
                break;
            case BIGINT:
                if (value instanceof Long) {
                    generator.writeNumber(((Long) value).longValue());
                } else {
                    generator.writeNumber((BigInteger) value);
                }
                break;
            case BOOLEAN:
                final String stringValue = value.toString();
                if ("true".equalsIgnoreCase(stringValue)) {
                    generator.writeBoolean(true);
                } else if ("false".equalsIgnoreCase(stringValue)) {
                    generator.writeBoolean(false);
                } else {
                    generator.writeString(stringValue);
                }
                break;
            case STRING:
                generator.writeString(value.toString());
                break;
            case ARRAY:
            case RECORD:
            default:
                if ("null".equals(value.toString())) {
                    generator.writeNull();
                } else if (value instanceof Map) {
                    final Map<?, ?> map = (Map<?, ?>) value;
                    generator.writeStartObject();

                    int i = 0;
                    for (final Map.Entry<?, ?> entry : map.entrySet()) {
                        generator.writeFieldName(entry.getKey().toString());
                        final boolean moreEntries = ++i < map.size();
                        writeValue(generator, entry.getValue(), getColType(entry.getValue()), moreEntries, dateFormat, timeFormat, timestampFormat);
                    }
                    generator.writeEndObject();
                } else if (value instanceof List) {
                    final List<?> list = (List<?>) value;
                    writeArray(list.toArray(), generator, dateFormat, timeFormat, timestampFormat);
                } else if (value instanceof Array) {
                    final Array array = (Array) value;
                    final Object[] values = (Object[]) array.getArray();
                    writeArray(values, generator, dateFormat, timeFormat, timestampFormat);
                } else if (value instanceof Object[]) {
                    final Object[] values = (Object[]) value;
                    writeArray(values, generator, dateFormat, timeFormat, timestampFormat);
                } else {
                    generator.writeString(value.toString());
                }
                break;
        }
    }

    private void writeArray(final Object[] values, final JsonGenerator generator, final DateFormat dateFormat, final DateFormat timeFormat, final DateFormat timestampFormat)
        throws JsonGenerationException, IOException, SQLException {
        generator.writeStartArray();
        for (int i = 0; i < values.length; i++) {
            final boolean moreEntries = i < values.length - 1;
            final Object element = values[i];
            writeValue(generator, element, getColType(element), moreEntries, dateFormat, timeFormat, timestampFormat);
        }
        generator.writeEndArray();
    }

    private DataType getColType(final Object value) {
        if (value instanceof String) {
            return RecordFieldType.STRING.getDataType();
        }
        if (value instanceof Double) {
            return RecordFieldType.DOUBLE.getDataType();
        }
        if (value instanceof Float) {
            return RecordFieldType.FLOAT.getDataType();
        }
        if (value instanceof Integer) {
            return RecordFieldType.INT.getDataType();
        }
        if (value instanceof Long) {
            return RecordFieldType.LONG.getDataType();
        }
        if (value instanceof BigInteger) {
            return RecordFieldType.BIGINT.getDataType();
        }
        if (value instanceof Boolean) {
            return RecordFieldType.BOOLEAN.getDataType();
        }
        if (value instanceof Byte || value instanceof Short) {
            return RecordFieldType.INT.getDataType();
        }
        if (value instanceof Character) {
            return RecordFieldType.STRING.getDataType();
        }
        if (value instanceof java.util.Date || value instanceof java.sql.Date) {
            return RecordFieldType.DATE.getDataType();
        }
        if (value instanceof java.sql.Time) {
            return RecordFieldType.TIME.getDataType();
        }
        if (value instanceof java.sql.Timestamp) {
            return RecordFieldType.TIMESTAMP.getDataType();
        }
        if (value instanceof Object[] || value instanceof List || value instanceof Array) {
            return RecordFieldType.ARRAY.getDataType();
        }

        return RecordFieldType.RECORD.getDataType();
    }

    @Override
    public String getMimeType() {
        return "application/json";
    }

    private static interface GeneratorTask {
        void apply(JsonGenerator generator) throws JsonGenerationException, IOException;
    }
}
