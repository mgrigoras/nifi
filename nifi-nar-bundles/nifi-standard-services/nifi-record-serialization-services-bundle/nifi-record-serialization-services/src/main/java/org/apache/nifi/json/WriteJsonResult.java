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
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.nifi.serialization.ResultSetWriter;
import org.apache.nifi.serialization.WriteResult;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonGenerator;

public class WriteJsonResult implements ResultSetWriter {
    private final boolean prettyPrint;

    public WriteJsonResult(final boolean prettyPrint) {
        this.prettyPrint = prettyPrint;
    }

    @Override
    public WriteResult write(final ResultSet resultSet, final OutputStream rawOut) throws IOException {
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

            while (resultSet.next()) {
                count++;
                writeObject(resultSet, generator, dateFormat, timeFormat, timestampFormat, g -> g.writeStartObject(), g -> g.writeEndObject());
            }

            generator.writeEndArray();
        } catch (final SQLException e) {
            throw new IOException("Failed to serialize Result Set to stream", e);
        }

        return WriteResult.of(count, Collections.emptyMap());
    }

    private void writeObject(final ResultSet rs, final JsonGenerator generator, final DateFormat dateFormat, final DateFormat timeFormat, final DateFormat timestampFormat,
        final GeneratorTask startTask, final GeneratorTask endTask)
        throws JsonGenerationException, IOException, SQLException {

        final ResultSetMetaData metadata = rs.getMetaData();

        startTask.apply(generator);
        for (int i = 0; i < metadata.getColumnCount(); i++) {
            final int col = i + 1;

            final String fieldName = metadata.getColumnLabel(col);
            final Object value = rs.getObject(col);
            if (value == null) {
                generator.writeNullField(fieldName);
                continue;
            }

            generator.writeFieldName(fieldName);
            final int colType = metadata.getColumnType(col);

            writeValue(generator, value, colType, i < metadata.getColumnCount() - 1, dateFormat, timeFormat, timestampFormat);
        }

        endTask.apply(generator);
    }


    private void writeValue(final JsonGenerator generator, final Object value, final int colType, final boolean moreCols,
        final DateFormat dateFormat, final DateFormat timeFormat, final DateFormat timestampFormat) throws JsonGenerationException, IOException, SQLException {

        if (value == null) {
            generator.writeNull();
            return;
        }

        switch (colType) {
            case Types.DATE:
                generator.writeString(dateFormat.format(value.toString()));
                break;
            case Types.TIME:
                generator.writeString(timeFormat.format(value.toString()));
                break;
            case Types.TIMESTAMP:
                generator.writeString(timestampFormat.format(value.toString()));
                break;
            case Types.DOUBLE:
                generator.writeNumber((Double) value);
                break;
            case Types.FLOAT:
                generator.writeNumber((Float) value);
                break;
            case Types.INTEGER:
            case Types.SMALLINT:
            case Types.TINYINT:
                generator.writeNumber((Integer) value);
                break;
            case Types.BIGINT:
                if (value instanceof Long) {
                    generator.writeNumber(((Long) value).longValue());
                } else {
                    generator.writeNumber((BigInteger) value);
                }
                break;
            case Types.BOOLEAN:
                final String stringValue = value.toString();
                if ("true".equalsIgnoreCase(stringValue)) {
                    generator.writeBoolean(true);
                } else if ("false".equalsIgnoreCase(stringValue)) {
                    generator.writeBoolean(false);
                } else {
                    generator.writeString(stringValue);
                }
                break;
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
                generator.writeString(value.toString());
                break;
            case Types.ARRAY:
            case Types.JAVA_OBJECT:
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
                    generator.writeStartArray();
                    for (int i = 0; i < list.size(); i++) {
                        final boolean moreEntries = i < list.size() - 1;
                        final Object element = list.get(i);
                        writeValue(generator, element, getColType(element), moreEntries, dateFormat, timeFormat, timestampFormat);
                    }
                    generator.writeEndArray();
                } else if (value instanceof Array) {
                    final Array array = (Array) value;
                    generator.writeStartArray();
                    final Object[] values = (Object[]) array.getArray();
                    for (int i = 0; i < values.length; i++) {
                        final boolean moreEntries = i < values.length - 1;
                        final Object element = values[i];
                        writeValue(generator, element, getColType(element), moreEntries, dateFormat, timeFormat, timestampFormat);
                    }
                    generator.writeEndArray();
                } else {
                    generator.writeString(value.toString());
                }
                break;
        }
    }

    private int getColType(final Object value) {
        if (value instanceof String) {
            return Types.LONGVARCHAR;
        }
        if (value instanceof Double) {
            return Types.DOUBLE;
        }
        if (value instanceof Float) {
            return Types.FLOAT;
        }
        if (value instanceof Integer) {
            return Types.INTEGER;
        }
        if (value instanceof Long) {
            return Types.BIGINT;
        }
        if (value instanceof BigInteger) {
            return Types.BIGINT;
        }
        if (value instanceof Boolean) {
            return Types.BOOLEAN;
        }
        if (value instanceof Byte || value instanceof Short) {
            return Types.INTEGER;
        }
        if (value instanceof Character) {
            return Types.VARCHAR;
        }
        if (value instanceof java.util.Date || value instanceof java.sql.Date) {
            return Types.DATE;
        }
        if (value instanceof java.sql.Time) {
            return Types.TIME;
        }
        if (value instanceof java.sql.Timestamp) {
            return Types.TIMESTAMP;
        }
        if (value instanceof Object[] || value instanceof List || value instanceof Array) {
            return Types.ARRAY;
        }

        return Types.OTHER;
    }

    @Override
    public String getMimeType() {
        return "application/json";
    }

    private static interface GeneratorTask {
        void apply(JsonGenerator generator) throws JsonGenerationException, IOException;
    }
}
