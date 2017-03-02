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

package org.apache.nifi.avro;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.nifi.serialization.ResultSetWriter;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;

public class WriteAvroResult implements ResultSetWriter {
    private final Schema schema;

    public WriteAvroResult(final Schema schema) {
        this.schema = schema;
    }

    @Override
    public WriteResult write(final RecordSet rs, final OutputStream outStream) throws IOException, SQLException {
        Record record = rs.next();
        if (record == null) {
            return WriteResult.of(0, Collections.emptyMap());
        }

        final GenericRecord rec = new GenericData.Record(schema);

        int nrOfRows = 0;
        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        try (final DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            dataFileWriter.create(schema, outStream);

            final RecordSchema recordSchema = rs.getSchema();
            final int numCols = recordSchema.getFieldCount();

            do {
                for (int i = 0; i < numCols; i++) {
                    final Object value = record.getValue(i);
                    final String fieldName = recordSchema.getField(i).getFieldName();

                    final Field field = schema.getField(fieldName);
                    if (field == null) {
                        continue;
                    }

                    final Object converted = convert(value, field.schema(), fieldName);
                    rec.put(fieldName, converted);
                }

                dataFileWriter.append(rec);
                nrOfRows++;
            } while ((record = rs.next()) != null);
        }

        return WriteResult.of(nrOfRows, Collections.emptyMap());
    }


    private Object convert(final Object value, final Schema schema, final String fieldName) throws SQLException, IOException {
        if (value == null) {
            return null;
        }

        // Need to handle CLOB and BLOB before getObject() is called, due to ResultSet's maximum portability statement
        if (value instanceof Clob) {
            final Clob clob = (Clob) value;

            long numChars = clob.length();
            char[] buffer = new char[(int) numChars];
            InputStream is = clob.getAsciiStream();
            int index = 0;
            int c = is.read();
            while (c > 0) {
                buffer[index++] = (char) c;
                c = is.read();
            }

            clob.free();
            return new String(buffer);
        }

        if (value instanceof Blob) {
            final Blob blob = (Blob) value;

            final long numChars = blob.length();
            final byte[] buffer = new byte[(int) numChars];
            final InputStream is = blob.getBinaryStream();
            int index = 0;
            int c = is.read();
            while (c > 0) {
                buffer[index++] = (byte) c;
                c = is.read();
            }

            final ByteBuffer bb = ByteBuffer.wrap(buffer);
            blob.free();
            return bb;
        }

        if (value instanceof byte[]) {
            // bytes requires little bit different handling
            return ByteBuffer.wrap((byte[]) value);
        } else if (value instanceof Byte) {
            // tinyint(1) type is returned by JDBC driver as java.sql.Types.TINYINT
            // But value is returned by JDBC as java.lang.Byte
            // (at least H2 JDBC works this way)
            // direct put to avro record results:
            // org.apache.avro.AvroRuntimeException: Unknown datum type java.lang.Byte
            return ((Byte) value).intValue();
        } else if (value instanceof Short) {
            //MS SQL returns TINYINT as a Java Short, which Avro doesn't understand.
            return ((Short) value).intValue();
        } else if (value instanceof BigDecimal) {
            // Avro can't handle BigDecimal as a number - it will throw an AvroRuntimeException such as: "Unknown datum type: java.math.BigDecimal: 38"
            return value.toString();
        } else if (value instanceof BigInteger) {
            // Check the precision of the BIGINT. Some databases allow arbitrary precision (> 19), but Avro won't handle that.
            // It the SQL type is BIGINT and the precision is between 0 and 19 (inclusive); if so, the BigInteger is likely a
            // long (and the schema says it will be), so try to get its value as a long.
            // Otherwise, Avro can't handle BigInteger as a number - it will throw an AvroRuntimeException
            // such as: "Unknown datum type: java.math.BigInteger: 38". In this case the schema is expecting a string.
            final BigInteger bigInt = (BigInteger) value;
            if (bigInt.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) > 0) {
                return value.toString();
            } else {
                return bigInt.longValue();
            }
        } else if (value instanceof Boolean) {
            return value;
        } else if (value instanceof Map) {
            switch (schema.getType()) {
                case MAP:
                    return value;
                case RECORD:
                    final GenericData.Record record = new GenericData.Record(schema);

                    final Map<?, ?> map = (Map<?, ?>) value;
                    for (final Map.Entry<?, ?> entry : map.entrySet()) {
                        final String key = (String) entry.getKey();
                        final Object mapValue = entry.getValue();

                        final Field field = schema.getField(key);
                        if (field == null) {
                            continue;
                        }

                        final Object converted = convert(mapValue, field.schema(), key);
                        record.put(key, converted);
                    }
                    return record;
            }

            return value.toString();

        } else if (value instanceof List) {
            return value;
        } else if (value instanceof Object[]) {
            final List<Object> list = new ArrayList<>();
            for (final Object o : ((Object[]) value)) {
                final Object converted = convert(o, schema.getElementType(), fieldName);
                list.add(converted);
            }
            return list;
        } else if (value instanceof Number) {
            return value;
        }

        // The different types that we support are numbers (int, long, double, float),
        // as well as boolean values and Strings. Since Avro doesn't provide
        // timestamp types, we want to convert those to Strings. So we will cast anything other
        // than numbers or booleans to strings by using the toString() method.
        return value.toString();
    }


    @Override
    public String getMimeType() {
        return "application/avro-binary";
    }


    public static String normalizeNameForAvro(String inputName) {
        String normalizedName = inputName.replaceAll("[^A-Za-z0-9_]", "_");
        if (Character.isDigit(normalizedName.charAt(0))) {
            normalizedName = "_" + normalizedName;
        }
        return normalizedName;
    }
}
