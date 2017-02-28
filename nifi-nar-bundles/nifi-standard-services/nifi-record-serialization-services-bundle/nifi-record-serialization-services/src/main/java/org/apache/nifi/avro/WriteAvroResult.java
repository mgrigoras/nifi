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

import static java.sql.Types.ARRAY;
import static java.sql.Types.BIGINT;
import static java.sql.Types.BINARY;
import static java.sql.Types.BIT;
import static java.sql.Types.BLOB;
import static java.sql.Types.BOOLEAN;
import static java.sql.Types.CHAR;
import static java.sql.Types.CLOB;
import static java.sql.Types.DATE;
import static java.sql.Types.DECIMAL;
import static java.sql.Types.DOUBLE;
import static java.sql.Types.FLOAT;
import static java.sql.Types.INTEGER;
import static java.sql.Types.JAVA_OBJECT;
import static java.sql.Types.LONGNVARCHAR;
import static java.sql.Types.LONGVARBINARY;
import static java.sql.Types.LONGVARCHAR;
import static java.sql.Types.NCHAR;
import static java.sql.Types.NUMERIC;
import static java.sql.Types.NVARCHAR;
import static java.sql.Types.OTHER;
import static java.sql.Types.REAL;
import static java.sql.Types.ROWID;
import static java.sql.Types.SMALLINT;
import static java.sql.Types.TIME;
import static java.sql.Types.TIMESTAMP;
import static java.sql.Types.TINYINT;
import static java.sql.Types.VARBINARY;
import static java.sql.Types.VARCHAR;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.serialization.ResultSetWriter;
import org.apache.nifi.serialization.WriteResult;

public class WriteAvroResult implements ResultSetWriter {
    private static final int MAX_DIGITS_IN_BIGINT = 19;
    private static final int MAX_DIGITS_IN_INT = 9;

    private final String recordName;
    private final boolean convertNames;

    public WriteAvroResult(final String recordName, final boolean convertNames) {
        this.recordName = recordName;
        this.convertNames = convertNames;
    }

    @Override
    public WriteResult write(final ResultSet rs, final OutputStream outStream) throws IOException, SQLException {
        if (!rs.next()) {
            return WriteResult.of(0, Collections.emptyMap());
        }

        final Schema schema = createSchema(rs, recordName, convertNames);
        final GenericRecord rec = new GenericData.Record(schema);

        int nrOfRows = 0;
        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        try (final DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            dataFileWriter.create(schema, outStream);

            final ResultSetMetaData meta = rs.getMetaData();
            final int nrOfColumns = meta.getColumnCount();
            do {
                for (int i = 1; i <= nrOfColumns; i++) {
                    final int javaSqlType = meta.getColumnType(i);

                    final Object value = rs.getObject(i);
                    final Object converted = convert(value, javaSqlType, rs, i, schema);
                    rec.put(i - 1, converted);
                }

                dataFileWriter.append(rec);
                nrOfRows++;
            } while (rs.next());
        }

        return WriteResult.of(nrOfRows, Collections.emptyMap());
    }


    private Object convert(final Object value, final int sqlType, final ResultSet rs, final int column, final Schema schema) throws SQLException, IOException {
        if (value == null) {
            return null;
        }

        // Need to handle CLOB and BLOB before getObject() is called, due to ResultSet's maximum portability statement
        if (sqlType == CLOB) {
            final Clob clob = rs.getClob(column);
            if (clob == null) {
                return null;
            }

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

        if (sqlType == BLOB) {
            final Blob blob = rs.getBlob(column);
            if (blob == null) {
                return null;
            }

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

        if (sqlType == BINARY || sqlType == VARBINARY || sqlType == LONGVARBINARY) {
            // bytes requires little bit different handling
            byte[] bytes = rs.getBytes(column);
            ByteBuffer bb = ByteBuffer.wrap(bytes);
            return bb;
        } else if (value instanceof byte[]) {
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
            if (sqlType == BIGINT) {
                final int precision = rs.getMetaData().getPrecision(column);
                if (precision < 0 || precision > MAX_DIGITS_IN_BIGINT) {
                    return value.toString();
                } else {
                    try {
                        return ((BigInteger) value).longValueExact();
                    } catch (ArithmeticException ae) {
                        // Since the value won't fit in a long, convert it to a string
                        return value.toString();
                    }
                }
            } else {
                return value.toString();
            }
        } else if (value instanceof Number || value instanceof Boolean) {
            if (sqlType == BIGINT) {
                final int precision = rs.getMetaData().getPrecision(column);
                if (precision < 0 || precision > MAX_DIGITS_IN_BIGINT) {
                    return value.toString();
                } else {
                    return value;
                }
            } else {
                return value;
            }
        } else if (value instanceof Map) {
            final Map<?, ?> map = (Map<?, ?>) value;
            if (map.isEmpty()) {
                return null;
            } else {
                final String fieldName = getFieldName(rs, column);
                final Field field = schema.getField(fieldName);

                if (field == null) {
                    return null;
                } else {
                    final Schema fieldSchema = field.schema();
                    final Type fieldType = fieldSchema.getType();
                    switch (fieldType) {
                        case MAP:
                            break;
                        case UNION:
                            break;
                        case RECORD:
                            final Record subRecord = new GenericData.Record(fieldSchema);

                            for (final Map.Entry<?, ?> entry : map.entrySet()) {
                                final String key = (String) entry.getKey();
                                final Object recordVal = entry.getValue();
                                final Object converted = convert(recordVal, Integer.MIN_VALUE, rs, column, schema);
                                subRecord.put(key, converted);
                            }

                            return subRecord;
                        default:
                            return value.toString();
                    }
                }

                final Object firstObj = map.values().iterator().next();
                final Type elementType = determineElementType(firstObj);
                if (elementType == Type.NULL) {
                    return value.toString();
                } else {
                    return value;
                }
            }
        } else if (value instanceof List) {
            return value;
        } else if (value instanceof Object[]) {
            final List<Object> list = new ArrayList<>();
            for (final Object o : ((Object[]) value)) {
                list.add(o);
            }
            return list;
        } else {
            // The different types that we support are numbers (int, long, double, float),
            // as well as boolean values and Strings. Since Avro doesn't provide
            // timestamp types, we want to convert those to Strings. So we will cast anything other
            // than numbers or booleans to strings by using the toString() method.
            return value.toString();
        }
    }


    @Override
    public String getMimeType() {
        return "application/avro-binary";
    }

    /**
     * Creates an Avro schema from a result set. If the table/record name is known a priori and provided, use that as a
     * fallback for the record name if it cannot be retrieved from the result set, and finally fall back to a default value.
     *
     * @param rs The result set to convert to Avro
     * @param recordName The a priori record name to use if it cannot be determined from the result set.
     * @param convertNames Specifies whether or not names of columns should be normalized to adhere to Avro-required naming conventions.
     *            If true, column names will be converted as necessary. If false, an Exception will be thrown if an invalid name is found.
     *
     * @return A Schema object representing the result set converted to an Avro record
     * @throws SQLException if any error occurs during conversion
     */
    public Schema createSchema(final ResultSet rs, final String recordName, final boolean convertNames) throws SQLException {
        final ResultSetMetaData meta = rs.getMetaData();
        final int nrOfColumns = meta.getColumnCount();

        final List<Field> fields = new ArrayList<>();

        // Some missing Avro types - Decimal, Date types. May need some additional work.
        for (int i = 1; i <= nrOfColumns; i++) {
            final Field field = createField(rs, i);
            fields.add(field);
        }

        final Schema schema = Schema.createRecord(recordName, null, null, false);
        schema.setFields(fields);
        return schema;
    }

    private String getFieldName(final ResultSet rs, final int column) throws SQLException {
        final ResultSetMetaData meta = rs.getMetaData();

        // As per JDBC 4 specs, getColumnLabel will have the alias for the column, if not it will have the column name.
        // so it may be a better option to check for column label first and if in case it is null is some implementation,
        // check for alias. Postgres is the one that has the null column names for calculated fields.
        final String nameOrLabel = StringUtils.isNotEmpty(meta.getColumnLabel(column)) ? meta.getColumnLabel(column) : meta.getColumnName(column);
        final String columnName = convertNames ? normalizeNameForAvro(nameOrLabel) : nameOrLabel;
        return columnName;
    }

    private Schema createNullable(final Type type) {
        return Schema.createUnion(Arrays.asList(Schema.create(Type.NULL), Schema.create(type)));
    }

    private Field createField(final ResultSet rs, final int column) throws SQLException {
        final ResultSetMetaData meta = rs.getMetaData();
        final int sqlType = meta.getColumnType(column);
        final String fieldName = getFieldName(rs, column);

        return new Field(fieldName, createSchema(sqlType, rs, column, Optional.empty()), null, null);
    }

    private Schema createSchema(final int sqlType, final ResultSet rs, final int column, final Optional<Object> value) throws SQLException {
        final int precision = rs.getMetaData().getPrecision(column);
        switch (sqlType) {
            case CHAR:
            case LONGNVARCHAR:
            case LONGVARCHAR:
            case NCHAR:
            case NVARCHAR:
            case VARCHAR:
            case CLOB:
                return createNullable(Type.STRING);

            case BIT:
            case BOOLEAN:
                return createNullable(Type.BOOLEAN);

            case INTEGER:
                if (precision > 0 && precision <= MAX_DIGITS_IN_INT) {
                    return createNullable(Type.INT);
                } else {
                    return createNullable(Type.LONG);
                }

            case SMALLINT:
            case TINYINT:
                return createNullable(Type.INT);

            case BIGINT:
                // Check the precision of the BIGINT. Some databases allow arbitrary precision (> 19), but Avro won't handle that.
                // If the precision > 19 (or is negative), use a string for the type, otherwise use a long. The object(s) will be converted
                // to strings as necessary
                if (precision < 0 || precision > MAX_DIGITS_IN_BIGINT) {
                    return createNullable(Type.STRING);
                } else {
                    return createNullable(Type.LONG);
                }

            case FLOAT:
            case REAL:
                return createNullable(Type.FLOAT);
            case DOUBLE:
                return createNullable(Type.DOUBLE);

            // Did not find direct suitable type, need to be clarified!!!!
            case ROWID:
            case DECIMAL:
            case NUMERIC:
            case DATE:
            case TIME:
            case TIMESTAMP:
                return createNullable(Type.STRING);

            case BINARY:
            case VARBINARY:
            case LONGVARBINARY:
            case BLOB:
                return createNullable(Type.BYTES);

            case ARRAY: {
                final Array array = rs.getArray(column);
                final int baseType = array.getBaseType();

                final Schema itemsSchema = createSchema(baseType, rs, column, value);
                return Schema.createArray(itemsSchema);
            }

            case OTHER:
            case JAVA_OBJECT: {
                final Object obj = value.isPresent() ? value.get() : rs.getObject(column);
                if (obj == null) {
                    return Schema.create(Type.STRING);
                }

                if (obj instanceof Map) {
                    final Map<?, ?> map = (Map<?, ?>) obj;

                    if (map.isEmpty()) {
                        return Schema.create(Type.STRING);
                    } else {
                        final Map<String, Type> typeMap = new HashMap<>();
                        for (final Map.Entry<?, ?> entry : map.entrySet()) {
                            final Object key = entry.getKey();
                            final Object mapValue = entry.getValue();
                            final Type type = determineElementType(mapValue);
                            typeMap.put((String) key, type);
                        }

                        final long distinctValues = typeMap.values().stream()
                            .distinct()
                            .count();

                        if (distinctValues == 1) {
                            return Schema.createMap(Schema.create(typeMap.values().iterator().next()));
                        }

                        // Different types so use a Record
                        final Schema subRecordSchema = Schema.createRecord(getFieldName(rs, column), null, null, false);
                        final List<Field> subRecordFields = new ArrayList<>();
                        for (final Map.Entry<?, ?> entry : map.entrySet()) {
                            final String key = (String) entry.getKey();
                            final Object mapValue = entry.getValue();
                            final Type type = determineElementType(mapValue);
                            final Schema schema;
                            if (type == Type.NULL) {
                                final int fieldSqlType = determineSqlType(type, mapValue);
                                schema = createSchema(fieldSqlType, rs, column, Optional.ofNullable(mapValue));
                            } else {
                                schema = Schema.create(type);
                            }

                            final Field field = new Field(key, schema, null, null);
                            subRecordFields.add(field);
                        }
                        subRecordSchema.setFields(subRecordFields);

                        return Schema.createMap(subRecordSchema);
                    }
                } else {
                    return Schema.create(Type.STRING);
                }
            }
        }

        return Schema.create(Type.STRING);
    }


    private int determineSqlType(final Type type, final Object value) {
        switch (type) {
            case ARRAY:
                return Types.ARRAY;
            case BOOLEAN:
                return Types.BOOLEAN;
            case BYTES:
                return Types.ARRAY;
            case DOUBLE:
                return Types.DOUBLE;
            case ENUM:
                return Types.VARCHAR;
            case FIXED:
                return Types.ARRAY;
            case FLOAT:
                return Types.FLOAT;
            case INT:
                return Types.INTEGER;
            case LONG:
                return Types.DOUBLE;
            case RECORD:
                return Types.JAVA_OBJECT;
            case STRING:
                return Types.VARCHAR;
            case UNION:
                return Types.JAVA_OBJECT;
        }

        if (value instanceof Object[]) {
            return Types.ARRAY;
        }

        return Types.JAVA_OBJECT;
    }

    private Type determineElementType(final Object value) {
        Type elementType = Type.NULL;

        if (value instanceof String) {
            elementType = Type.STRING;
        } else if (value instanceof Long) {
            elementType = Type.LONG;
        } else if (value instanceof Integer) {
            elementType = Type.INT;
        } else if (value instanceof Double) {
            elementType = Type.DOUBLE;
        } else if (value instanceof Float) {
            elementType = Type.FLOAT;
        } else if (value instanceof Boolean) {
            elementType = Type.BOOLEAN;
        } else if (value instanceof byte[]) {
            elementType = Type.BYTES;
        }

        return elementType;
    }

    public static String normalizeNameForAvro(String inputName) {
        String normalizedName = inputName.replaceAll("[^A-Za-z0-9_]", "_");
        if (Character.isDigit(normalizedName.charAt(0))) {
            normalizedName = "_" + normalizedName;
        }
        return normalizedName;
    }
}
