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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Array;
import org.apache.avro.generic.GenericData.StringType;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.ObjectArrayRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;

public class AvroRecordReader implements RecordReader {
    private final InputStream in;
    private final Schema schema;
    private final DataFileStream<GenericRecord> dataFileStream;

    public AvroRecordReader(final InputStream in) throws IOException, MalformedRecordException {
        this.in = in;

        dataFileStream = new DataFileStream<>(in, new GenericDatumReader<GenericRecord>());
        this.schema = dataFileStream.getSchema();
        GenericData.setStringType(this.schema, StringType.String);
    }

    @Override
    public void close() throws IOException {
        dataFileStream.close();
        in.close();
    }

    @Override
    public Record nextRecord(final RecordSchema schema) throws IOException, MalformedRecordException {
        if (!dataFileStream.hasNext()) {
            return null;
        }

        GenericRecord record = null;
        while (record == null && dataFileStream.hasNext()) {
            record = dataFileStream.next();
        }

        final Object[] values = convertRecordToObjectArray(record, schema);
        return new ObjectArrayRecord(schema, values);
    }


    private Object[] convertRecordToObjectArray(final GenericRecord record, final RecordSchema schema) {
        final Object[] values = new Object[schema.getFieldCount()];

        for (int i = 0; i < schema.getFieldCount(); i++) {
            final RecordField recordField = schema.getField(i);
            final Object value = record.get(recordField.getFieldName());

            final Field avroField = record.getSchema().getField(recordField.getFieldName());
            if (avroField == null) {
                values[i] = null;
                continue;
            }
            final Schema fieldSchema = avroField.schema();

            values[i] = convertValue(value, fieldSchema, avroField.name(), recordField.getDataType());
        }

        return values;
    }


    @Override
    public RecordSchema getSchema() throws MalformedRecordException {
        final List<RecordField> recordFields = new ArrayList<>(schema.getFields().size());
        for (final Field field : schema.getFields()) {
            final String fieldName = field.name();
            final RecordFieldType recordFieldType = determineFieldType(field.schema());
            recordFields.add(new RecordField(fieldName, recordFieldType.getDataType()));
        }

        return new SimpleRecordSchema(recordFields);
    }

    private Object convertValue(final Object value, final Schema avroSchema, final String fieldName, final DataType desiredType) {
        if (value == null) {
            return null;
        }

        switch (avroSchema.getType()) {
            case UNION:
                if (value instanceof GenericData.Record) {
                    final GenericData.Record record = (GenericData.Record) value;
                    return convertValue(value, record.getSchema(), fieldName, desiredType);
                }
                break;
            case RECORD:
                final GenericData.Record record = (GenericData.Record) value;
                final Schema recordSchema = record.getSchema();
                final List<Field> recordFields = recordSchema.getFields();
                final Map<String, Object> values = new HashMap<>(recordFields.size());
                for (final Field field : recordFields) {
                    final RecordFieldType desiredFieldType = determineFieldType(field.schema());
                    final Object avroFieldValue = record.get(field.name());
                    final Object fieldValue = convertValue(avroFieldValue, field.schema(), field.name(), desiredFieldType.getDataType());
                    values.put(field.name(), fieldValue);
                }
                return values;
            case BYTES:
                final ByteBuffer bb = (ByteBuffer) value;
                return bb.array();
            case FIXED:
                final GenericFixed fixed = (GenericFixed) value;
                return fixed.bytes();
            case ENUM:
                return value.toString();
            case NULL:
                return null;
            case STRING:
                return value.toString();
            case ARRAY:
                final Array<?> array = (Array<?>) value;
                final Object[] valueArray = new Object[array.size()];
                for (int i = 0; i < array.size(); i++) {
                    final Schema elementSchema = avroSchema.getElementType();
                    valueArray[i] = convertValue(array.get(i), elementSchema, fieldName, determineFieldType(elementSchema).getDataType());
                }
                return valueArray;
            case MAP:
                final Map<?, ?> avroMap = (Map<?, ?>) value;
                final Map<String, Object> map = new HashMap<>(avroMap.size());
                for (final Map.Entry<?, ?> entry : avroMap.entrySet()) {
                    Object obj = entry.getValue();
                    if (obj instanceof Utf8 || obj instanceof CharSequence) {
                        obj = obj.toString();
                    }

                    map.put(entry.getKey().toString(), obj);
                }
                return map;
        }

        return value;
    }


    private RecordFieldType determineFieldType(final Schema avroSchema) {
        final Type avroType = avroSchema.getType();

        RecordFieldType recordFieldType = null;
        switch (avroType) {
            case ARRAY:
            case BYTES:
            case FIXED:
                recordFieldType = RecordFieldType.ARRAY;
                break;
            case BOOLEAN:
                recordFieldType = RecordFieldType.BOOLEAN;
                break;
            case DOUBLE:
                recordFieldType = RecordFieldType.DOUBLE;
                break;
            case ENUM:
            case STRING:
                recordFieldType = RecordFieldType.STRING;
                break;
            case FLOAT:
                recordFieldType = RecordFieldType.FLOAT;
                break;
            case INT:
                recordFieldType = RecordFieldType.INT;
                break;
            case LONG:
                recordFieldType = RecordFieldType.LONG;
                break;
            case NULL:
            case MAP:
            case RECORD:
                recordFieldType = RecordFieldType.OBJECT;
                break;
            case UNION:
                final List<Schema> subSchemas = avroSchema.getTypes();
                recordFieldType = RecordFieldType.OBJECT;

                if (subSchemas.size() == 2) {
                    if (subSchemas.get(0).getType() == Type.NULL) {
                        recordFieldType = determineFieldType(subSchemas.get(1));
                    } else if (subSchemas.get(1).getType() == Type.NULL) {
                        recordFieldType = determineFieldType(subSchemas.get(0));
                    }
                }

                break;
        }

        return recordFieldType;
    }

}
