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

package org.apache.nifi.csv;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.ObjectArrayRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;
import org.junit.Test;


public class TestWriteCSVResult {

    @Test
    public void testDataTypes() throws IOException {
        final WriteCSVResult result = new WriteCSVResult(RecordFieldType.DATE.getDefaultFormat(), RecordFieldType.TIME.getDefaultFormat(), RecordFieldType.TIMESTAMP.getDefaultFormat());

        final StringBuilder headerBuilder = new StringBuilder();
        final List<RecordField> fields = new ArrayList<>();
        for (final RecordFieldType fieldType : RecordFieldType.values()) {
            fields.add(new RecordField(fieldType.name().toLowerCase(), fieldType.getDataType()));
            headerBuilder.append('"').append(fieldType.name().toLowerCase()).append('"').append(",");
        }
        final RecordSchema schema = new SimpleRecordSchema(fields);

        final long now = System.currentTimeMillis();
        final Record record = new ObjectArrayRecord(schema, new Object[] {"string", true, (byte) 1, 'c', (short) 8, 9, BigInteger.valueOf(8L), 8L, 8.0F, 8.0D,
            new Date(now), new Time(now), new Timestamp(now), null, null});
        final RecordSet rs = RecordSet.of(schema, record);

        final String output;
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            result.write(rs, baos);
            output = new String(baos.toByteArray(), StandardCharsets.UTF_8);
        }

        headerBuilder.deleteCharAt(headerBuilder.length() - 1);
        final String headerLine = headerBuilder.toString();

        final String[] splits = output.split("\n");
        assertEquals(2, splits.length);
        assertEquals(headerLine, splits[0]);

        final String values = splits[1];
        final StringBuilder expectedBuilder = new StringBuilder();
        expectedBuilder.append("\"string\",\"true\",\"1\",\"c\",\"8\",\"9\",\"8\",\"8\",\"8.0\",\"8.0\",");

        final String dateValue = new SimpleDateFormat(RecordFieldType.DATE.getDefaultFormat()).format(now);
        final String timeValue = new SimpleDateFormat(RecordFieldType.TIME.getDefaultFormat()).format(now);
        final String timestampValue = new SimpleDateFormat(RecordFieldType.TIMESTAMP.getDefaultFormat()).format(now);

        expectedBuilder.append('"').append(dateValue).append('"').append(',');
        expectedBuilder.append('"').append(timeValue).append('"').append(',');
        expectedBuilder.append('"').append(timestampValue).append('"').append(',');
        expectedBuilder.append(',');
        final String expectedValues = expectedBuilder.toString();

        assertEquals(expectedValues, values);
    }

}
