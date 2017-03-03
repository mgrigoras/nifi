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

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;
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

public class TestWriteJsonResult {

    @Test
    public void testDataTypes() throws IOException, ParseException {
        final WriteJsonResult writer = new WriteJsonResult(true, RecordFieldType.DATE.getDefaultFormat(),
            RecordFieldType.TIME.getDefaultFormat(), RecordFieldType.TIMESTAMP.getDefaultFormat());

        final List<RecordField> fields = new ArrayList<>();
        for (final RecordFieldType fieldType : RecordFieldType.values()) {
            fields.add(new RecordField(fieldType.name().toLowerCase(), fieldType.getDataType()));
        }
        final RecordSchema schema = new SimpleRecordSchema(fields);

        final long time = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS").parse("2017/01/01 17:00:00.000").getTime();
        final Record record = new ObjectArrayRecord(schema, new Object[] {"string", true, (byte) 1, 'c', (short) 8, 9, BigInteger.valueOf(8L), 8L, 8.0F, 8.0D,
            new Date(time), new Time(time), new Timestamp(time), null, null});
        final RecordSet rs = RecordSet.of(schema, record);

        final String output;
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            writer.write(rs, baos);
            output = baos.toString();
        }

        final String expected = new String(Files.readAllBytes(Paths.get("src/test/resources/json/output/dataTypes.json")));
        assertEquals(expected, output);
    }

}
