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

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Collections;

import org.apache.nifi.serialization.ResultSetWriter;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;

import au.com.bytecode.opencsv.CSVWriter;

public class WriteCSVResult implements ResultSetWriter {

    @Override
    public WriteResult write(final RecordSet rs, final OutputStream out) throws IOException {
        int count = 0;
        try (final OutputStreamWriter streamWriter = new OutputStreamWriter(out);
            final CSVWriter writer = new CSVWriter(streamWriter)) {

            try {
                final RecordSchema schema = rs.getSchema();
                final String[] columnNames = schema.getFieldNames().toArray(new String[0]);
                writer.writeNext(columnNames);

                Record record;
                while ((record = rs.next()) != null) {
                    final String[] colVals = new String[schema.getFieldCount()];
                    for (int i = 0; i < schema.getFieldCount(); i++) {
                        colVals[i] = record.getAsString(i);
                    }

                    writer.writeNext(colVals);
                    count++;
                }
            } catch (final Exception e) {
                throw new IOException("Failed to serialize results", e);
            }
        }

        return WriteResult.of(count, Collections.emptyMap());
    }

    @Override
    public String getMimeType() {
        return "text/csv";
    }
}
