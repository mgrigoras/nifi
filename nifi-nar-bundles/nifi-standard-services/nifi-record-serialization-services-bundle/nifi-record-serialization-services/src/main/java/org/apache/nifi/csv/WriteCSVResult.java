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
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collections;

import org.apache.nifi.serialization.ResultSetWriter;
import org.apache.nifi.serialization.WriteResult;

import au.com.bytecode.opencsv.CSVWriter;
import au.com.bytecode.opencsv.ResultSetHelperService;

public class WriteCSVResult implements ResultSetWriter {

    private String[] getColumnLabels(final ResultSet rs) throws SQLException {
        final ResultSetMetaData metadata = rs.getMetaData();
        final String[] cols = new String[metadata.getColumnCount()];

        for (int i = 0; i < metadata.getColumnCount(); i++) {
            cols[i] = metadata.getColumnLabel(i + 1);
        }
        return cols;
    }

    @Override
    public WriteResult write(final ResultSet rs, final OutputStream out) throws IOException {
        int count = 0;
        try (final OutputStreamWriter streamWriter = new OutputStreamWriter(out);
            final CSVWriter writer = new CSVWriter(streamWriter)) {

            try {
                ResultSetHelperService rsHelper = new ResultSetHelperService();
                final String[] columnNames = getColumnLabels(rs);
                writer.writeNext(columnNames);

                while (rs.next()) {
                    writer.writeNext(rsHelper.getColumnValues(rs));
                    count++;
                }
            } catch (final SQLException e) {
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
