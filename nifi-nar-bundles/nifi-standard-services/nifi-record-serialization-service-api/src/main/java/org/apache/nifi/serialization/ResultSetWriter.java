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

package org.apache.nifi.serialization;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * <p>
 * A ResultSetWriter is responsible for writing a ResultSet to a given {@link OutputStream}.
 * </p>
 *
 * <p>
 * PLEASE NOTE: This interface is still considered 'unstable' and may change in a non-backward-compatible
 * manner between minor or incremental releases of NiFi.
 * </p>
 */
public interface ResultSetWriter {
    /**
     * Writes the given result set to the given output stream
     *
     * @param resultSet the result set to serialize
     * @param out the OutputStream to write to
     * @return the results of writing the data
     * @throws IOException if unable to write to the given OutputStream
     * @throws SQLException if unable to process the result set as necessary
     */
    // TODO: RowRecordReader is given an InputStream as a constructor argument.
    // ResultSetWriter is given an OutputStream to write to. These should probably be consistent.
    WriteResult write(ResultSet resultSet, OutputStream out) throws IOException, SQLException;

    /**
     * @return the MIME Type that the Result Set Writer produces. This will be added to FlowFiles using
     *         the mime.type attribute.
     */
    String getMimeType();
}
