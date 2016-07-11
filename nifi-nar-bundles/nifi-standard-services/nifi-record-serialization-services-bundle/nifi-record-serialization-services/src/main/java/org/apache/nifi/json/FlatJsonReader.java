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

import java.io.IOException;
import java.io.InputStream;

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RowRecordReader;
import org.apache.nifi.serialization.RowRecordReaderFactory;
import org.apache.nifi.serialization.UserTypeOverrideRowReader;

// TODO: Need to provide examples of Object/Array in the Usage!
@Tags({"json", "flat", "record", "reader", "parser"})
@CapabilityDescription("Parses \"flat\" JSON records into a row-oriented record. By \"flat\" we mean JSON that does not "
    + "have any nested elements. The root JSON element can be either a single element or an array of JSON elements, and each "
    + "element in that array will be treated as a separate record. If any of the elements has a nested array or a nested "
    + "element, they will be returned as OBJECT or ARRAY types, not broken out into individual records. "
    + "The schema for the record is determined by the first "
    + "JSON element in the array, if an array is used. The data type of a field can be overridden by adding a property to "
    + "the controller service where the name of the property matches the JSON field name and the value of the property is "
    + "the data type to use. If that field does not exist in a JSON element, the field will be assumed to be null. "
    + "See the Usage of the Controller Service for more information.")
@SeeAlso(JsonPathReader.class)
@DynamicProperty(name = "<name of JSON field>", value = "<data type of JSON field>",
    description = "User-defined properties are used to indicate that the values of a specific field should be interpreted as a "
    + "user-defined data type (e.g., int, double, float, date, etc.)", supportsExpressionLanguage = false)
public class FlatJsonReader extends UserTypeOverrideRowReader implements RowRecordReaderFactory {

    @Override
    public RowRecordReader createRecordReader(final InputStream in, final ComponentLog logger) throws IOException, MalformedRecordException {
        return new FlatJsonRowRecordReader(in, logger, getFieldTypeOverrides());
    }
}
