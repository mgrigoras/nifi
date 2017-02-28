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

import java.util.Arrays;
import java.util.List;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.ResultSetWriter;
import org.apache.nifi.serialization.ResultSetWriterFactory;

@Tags({"avro", "result", "set", "writer", "serializer", "record", "row"})
@CapabilityDescription("Writes the contents of a Database ResultSet in Binary Avro format.")
public class AvroResultSetWriter extends AbstractControllerService implements ResultSetWriterFactory {
    static final PropertyDescriptor ROOT_RECORD_NAME = new PropertyDescriptor.Builder()
        .name("Root Record Name")
        .description("The name of the root Avro Record")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(false)
        .defaultValue("FlowFile")
        .required(true)
        .build();

    static final PropertyDescriptor CONVERT_NAMES = new PropertyDescriptor.Builder()
        .name("Normalize Names")
        .description("Avro has a strict naming policy for fields. If any column in the ResultSet has a label that "
            + "does not adhere to this naming policy, this property controls whether NiFi will normalize the name "
            + "into a legal Avro name (if 'true') or fail to write to the data (if 'false')")
        .expressionLanguageSupported(false)
        .allowableValues("true", "false")
        .defaultValue("true")
        .required(true)
        .build();

    private volatile boolean convertNames;
    private volatile String rootRecordName;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Arrays.asList(ROOT_RECORD_NAME, CONVERT_NAMES);
    }

    @OnEnabled
    public void storePropertyValues(final ConfigurationContext context) {
        rootRecordName = context.getProperty(ROOT_RECORD_NAME).getValue();
        convertNames = context.getProperty(CONVERT_NAMES).asBoolean();
    }

    @Override
    public ResultSetWriter createWriter(final ComponentLog logger) {
        return new WriteAvroResult(rootRecordName, convertNames);
    }

}
