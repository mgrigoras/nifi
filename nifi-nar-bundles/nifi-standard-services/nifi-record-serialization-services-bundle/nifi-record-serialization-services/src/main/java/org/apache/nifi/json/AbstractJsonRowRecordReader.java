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
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RowRecordReader;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;


public abstract class AbstractJsonRowRecordReader implements RowRecordReader {
    private final ComponentLog logger;
    private final JsonParser jsonParser;
    private final JsonFactory jsonFactory;
    private final boolean array;
    private final JsonNode firstJsonNode;

    private boolean firstObjectConsumed = false;

    private static final TimeZone gmt = TimeZone.getTimeZone("GMT");


    public AbstractJsonRowRecordReader(final InputStream in, final ComponentLog logger) throws IOException, MalformedRecordException {
        this.logger = logger;

        jsonFactory = new JsonFactory();
        try {
            jsonParser = jsonFactory.createJsonParser(in);
            jsonParser.setCodec(new ObjectMapper());

            JsonToken token = jsonParser.nextToken();
            if (token == JsonToken.START_ARRAY) {
                array = true;
                token = jsonParser.nextToken(); // advance to START_OBJECT token
            } else {
                array = false;
            }

            if (token == JsonToken.START_OBJECT) { // could be END_ARRAY also
                firstJsonNode = jsonParser.readValueAsTree();
            } else {
                firstJsonNode = null;
            }
        } catch (final JsonParseException e) {
            throw new MalformedRecordException("Could not parse data as JSON", e);
        }
    }

    @Override
    public Object[] nextRecord(final RecordSchema schema) throws IOException, MalformedRecordException {
        if (firstObjectConsumed && !array) {
            return null;
        }

        final JsonNode nextNode = getNextJsonNode();
        return convertJsonNodeToObjectArray(nextNode, schema);
    }

    protected RecordFieldType determineFieldType(final JsonNode node) {
        if (node.isDouble()) {
            return RecordFieldType.DOUBLE;
        }
        if (node.isBoolean()) {
            return RecordFieldType.BOOLEAN;
        }
        if (node.isFloatingPointNumber()) {
            return RecordFieldType.FLOAT;
        }
        if (node.isBigInteger()) {
            return RecordFieldType.BIGINT;
        }
        if (node.isBigDecimal()) {
            return RecordFieldType.DOUBLE;
        }
        if (node.isLong()) {
            return RecordFieldType.LONG;
        }
        if (node.isInt()) {
            return RecordFieldType.INT;
        }
        if (node.isTextual()) {
            return RecordFieldType.STRING;
        }
        if (node.isArray()) {
            return RecordFieldType.ARRAY;
        }
        if (node.isObject()) {
            return RecordFieldType.RECORD;
        }

        return RecordFieldType.RECORD;
    }


    protected Object convertField(final JsonNode fieldNode, final String fieldName, final DataType desiredType) {
        if (fieldNode == null) {
            return null;
        }

        switch (desiredType.getFieldType()) {
            case BOOLEAN:
                return fieldNode.asBoolean();
            case BYTE:
                return (byte) fieldNode.asInt();
            case CHAR:
                final String text = fieldNode.asText();
                if (text.isEmpty()) {
                    return null;
                }
                return text.charAt(0);
            case DOUBLE:
                return fieldNode.asDouble();
            case FLOAT:
                return (float) fieldNode.asDouble();
            case INT:
                return fieldNode.asInt();
            case LONG:
                return fieldNode.asLong();
            case SHORT:
                return (short) fieldNode.asInt();
            case STRING:
                return fieldNode.asText();
            case DATE: {
                final String string = fieldNode.asText();
                if (string.isEmpty()) {
                    return null;
                }

                try {
                    final DateFormat dateFormat = new SimpleDateFormat(desiredType.getFormat());
                    dateFormat.setTimeZone(gmt);
                    final Date date = dateFormat.parse(string);
                    return new java.sql.Date(date.getTime());
                } catch (ParseException e) {
                    logger.warn("Failed to convert JSON field to Date for field {} (value {})", new Object[] {fieldName, string, e});
                    return null;
                }
            }
            case TIME: {
                final String string = fieldNode.asText();
                if (string.isEmpty()) {
                    return null;
                }

                try {
                    final DateFormat dateFormat = new SimpleDateFormat(desiredType.getFormat());
                    dateFormat.setTimeZone(gmt);
                    final Date date = dateFormat.parse(string);
                    return new java.sql.Date(date.getTime());
                } catch (ParseException e) {
                    logger.warn("Failed to convert JSON field to Time for field {} (value {})", new Object[] {fieldName, string, e});
                    return null;
                }
            }
            case TIMESTAMP: {
                final String string = fieldNode.asText();
                if (string.isEmpty()) {
                    return null;
                }

                try {
                    final DateFormat dateFormat = new SimpleDateFormat(desiredType.getFormat());
                    dateFormat.setTimeZone(gmt);
                    final Date date = dateFormat.parse(string);
                    return new java.sql.Date(date.getTime());
                } catch (ParseException e) {
                    logger.warn("Failed to convert JSON field to Timestamp for field {} (value {})", new Object[] {fieldName, string, e});
                    return null;
                }
            }
            case ARRAY: {
                final ArrayNode arrayNode = (ArrayNode) fieldNode;
                final int numElements = arrayNode.size();
                final Object[] arrayElements = new Object[numElements];
                int count = 0;
                for (final JsonNode node : arrayNode) {
                    final Object converted = convertField(node, fieldName, determineFieldType(node).getDataType());
                    arrayElements[count++] = converted;
                }

                return arrayElements;
            }
            case RECORD: {
                if (fieldNode.isObject()) {
                    final ObjectNode objectNode = (ObjectNode) fieldNode;
                    final Iterator<Map.Entry<String, JsonNode>> childItr = objectNode.getFields();
                    final Map<String, Object> childMap = new HashMap<>();
                    while (childItr.hasNext()) {
                        final Map.Entry<String, JsonNode> entry = childItr.next();
                        final JsonNode node = entry.getValue();
                        final RecordFieldType determinedType = determineFieldType(node);
                        final Object value = convertField(node, node.asText(), determinedType.getDataType());
                        childMap.put(entry.getKey(), value);
                    }
                    return childMap;
                } else {
                    return fieldNode.toString();
                }
            }
        }

        return fieldNode.toString();
    }

    private JsonNode getNextJsonNode() throws JsonParseException, IOException, MalformedRecordException {
        if (!firstObjectConsumed) {
            firstObjectConsumed = true;
            return firstJsonNode;
        }

        while (true) {
            final JsonToken token = jsonParser.nextToken();
            if (token == null) {
                return null;
            }

            switch (token) {
                case END_OBJECT:
                    continue;
                case START_OBJECT:
                    return jsonParser.readValueAsTree();
                case END_ARRAY:
                case START_ARRAY:
                    return null;
                default:
                    throw new MalformedRecordException("Expected to get a JSON Object but got a token of type " + token.name());
            }
        }
    }

    @Override
    public void close() throws IOException {
        jsonParser.close();
    }

    protected JsonParser getJsonParser() {
        return jsonParser;
    }

    protected JsonFactory getJsonFactory() {
        return jsonFactory;
    }

    protected Optional<JsonNode> getFirstJsonNode() {
        return Optional.ofNullable(firstJsonNode);
    }

    protected abstract Object[] convertJsonNodeToObjectArray(final JsonNode nextNode, final RecordSchema schema) throws IOException, MalformedRecordException;
}
