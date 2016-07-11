package org.apache.nifi.queryflowfile.xml;
///*
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements.  See the NOTICE file distributed with
// * this work for additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package org.apache.nifi.sqltransform.xml;
//
//import java.io.IOException;
//import java.io.InputStream;
//
//import javax.xml.parsers.DocumentBuilder;
//import javax.xml.parsers.DocumentBuilderFactory;
//
//import org.apache.nifi.logging.ComponentLog;
//import org.apache.nifi.sqltransform.RecordField;
//import org.apache.nifi.sqltransform.RecordFieldType;
//import org.apache.nifi.sqltransform.RecordParser;
//import org.apache.nifi.sqltransform.RecordSchema;
//import org.w3c.dom.Document;
//import org.w3c.dom.Node;
//import org.w3c.dom.NodeList;
//
//public class XMLParser implements RecordParser {
//
//    private final InputStream source;
//
//    private NodeList nodeList;
//    private int nodeIndex;
//
//    public XMLParser(final InputStream source, final ComponentLog logger) {
//        this.source = source;
//    }
//
//    @Override
//    public Object[] nextRecord(final RecordSchema schema) throws IOException {
//        if (nodeList == null) {
//            try {
//                final DocumentBuilder docBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
//                final Document doc = docBuilder.parse(source);
//                nodeList = doc.getDocumentElement().getChildNodes();
//            } catch (final RuntimeException e) {
//                throw e;
//            } catch (final Exception e) {
//                throw new RuntimeException(e);
//            }
//        }
//
//        if (nodeList.getLength() < nodeIndex + 1) {
//            return null;
//        }
//
//        final Node node = nodeList.item(nodeIndex++);
//
//        final NodeList childList = node.getChildNodes();
//        final Object[] cellValues = new Object[childList.getLength()];
//        for (int i = 0; i < childList.getLength(); i++) {
//            final Node childNode = childList.item(i);
//            final String textContent = childNode.getTextContent();
//
//            final RecordField recordField = schema.getFields().stream()
//                .filter(field -> field.getFieldName().equals(childNode.getNodeName()))
//                .findFirst()
//                .orElse(null);
//
//            if (recordField != null) {
//                final RecordFieldType fieldType = recordField.getDataType();
//                switch (fieldType) {
//                    case BOOLEAN:
//                        cellValues[i] = Boolean.parseBoolean(textContent);
//                        break;
//                    case DOUBLE:
//                    case FLOAT:
//                        cellValues[i] = Double.parseDouble(textContent);
//                        break;
//                    case INT:
//                    case SHORT:
//                    case LONG:
//                        cellValues[i] = Integer.parseInt(textContent);
//                        break;
//                    case STRING:
//                    case CHAR:
//                    default:
//                        cellValues[i] = textContent;
//                        break;
//                }
//            }
//        }
//
//        return cellValues;
//    }
//
//    @Override
//    public void close() throws IOException {
//        source.close();
//    }
//}
