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
package org.apache.nifi.spring;

import java.io.File;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public final class Utils {

    private static final Logger logger = LoggerFactory.getLogger(Utils.class);

    /**
     * Creates new instance of the class specified by 'className' by first
     * loading it using thread context class loader and then executing default
     * constructor.
     */
    @SuppressWarnings("unchecked")
    static <T> T newDefaultInstance(String className) {
        try {
            Class<T> clazz = (Class<T>) Class.forName(className, false, Thread.currentThread().getContextClassLoader());
            return clazz.newInstance();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to load and/or instantiate class '" + className + "'", e);
        }
    }

    /**
     * Finds a method by name on the target class. If more then one method
     * present it will return the first one encountered.
     *
     * @param name
     *            method name
     * @param targetClass
     *            instance of target class
     * @return instance of {@link Method}
     */
    public static Method findMethod(String name, Class<?> targetClass) {
        Class<?> searchType = targetClass;
        while (searchType != null) {
            Method[] methods = (searchType.isInterface() ? searchType.getMethods() : searchType.getDeclaredMethods());
            for (Method method : methods) {
                if (name.equals(method.getName())) {
                    return method;
                }
            }
            searchType = searchType.getSuperclass();
        }
        return null;
    }

    static URL[] gatherAdditionalClassPathUrls(String path) {
        if (logger.isDebugEnabled()) {
            logger.debug("Adding additional resources from '" + path + "' to the classpath.");
        }
        if (path == null) {
            throw new IllegalArgumentException("'path' must not be null");
        }
        File libraryDir = new File(path);
        if (libraryDir.exists() && libraryDir.isDirectory()) {
            String[] cpResourceNames = libraryDir.list();
            try {
                URLClassLoader thisCl = (URLClassLoader) Utils.class.getClassLoader();
                List<URL> urls = new ArrayList<>();
                for (int i = 0; i < cpResourceNames.length; i++) {
                    if (!isDuplicate(thisCl.getURLs(), cpResourceNames[i])) {
                        URL url = new File(libraryDir, cpResourceNames[i]).toURI().toURL();
                        urls.add(url);
                        if (logger.isDebugEnabled()) {
                            logger.debug("Identifying additional resource to the classpath: " + url);
                        }
                    }
                }
                return urls.toArray(new URL[] {});
            } catch (Exception e) {
                throw new IllegalStateException(
                        "Failed to parse user libraries from '" + libraryDir.getAbsolutePath() + "'", e);
            }
        } else {
            throw new IllegalArgumentException("Path '" + libraryDir.getAbsolutePath()
                    + "' is not valid because it doesn't exist or does not point to a directory.");
        }
    }

    static boolean isDuplicate(URL[] currentURLs, String resourceName) {
        if (resourceName.startsWith("spring")) {
            resourceName = resourceName.substring(0, resourceName.lastIndexOf("-"));
        }
        for (URL eURL : currentURLs) {
            String path = eURL.getPath();
            if (path.contains(resourceName)) {
                return true;
            }
        }
        return false;
    }
}